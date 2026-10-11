package dev.whitewater;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import dev.whitewater.internal.Transport;
import dev.whitewater.internal.Wire;
import dev.whitewater.model.AppendResult;
import dev.whitewater.model.ServerFeedback;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * One Writer session: every append carries the session epoch and a stable
 * request identity. Retries after ambiguity reuse the same
 * {@code request_id} — never regenerate it — so a committed append is
 * returned identically ({@code deduplicated: true}) rather than written
 * twice.
 *
 * Session sequencing (per-range order, stale-sequence healing) is a
 * server-side concern; the client supplies records in call order.
 */
public final class WriterSessionClient {

    /** One record to append. */
    public record Append(
            UUID requestId,
            byte[] key,
            byte[] payload,
            Long eventTimeNs,
            Map<String, byte[]> metadata) {

        public Append(UUID requestId, byte[] key, byte[] payload) {
            this(requestId, key, payload, null, Map.of());
        }
    }

    /** Batch result: per-record results plus server batching guidance. */
    public record BatchResult(List<AppendResult> results, ServerFeedback feedback) {}

    private final Transport transport;
    private final Executor executor;
    private final String writer;
    private final long sessionEpoch;

    WriterSessionClient(Transport transport, Executor executor, String writer, long sessionEpoch) {
        this.transport = transport;
        this.executor = executor;
        this.writer = Objects.requireNonNull(writer);
        this.sessionEpoch = sessionEpoch;
    }

    public String writer() {
        return writer;
    }

    public long sessionEpoch() {
        return sessionEpoch;
    }

    /** Append one record. Identical {@code requestId} retries are safe. */
    public AppendResult append(UUID requestId, byte[] key, byte[] payload) {
        return append(new Append(requestId, key, payload));
    }

    /** Append one record with event time and Metadata. */
    public AppendResult append(Append record) {
        JsonNode body = Wire.appendRequest(
                record.requestId(), writer, sessionEpoch,
                record.key(), record.payload(), record.eventTimeNs(), record.metadata());
        return Wire.appendResult(transport.post("/v1/writers/append", body));
    }

    /** Append a batch; per-record results plus aggregate feedback. */
    public BatchResult appendBatch(List<Append> records) {
        ArrayNode array = Transport.json().createArrayNode();
        for (Append record : records) {
            array.add(Wire.appendRequest(
                    record.requestId(), writer, sessionEpoch,
                    record.key(), record.payload(), record.eventTimeNs(), record.metadata()));
        }
        var body = Transport.json().createObjectNode();
        body.set("records", array);
        JsonNode response = transport.post("/v1/writers/append-batch", body);
        List<AppendResult> results = new ArrayList<>(response.path("results").size());
        response.path("results").forEach(node -> results.add(Wire.appendResult(node)));
        return new BatchResult(List.copyOf(results), Wire.feedback(response.path("feedback")));
    }

    public CompletableFuture<AppendResult> appendAsync(UUID requestId, byte[] key, byte[] payload) {
        return CompletableFuture.supplyAsync(() -> append(requestId, key, payload), executor);
    }
}
