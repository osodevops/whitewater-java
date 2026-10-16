package dev.whitewater;

import com.fasterxml.jackson.databind.JsonNode;
import dev.whitewater.internal.Transport;
import dev.whitewater.internal.Wire;
import dev.whitewater.model.AppendResult;
import dev.whitewater.model.ServerFeedback;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A session-bound Writer: the {@code session_epoch} and per-record
 * {@code request_id} identities stay inside the handle, so appending is
 * {@code writer.append(key, payload)} — no epoch plumbing, no request-ID
 * bookkeeping.
 *
 * <pre>{@code
 * var writer = client.writer("orders.ingest");            // opens a session
 * writer.append("order-1".getBytes(), payload);           // auto request_id
 * writer.append(new Writer.Record("order-2".getBytes(), payload)); // + event time
 * }</pre>
 *
 * <p>Retry an ambiguous append by repeating the same call after
 * {@link WhitewaterException.AmbiguousResult}/{@link WhitewaterException.Unavailable};
 * the record's request identity is stable inside the handle only when you
 * supply it — use {@link Record#Record(UUID, byte[], byte[])} to pin one.
 * On {@link WhitewaterException.StaleSession} the session was fenced
 * (another client opened the Writer); call {@link #reopen()} to bind a
 * fresh epoch.
 */
public final class Writer {

    /** One record: request identity, key, payload, optional event time and Metadata. */
    public record Record(
            UUID requestId,
            byte[] key,
            byte[] payload,
            Long eventTimeNs,
            Map<String, byte[]> metadata) {

        /** Auto-identified record with the current ingest clock as event time. */
        public Record(byte[] key, byte[] payload) {
            this(null, key, payload, null, null);
        }

        /** Explicit request identity for retryable writes. */
        public Record(UUID requestId, byte[] key, byte[] payload) {
            this(requestId, key, payload, null, null);
        }
    }

    /** Batch result: per-record results plus server batching guidance. */
    public record BatchResult(List<AppendResult> results, ServerFeedback feedback) {}

    private final Transport transport;
    private final Executor executor;
    private final String writer;
    private volatile long sessionEpoch;
    private boolean compression;

    Writer(Transport transport, Executor executor, String writer) {
        this.transport = transport;
        this.executor = executor;
        this.writer = Objects.requireNonNull(writer);
    }

    /** Open the session and bind this Writer to the issued epoch. */
    Writer open() {
        reopen();
        return this;
    }

    /**
     * Reopen the session after {@link WhitewaterException.StaleSession}:
     * issues a fresh fencing epoch and rebinds the handle.
     */
    public synchronized void reopen() {
        var body = Transport.json().createObjectNode();
        body.put("request_id", UUID.randomUUID().toString());
        var commands = body.putArray("commands");
        commands.addObject()
                .put("command", "open_writer_session")
                .put("writer", writer);
        JsonNode epoch = transport.post("/v1/admin/commands", body)
                .path("results").path(0).path("data").path("session_epoch");
        if (!epoch.isNumber()) {
            throw new IllegalStateException(
                    "open_writer_session returned no session_epoch for " + writer);
        }
        this.sessionEpoch = epoch.asLong();
    }

    public String writer() {
        return writer;
    }

    /** The session's fencing epoch. */
    public long sessionEpoch() {
        return sessionEpoch;
    }

    /** Enable wire compression on subsequent appends. */
    public Writer useCompression() {
        this.compression = true;
        return this;
    }

    /** Append one record; request identity and event time auto-generate. */
    public AppendResult append(byte[] key, byte[] payload) {
        return append(new Record(key, payload));
    }

    /** Append one record with explicit request identity (retry-safe). */
    public AppendResult append(Record record) {
        UUID requestId = record.requestId() == null ? UUID.randomUUID() : record.requestId();
        var body = Wire.appendRequest(requestId, writer, sessionEpoch,
                record.key(), record.payload(), record.eventTimeNs(), record.metadata());
        if (compression) {
            body.put("use_compression", true);
        }
        return Wire.appendResult(transport.post("/v1/writers/append", body));
    }

    /** Append a batch under one request; per-record results + batching guidance. */
    public BatchResult appendBatch(List<Record> records) {
        var array = Transport.json().createArrayNode();
        for (Record record : records) {
            UUID requestId = record.requestId() == null ? UUID.randomUUID() : record.requestId();
            var node = Wire.appendRequest(requestId, writer, sessionEpoch,
                    record.key(), record.payload(), record.eventTimeNs(), record.metadata());
            if (compression) {
                node.put("use_compression", true);
            }
            array.add(node);
        }
        var body = Transport.json().createObjectNode();
        body.set("records", array);
        JsonNode response = transport.post("/v1/writers/append-batch", body);
        List<AppendResult> results = new java.util.ArrayList<>(response.path("results").size());
        response.path("results").forEach(node -> results.add(Wire.appendResult(node)));
        return new BatchResult(List.copyOf(results), Wire.feedback(response.path("feedback")));
    }

    public CompletableFuture<AppendResult> appendAsync(byte[] key, byte[] payload) {
        return CompletableFuture.supplyAsync(() -> append(key, payload), executor);
    }
}
