package dev.whitewater;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.whitewater.internal.Transport;
import dev.whitewater.internal.Wire;
import dev.whitewater.model.AppendResult;
import dev.whitewater.model.Cursor;
import dev.whitewater.model.FeedAppend;
import dev.whitewater.model.FeedRecord;
import dev.whitewater.model.TemporaryFetchOptions;
import dev.whitewater.model.TemporaryFetchPage;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Feed-level ad-hoc operations: committed record reads, anonymous temporary
 * Reader fetches, and single-shot appends with a caller-supplied writer
 * identity. For durable named producers use {@link WriterSessionClient}; for
 * durable progress use {@link ReaderSessionClient}.
 */
public final class FeedClient {

    private final Transport transport;
    private final Executor executor;

    FeedClient(Transport transport, Executor executor) {
        this.transport = transport;
        this.executor = executor;
    }

    /**
     * Committed records on a Feed, from the beginning or after an opaque
     * {@code after} Cursor.
     */
    public List<FeedRecord> records(String feed, Integer limit, Cursor after) {
        StringBuilder path = new StringBuilder("/v1/feeds/records?feed=").append(encode(feed));
        if (limit != null && limit > 0) {
            path.append("&limit=").append(limit);
        }
        if (after != null && after.value() != null && !after.value().isBlank()) {
            path.append("&after=").append(encode(after.value()));
        }
        return Wire.records(transport.get(path.toString()));
    }

    /**
     * Anonymous temporary Reader fetch: records plus the opaque continuation
     * frontier. Retains no server-side progress — resume by passing
     * {@link TemporaryFetchPage#nextCursor()} back as
     * {@link TemporaryFetchOptions#after()}.
     */
    public TemporaryFetchPage temporaryFetch(String feed, TemporaryFetchOptions options) {
        Objects.requireNonNull(feed, "feed");
        Objects.requireNonNull(options, "options");
        ObjectNode body = Transport.json().createObjectNode();
        body.put("feed", feed);
        if (options.after() != null && options.after().value() != null) {
            body.put("after", options.after().value());
        }
        if (options.limit() != null) {
            body.put("limit", options.limit());
        }
        if (options.newOnly() != null) {
            body.put("new_only", options.newOnly());
        }
        if (options.tail() != null) {
            body.put("tail", options.tail());
        }
        if (options.waitMs() != null) {
            body.put("wait_ms", options.waitMs());
        }
        if (options.afterEventTimeNs() != null) {
            body.put("after_event_time_ns", Long.toString(options.afterEventTimeNs()));
        }
        JsonNode response = transport.post("/v1/readers/temporary/fetch", body);
        JsonNode next = response.path("next_cursor");
        return new TemporaryFetchPage(
                Wire.records(response.path("records")),
                next.isNull() ? null : Cursor.of(next.asText()));
    }

    public TemporaryFetchPage temporaryFetch(String feed, Cursor after, int limit) {
        return temporaryFetch(feed, TemporaryFetchOptions.page(after, limit));
    }

    /**
     * Ad-hoc append: dedupes on {@code requestId}; the caller-owned writer
     * identity ({@code writerSessionId} + {@code writerEpoch} +
     * {@code sequence}) orders retries within an ephemeral session.
     */
    public AppendResult append(String feed, FeedAppend record) {
        Objects.requireNonNull(feed, "feed");
        Objects.requireNonNull(record, "record");
        return Wire.appendResult(transport.post(
                "/v1/feeds/append", Wire.feedAppendRequest(feed, record)));
    }

    /** Convenience single-record append. */
    public AppendResult append(String feed, UUID writerSessionId, long writerEpoch, long sequence,
                               byte[] key, byte[] payload) {
        return append(feed, new FeedAppend(
                UUID.randomUUID(), writerSessionId, writerEpoch, sequence,
                key, payload, null, Map.of()));
    }

    public CompletableFuture<TemporaryFetchPage> temporaryFetchAsync(
            String feed, TemporaryFetchOptions options) {
        return CompletableFuture.supplyAsync(() -> temporaryFetch(feed, options), executor);
    }

    private static String encode(String value) {
        return URLEncoder.encode(value == null ? "" : value, StandardCharsets.UTF_8);
    }
}
