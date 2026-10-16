package dev.whitewater;

import dev.whitewater.internal.Wire;
import dev.whitewater.model.AppendResult;
import dev.whitewater.model.Codec;
import dev.whitewater.model.FeedEvent;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * A session-bound Writer with the feed's {@link Codec}s applied — the
 * {@code session_epoch} and per-record {@code request_id} identities
 * stay inside the handle, and keys/payloads serialize automatically:
 *
 * <pre>{@code
 * FeedWriter<String, Order> writer = orders.writer("orders.ingest");
 * writer.append("order-1", order);                    // encoded for you
 * writer.append(new FeedEvent<>("order-2", order2)
 *         .withTextMetadata(Map.of("tenant", "acme")));
 * }</pre>
 *
 * <p>Retry an ambiguous append by repeating the same call after
 * {@link WhitewaterException.AmbiguousResult}/{@link WhitewaterException.Unavailable};
 * pin a request identity with {@link FeedEvent#FeedEvent(UUID, Object, Object)}
 * to keep retries deduplicating. On {@link WhitewaterException.StaleSession}
 * the session was fenced (another client opened the Writer); call
 * {@link #reopen()} to bind a fresh epoch.
 */
public final class FeedWriter<K, V> {

    private final Writer writer;
    private final Codec<K> keyCodec;
    private final Codec<V> valueCodec;

    FeedWriter(Writer writer, Codec<K> keyCodec, Codec<V> valueCodec) {
        this.writer = Objects.requireNonNull(writer);
        this.keyCodec = Objects.requireNonNull(keyCodec, "keyCodec");
        this.valueCodec = Objects.requireNonNull(valueCodec, "valueCodec");
    }

    /** The bound Writer's name. */
    public String writer() {
        return writer.writer();
    }

    /** The session's fencing epoch. */
    public long sessionEpoch() {
        return writer.sessionEpoch();
    }

    /** Reopen after {@link WhitewaterException.StaleSession}; binds a fresh epoch. */
    public synchronized void reopen() {
        writer.reopen();
    }

    /** Enable wire compression on subsequent appends. */
    public FeedWriter<K, V> useCompression() {
        writer.useCompression();
        return this;
    }

    /** The underlying wire-level Writer — escape hatch for raw bytes. */
    public Writer raw() {
        return writer;
    }

    /** Append one record; codecs serialize key/payload, identity auto-generates. */
    public AppendResult append(K key, V value) {
        return append(new FeedEvent<>(key, value));
    }

    /** {@link #append(Object, Object)} with Metadata. */
    public AppendResult append(K key, V value, Map<String, byte[]> metadata) {
        return append(new FeedEvent<>(null, key, value, null, metadata));
    }

    /** Append one record with explicit request identity (retry-safe). */
    public AppendResult append(FeedEvent<K, V> event) {
        return writer.append(toWire(event));
    }

    /** Append a batch under one request; per-record results + batching guidance. */
    public Writer.BatchResult appendBatch(List<FeedEvent<K, V>> events) {
        List<Writer.Record> records = events.stream().map(this::toWire).toList();
        return writer.appendBatch(records);
    }

    /** Alias for {@link #appendBatch(List)}. */
    public Writer.BatchResult appendAll(List<FeedEvent<K, V>> events) {
        return appendBatch(events);
    }

    public CompletableFuture<AppendResult> appendAsync(K key, V value) {
        return CompletableFuture.supplyAsync(() -> append(key, value), writer.executor());
    }

    private Writer.Record toWire(FeedEvent<K, V> event) {
        return new Writer.Record(
                event.requestId(),
                event.key() == null ? null : keyCodec.encode(event.key()),
                event.value() == null ? null : valueCodec.encode(event.value()),
                event.eventTimeNs(),
                event.metadata());
    }
}
