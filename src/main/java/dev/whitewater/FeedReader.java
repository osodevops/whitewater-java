package dev.whitewater;

import dev.whitewater.model.Codec;
import dev.whitewater.model.Cursor;
import dev.whitewater.model.FeedMessage;
import dev.whitewater.model.FeedReaderPage;
import dev.whitewater.model.ReaderPage;
import dev.whitewater.model.ReaderSession;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * A session-bound named Reader with the feed's {@link Codec}s applied —
 * {@link #fetch} returns a bounded page of decoded {@link FeedMessage}s;
 * {@link #ack} commits the page's delivered Cursor durably.
 *
 * <pre>{@code
 * try (FeedReader<String, Order> reader = orders.reader("orders.audit", 64)) {
 *     for (var page = reader.fetch(); !page.records().isEmpty(); page = reader.fetch()) {
 *         for (FeedMessage<String, Order> m : page.records()) {
 *             handle(m.value());
 *         }
 *         reader.ack();               // commits the page's frontier
 *     }
 * }
 * }</pre>
 *
 * <p>Delivery is at-least-once: a replay between delivered and
 * acknowledged re-delivers the overlap. Reopening the named Reader
 * resumes at the last acknowledged position.
 */
public final class FeedReader<K, V> implements AutoCloseable {

    private final Reader reader;
    private final Codec<K> keyCodec;
    private final Codec<V> valueCodec;

    FeedReader(Reader reader, Codec<K> keyCodec, Codec<V> valueCodec) {
        this.reader = Objects.requireNonNull(reader);
        this.keyCodec = Objects.requireNonNull(keyCodec, "keyCodec");
        this.valueCodec = Objects.requireNonNull(valueCodec, "valueCodec");
    }

    /** The bound Reader's name. */
    public String reader() {
        return reader.reader();
    }

    /** The session's fencing epoch. */
    public long sessionEpoch() {
        return reader.sessionEpoch();
    }

    /** The Cursor the last {@link #ack} durably committed, or null. */
    public Cursor acknowledgedCursor() {
        return reader.acknowledgedCursor();
    }

    /** Fetch up to {@code limit} committed records after the frontier, decoded. */
    public FeedReaderPage<K, V> fetch(Integer limit) {
        return decode(reader.fetch(limit));
    }

    /** Fetch with the session's configured capacity. */
    public FeedReaderPage<K, V> fetch() {
        return decode(reader.fetch());
    }

    /**
     * Durably commit the last fetched page's delivered Cursor. Call after
     * processing a page; anything unacknowledged may replay.
     */
    public ReaderSession ack() {
        return reader.ack();
    }

    /** Durably commit an explicit Cursor. */
    public ReaderSession ack(Cursor cursor) {
        return reader.ack(cursor);
    }

    /** Close the session; durable acknowledged progress survives. */
    @Override
    public void close() {
        reader.close();
    }

    /** The underlying wire-level Reader — escape hatch for raw pages. */
    public Reader raw() {
        return reader;
    }

    public CompletableFuture<FeedReaderPage<K, V>> fetchAsync(Integer limit) {
        return CompletableFuture.supplyAsync(() -> fetch(limit), reader.executor());
    }

    private FeedReaderPage<K, V> decode(ReaderPage page) {
        return new FeedReaderPage<>(
                page.records().stream()
                        .map(r -> FeedMessage.decode(r, keyCodec, valueCodec))
                        .toList(),
                page.deliveredCursor(),
                page.acknowledgedCursor(),
                page.recommendedCapacity(),
                page.retryAfterMs());
    }
}
