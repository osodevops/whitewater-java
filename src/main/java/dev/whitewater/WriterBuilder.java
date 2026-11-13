package dev.whitewater;

import dev.whitewater.model.ControlExecution;

import java.util.Objects;

/**
 * Fluent declaration + session-open of a durable named Writer — the
 * typed companion to {@link Domain#createWriter(Feed)}:
 *
 * <pre>{@code
 * Domain shop = client.domain("shop");
 * var writer = shop.createWriter(shop.getFeed("orders"))
 *         .writerId("ingest")
 *         .open();
 * writer.append("order-1", order);             // encoded by the Feed's codecs
 * }</pre>
 */
public final class WriterBuilder<K, V> {

    private final WhitewaterClient client;
    private final Feed<K, V> feed;
    private String writerId;
    private java.time.Duration linger = FeedWriter.DEFAULT_LINGER;
    private int maxBatchRecords = FeedWriter.DEFAULT_MAX_BATCH_RECORDS;

    WriterBuilder(WhitewaterClient client, Feed<K, V> feed) {
        this.client = Objects.requireNonNull(client);
        this.feed = Objects.requireNonNull(feed);
    }

    /** The durable Writer identity — required before
     * {@link #open()}/{@link #declare()} because it carries the
     * transactional guarantees: the session fencing epoch binds to it
     * (a second opener fences the first, so a partitioned zombie can't
     * double-commit) and the retry-dedup ledger lives under it across
     * restarts. Resolves inside the Feed's Domain:
     * {@code writerId("ingest")} on {@code shop.orders} binds
     * {@code shop.ingest}. */
    public WriterBuilder<K, V> writerId(String writerId) {
        this.writerId = feed.scope(
                Objects.requireNonNull(writerId, "writerId is required"));
        return this;
    }

    /** How long {@link FeedWriter#write} holds an unsent batch waiting
     * for records to coalesce — the throughput/latency tradeoff knob.
     * Default 5ms. */
    public WriterBuilder<K, V> linger(java.time.Duration linger) {
        this.linger = Objects.requireNonNull(linger, "linger");
        return this;
    }

    /** Most records {@link FeedWriter#write} coalesces into one commit —
     * hitting the bound flushes immediately. Default 256. */
    public WriterBuilder<K, V> maxBatchRecords(int maxBatchRecords) {
        if (maxBatchRecords < 1) {
            throw new IllegalArgumentException("maxBatchRecords must be >= 1");
        }
        this.maxBatchRecords = maxBatchRecords;
        return this;
    }

    /**
     * Declare the Writer in the catalog without opening a session —
     * fails when the name is already declared.
     */
    public ControlExecution declare() {
        return client.createWriter(requireId(), feed.name());
    }

    /**
     * Declare the Writer when missing, open a session (the fencing
     * epoch stays inside the handle), and bind the Feed's codecs —
     * {@link FeedWriter#append} encodes keys/payloads.
     */
    public FeedWriter<K, V> open() {
        var id = requireId();
        feed.ensureWriter(id);
        var writer = client.writer(id);
        writer.ensureOpen();    // the .open() contract: bind the epoch now, fail fast
        return new FeedWriter<>(writer, feed.keyCodec(), feed.valueCodec(), linger, maxBatchRecords);
    }

    private String requireId() {
        if (writerId == null) {
            throw new IllegalStateException(
                    "writerId(...) is required — it anchors session fencing and retry dedup");
        }
        return writerId;
    }
}
