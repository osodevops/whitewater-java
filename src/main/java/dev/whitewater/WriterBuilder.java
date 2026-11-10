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

    WriterBuilder(WhitewaterClient client, Feed<K, V> feed) {
        this.client = Objects.requireNonNull(client);
        this.feed = Objects.requireNonNull(feed);
    }

    /** The durable Writer identity. Required before
     * {@link #open()}/{@link #declare()}. Resolves inside the Feed's
     * Domain: {@code writerId("ingest")} on {@code shop.orders} binds
     * {@code shop.ingest}. */
    public WriterBuilder<K, V> writerId(String writerId) {
        this.writerId = feed.scope(
                Objects.requireNonNull(writerId, "writerId is required"));
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
        return new FeedWriter<>(client.writer(id), feed.keyCodec(), feed.valueCodec());
    }

    private String requireId() {
        if (writerId == null) {
            throw new IllegalStateException("writerId(...) is required");
        }
        return writerId;
    }
}
