package dev.whitewater;

import dev.whitewater.model.ControlExecution;
import dev.whitewater.model.ReaderStart;

import java.util.Objects;

/**
 * Fluent declaration + open of a durable named Reader — the typed
 * companion to {@link WhitewaterClient#createReader(Domain, Feed)}:
 *
 * <pre>{@code
 * import static dev.whitewater.model.ReaderStart.*;
 *
 * Domain shop = client.domain("shop");
 * try (var reader = client.createReader(shop, shop.getFeed("orders"))
 *         .readerId("fulfillment")            // the Kafka groupId analogue
 *         .readerStartPoint(Beginning)        // first-time start — like auto.offset.reset
 *         .capacity(256)
 *         .open()) {
 *     for (var page = reader.fetch(); !page.records().isEmpty(); page = reader.fetch()) {
 *         page.records().forEach(m -> handle(m.value()));
 *         reader.ack();
 *     }
 * }
 * }</pre>
 *
 * <p>{@code readerId} is the durable consume identity — progress the
 * Reader acknowledges survives restarts and resumes under the same
 * name. {@code readerStartPoint} applies only when the Reader is first
 * declared: reopening an existing Reader resumes its acknowledged
 * frontier, exactly like Kafka's {@code auto.offset.reset}. If the id
 * is already declared its existing start wins.
 */
public final class ReaderBuilder<K, V> {

    private final WhitewaterClient client;
    private final Feed<K, V> feed;
    private String readerId;
    private ReaderStart start = ReaderStart.beginning();
    private int capacity = 128;

    ReaderBuilder(WhitewaterClient client, Feed<K, V> feed) {
        this.client = Objects.requireNonNull(client);
        this.feed = Objects.requireNonNull(feed);
    }

    /**
     * The durable Reader identity — the Kafka {@code groupId}
     * analogue. Required before {@link #open()}/{@link #declare()}.
     */
    public ReaderBuilder<K, V> readerId(String readerId) {
        this.readerId = Objects.requireNonNull(readerId, "readerId is required");
        return this;
    }

    /**
     * First-time start position — {@link ReaderStart#Beginning},
     * {@link ReaderStart#End}, {@code cursor(c)}, or
     * {@code timestamp(instant)}. Applies only when the Reader is
     * first declared; an existing Reader resumes its acknowledged
     * frontier.
     */
    public ReaderBuilder<K, V> readerStartPoint(ReaderStart start) {
        this.start = Objects.requireNonNull(start, "start is required");
        return this;
    }

    /** Bounded fetch page capacity — the default is 128. */
    public ReaderBuilder<K, V> capacity(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.capacity = capacity;
        return this;
    }

    /**
     * Declare the Reader in the catalog without opening a session —
     * fails when the name is already declared.
     */
    public ControlExecution declare() {
        return client.createReader(requireId(), feed.name(), start);
    }

    /**
     * Declare the Reader when missing (an existing declaration keeps
     * its own start and durable progress), open a session, and bind
     * the Feed's codecs — {@link FeedReader#fetch} returns decoded
     * {@link dev.whitewater.model.FeedMessage}s.
     */
    public FeedReader<K, V> open() {
        var id = requireId();
        feed.ensureReader(id, start);
        return new FeedReader<>(client.reader(id, capacity), feed.keyCodec(), feed.valueCodec());
    }

    private String requireId() {
        if (readerId == null) {
            throw new IllegalStateException(
                    "readerId(...) is required — the durable consume identity"
                            + " (the Kafka groupId analogue)");
        }
        return readerId;
    }
}
