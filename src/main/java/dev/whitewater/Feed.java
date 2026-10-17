package dev.whitewater;

import dev.whitewater.model.AppendResult;
import dev.whitewater.model.Codec;
import dev.whitewater.model.Codecs;
import dev.whitewater.model.Cursor;
import dev.whitewater.model.FeedMessage;
import dev.whitewater.model.FeedPage;
import dev.whitewater.model.FeedRecord;
import dev.whitewater.model.ReaderStart;
import dev.whitewater.model.TemporaryFetchOptions;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * A typed facade over one Feed — the immutable temporal history on the
 * Nodes. The bound {@link Codec}s serialize on write and deserialize on
 * read, so application code stays in its own types:
 *
 * <pre>{@code
 * Feed<String, Order> orders = client.feed("orders.events", Order.class);
 *
 * FeedWriter<String, Order> writer = orders.writer("orders.ingest");
 * writer.append("order-1", order);                    // encoded for you
 *
 * try (FeedReader<String, Order> reader = orders.reader("orders.audit", 64)) {
 *     for (var page = reader.fetch(); !page.records().isEmpty(); page = reader.fetch()) {
 *         page.records().forEach(m -> handle(m.value()));
 *         reader.ack();
 *     }
 * }
 * }</pre>
 *
 * <p>{@link WhitewaterClient#feed(String)} binds String keys with raw
 * {@code byte[]} payloads; pass a payload {@link Codec} or class (or
 * key + payload Codecs) for objects. {@link #view} rebinds the same
 * feed under different Codecs — {@code feed.view(String.class, byte[].class)}
 * is the escape hatch to raw bytes.
 */
public final class Feed<K, V> {

    private final WhitewaterClient client;
    private final FeedClient feedOps;
    private final String name;
    private final Codec<K> keyCodec;
    private final Codec<V> valueCodec;

    private Feed(
            WhitewaterClient client,
            FeedClient feedOps,
            String name,
            Codec<K> keyCodec,
            Codec<V> valueCodec) {
        this.client = Objects.requireNonNull(client);
        this.feedOps = Objects.requireNonNull(feedOps);
        this.name = Objects.requireNonNull(name, "name");
        this.keyCodec = Objects.requireNonNull(keyCodec, "keyCodec");
        this.valueCodec = Objects.requireNonNull(valueCodec, "valueCodec");
    }

    static <K, V> Feed<K, V> bound(
            WhitewaterClient client, String name, Codec<K> keyCodec, Codec<V> valueCodec) {
        return new Feed<>(
                client, new FeedClient(client.transport(), client.executor()),
                WhitewaterClient.dottedName(name), keyCodec, valueCodec);
    }

    /** The Feed's dotted name. */
    public String name() {
        return name;
    }

    public Codec<K> keyCodec() {
        return keyCodec;
    }

    public Codec<V> valueCodec() {
        return valueCodec;
    }

    /** Rebind the same Feed under different Codecs — no wire call. */
    public <K2, V2> Feed<K2, V2> view(Codec<K2> keyCodec, Codec<V2> valueCodec) {
        return new Feed<>(client, feedOps, name, keyCodec, valueCodec);
    }

    /** {@link #view(Codec, Codec)} by classes. */
    public <K2, V2> Feed<K2, V2> view(Class<K2> keyType, Class<V2> valueType) {
        return view(Codecs.forClass(keyType), Codecs.forClass(valueType));
    }

    /** Wire compression on fetches and ad-hoc appends through this facade. */
    public Feed<K, V> useCompression() {
        return new Feed<>(client, feedOps.useCompression(), name, keyCodec, valueCodec);
    }

    // ---- reading ---------------------------------------------------------

    /**
     * Committed records from the beginning or after an opaque
     * {@code after} Cursor, decoded through the bound Codecs.
     */
    public List<FeedMessage<K, V>> records(Integer limit, Cursor after) {
        return feedOps.records(name, limit, after).stream()
                .map(this::decode)
                .toList();
    }

    /** {@link #records(Integer, Cursor)} from the beginning, unbounded. */
    public List<FeedMessage<K, V>> records() {
        return records(null, null);
    }

    /**
     * Anonymous temporary Reader fetch: decoded records plus the opaque
     * continuation frontier. Retains no server-side progress.
     */
    public FeedPage<K, V> fetch(TemporaryFetchOptions options) {
        var page = feedOps.temporaryFetch(name, options);
        return new FeedPage<>(
                page.records().stream().map(this::decode).toList(),
                page.nextCursor());
    }

    /** {@link #fetch(TemporaryFetchOptions)} for a bounded page after {@code after}. */
    public FeedPage<K, V> fetch(Cursor after, int limit) {
        return fetch(TemporaryFetchOptions.page(after, limit));
    }

    /**
     * Bulk-read committed history once, from {@code start}, delivering
     * each decoded page to {@code sink} until the frontier is drained.
     */
    public long load(ReaderStart start, Consumer<List<FeedMessage<K, V>>> sink) {
        Objects.requireNonNull(sink, "sink");
        return feedOps.load(name, start,
                records -> sink.accept(records.stream().map(this::decode).toList()));
    }

    // ---- writing ---------------------------------------------------------

    /**
     * A session-bound Writer on this Feed: declares the Writer when
     * missing (reusing an existing one), opens a session, and binds the
     * feed's Codecs so {@code writer.append(key, value)} just works.
     */
    public FeedWriter<K, V> writer(String writerName) {
        ensureWriter(writerName);
        return new FeedWriter<>(client.writer(writerName), keyCodec, valueCodec);
    }

    /**
     * Ad-hoc single append with a caller-owned writer identity
     * ({@code writerSessionId} + {@code writerEpoch} + {@code sequence}
     * ordering) — codecs serialize key/payload. For durable named
     * producers prefer {@link #writer(String)}.
     */
    public AppendResult append(
            UUID writerSessionId, long writerEpoch, long sequence, K key, V value) {
        return client.feeds().append(name, writerSessionId, writerEpoch, sequence,
                key == null ? null : keyCodec.encode(key),
                value == null ? null : valueCodec.encode(value));
    }

    // ---- readers / subscriptions ------------------------------------------

    /**
     * A session-bound named Reader on this Feed: declares the Reader
     * (at BEGINNING) when missing, opens a session with the bounded
     * fetch capacity, and returns decoded {@link FeedMessage}s.
     */
    public FeedReader<K, V> reader(String readerName, int capacity) {
        ensureReader(readerName);
        return new FeedReader<>(client.reader(readerName, capacity), keyCodec, valueCodec);
    }

    /**
     * A running subscription on this Feed: a named Reader polled on a
     * background thread, delivering decoded batches to {@code onRecords}
     * and acknowledging durable progress after each. See
     * {@link WhitewaterClient#subscribe} for the error policy.
     */
    public Subscription subscribe(
            String reader,
            ReaderStart start,
            Consumer<List<FeedMessage<K, V>>> onRecords,
            Consumer<WhitewaterException> onError) {
        return subscribeTyped(reader, start, Subscription.DEFAULT_CAPACITY,
                onRecords, onError);
    }

    /** {@link #subscribe(String, ReaderStart, Consumer, Consumer)} without an error sink. */
    public Subscription subscribe(
            String reader,
            ReaderStart start,
            Consumer<List<FeedMessage<K, V>>> onRecords) {
        return subscribe(reader, start, onRecords, null);
    }

    /**
     * A permanent, supervised consumer bound to this feed's named
     * Reader — per-record delivery, internal restart/backoff, durable
     * progress, graceful {@link FeedConsumer#close}. See {@link FeedConsumer}.
     */
    public FeedConsumer<K, V> consumer(String reader) {
        return new FeedConsumer<>(this, reader);
    }

    Subscription subscribeTyped(
            String reader,
            ReaderStart start,
            int capacity,
            Consumer<List<FeedMessage<K, V>>> onRecords,
            Consumer<WhitewaterException> onError) {
        return client.subscribe(reader, name, start, capacity,
                records -> onRecords.accept(
                        records.stream().map(this::decode).toList()),
                onError);
    }

    // ---- internals ----------------------------------------------------------

    FeedMessage<K, V> decode(FeedRecord record) {
        return FeedMessage.decode(record, keyCodec, valueCodec);
    }

    /** Declare the Writer when missing; an existing Writer is reused as-is. */
    private void ensureWriter(String writerName) {
        try {
            client.createWriter(writerName, name);
        } catch (WhitewaterException.Refused | WhitewaterException.Unavailable e) {
            // The server can surface "already exists" on either subtype
            // depending on whether the Control Plane verdict or the local
            // catalog answered.
            if (!String.valueOf(e.getMessage()).contains("exists")) {
                throw e;
            }
        }
    }

    /** Declare the Reader when missing; an existing Reader is reused as-is. */
    private void ensureReader(String readerName) {
        try {
            client.createReader(readerName, name);
        } catch (WhitewaterException.Refused | WhitewaterException.Unavailable e) {
            if (!String.valueOf(e.getMessage()).contains("exists")) {
                throw e;
            }
        }
    }
}
