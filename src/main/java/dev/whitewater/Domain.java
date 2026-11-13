package dev.whitewater;

import com.fasterxml.jackson.databind.JsonNode;
import dev.whitewater.admin.Commands;
import dev.whitewater.model.Codec;
import dev.whitewater.model.Codecs;
import dev.whitewater.model.ControlExecution;
import dev.whitewater.model.FeedSchema;
import dev.whitewater.model.ReaderStart;
import dev.whitewater.model.ResourceKind;
import dev.whitewater.model.ShowKind;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A strongly-typed handle on one Domain — the administrative namespace
 * that owns every Feed whose name it prefixes ({@code shop} owns
 * {@code shop.orders}). Two flavours:
 *
 * <pre>{@code
 * Domain shop = client.domain("shop");       // lazy handle — no I/O until an accessor
 * Domain shop = client.getDomain("shop");    // eager — verifies the Domain exists now
 * }</pre>
 *
 * <p>The first catalog accessor ({@link #feeds()}, {@link #readers()},
 * {@link #describe()}, …) materializes the Domain's catalog view in one
 * batched control read — the Domain row plus every SHOW listing — and
 * caches it. {@link #refresh()} re-pulls. {@link #feed(String)} is pure
 * name composition and never performs I/O, so
 * {@code client.createReader(domain, domain.feed("orders"))} costs no
 * extra round trips.
 */
public final class Domain {

    private final WhitewaterClient client;
    private final String name;
    private final ConcurrentHashMap<String, Feed<?, ?>> feedCache = new ConcurrentHashMap<>();
    private volatile Snapshot snapshot;

    Domain(WhitewaterClient client, String name) {
        this.client = Objects.requireNonNull(client);
        this.name = WhitewaterClient.dottedName(name);
    }

    /** The Domain's dotted name ({@code "shop"}). */
    public String name() {
        return name;
    }

    /** The Domain's immutable id — survives renames. Materializes the snapshot. */
    public UUID domainId() {
        return snapshot().domainId;
    }

    /** The catalog revision this snapshot was taken at. */
    public long revision() {
        return snapshot().revision;
    }

    /** The raw {@code DESCRIBE DOMAIN} row — status, created_at_ns, history_policy. */
    public JsonNode describe() {
        return snapshot().describe;
    }

    /** The declared History Policy row, or null when unset. */
    public JsonNode historyPolicy() {
        var policy = snapshot().describe.path("history_policy");
        return policy.isMissingNode() || policy.isNull() ? null : policy;
    }

    /**
     * The declared record schema default for this Domain — the
     * {@code SET SCHEMA ON DOMAIN} row, or null when undeclared. Feed
     * overrides layer over this via {@link #feedSchema(String)}.
     */
    public FeedSchema schema() {
        return FeedSchema.fromWire(snapshot().describe.path("schema"));
    }

    // ---- scoped resources ----------------------------------------------------

    /**
     * Every Feed in this Domain as typed {@link Feed} handles, sorted by
     * name. Materializes the snapshot.
     */
    public List<Feed<String, String>> feeds() {
        return feeds(null);
    }

    /** {@link #feeds()} filtered to relative names starting with {@code prefix}. */
    public List<Feed<String, String>> feeds(String prefix) {
        var result = new ArrayList<Feed<String, String>>();
        for (var feed : snapshot().feeds) {
            var relative = relativeName(feed);
            if (prefix == null || relative.startsWith(prefix)) {
                result.add(feed(relative));
            }
        }
        return List.copyOf(result);
    }

    /** Alias of {@link #feeds(String)}. */
    public List<Feed<String, String>> getFeeds(String prefix) {
        return feeds(prefix);
    }

    /** The Feed names in this Domain, sorted — the plain-string view of {@link #feeds()}. */
    public List<String> feedNames() {
        return snapshot().feeds;
    }

    /**
     * The effective record schema a Feed in this Domain carries — its
     * declared override layered over the Domain's — or null when
     * nothing is declared (schema-agnostic bytes). Materializes the
     * snapshot; the name resolves like {@link #feed(String)}.
     */
    public FeedSchema feedSchema(String relative) {
        return snapshot().effectiveSchemas.get(resolve(relative));
    }

    /**
     * The schema declared directly on the Feed, without Domain
     * inheritance — null when the Feed has no {@code SET SCHEMA}
     * override of its own.
     */
    public FeedSchema declaredFeedSchema(String relative) {
        return snapshot().declaredSchemas.get(resolve(relative));
    }

    /** Names of declared Readers on this Domain's Feeds, sorted. */
    public List<String> readers() {
        return snapshot().readers;
    }

    /** Names of declared Writers on this Domain's Feeds, sorted. */
    public List<String> writers() {
        return snapshot().writers;
    }

    /** Names of declared Subscriptions in this Domain, sorted. */
    public List<String> subscriptions() {
        return snapshot().subscriptions;
    }

    /** Names of declared Pipes in this Domain, sorted. */
    public List<String> pipes() {
        return snapshot().pipes;
    }

    /** Names of declared Indexes in this Domain, sorted. */
    public List<String> indexes() {
        return snapshot().indexes;
    }

    /** Names of declared StateStores in this Domain, sorted. */
    public List<String> stateStores() {
        return snapshot().stateStores;
    }

    /** Re-pull the catalog snapshot — the next accessor sees fresh rows. */
    public Domain refresh() {
        this.snapshot = load();
        return this;
    }

    // ---- Feed handles ---------------------------------------------------------

    /**
     * {@code CREATE FEED} inside this Domain: {@code shop.createFeed("orders")}
     * declares {@code shop.orders}. The next {@link #refresh()} (or a
     * lazy accessor's first load) sees the new Feed in {@link #feeds()}.
     */
    public ControlExecution createFeed(String relative) {
        return client.createFeed(resolve(relative));
    }

    /**
     * Fluent declaration + session-open of a durable named Reader on a
     * Feed inside this Domain — {@code shop.createReader("orders")}
     * binds {@code shop.orders}:
     * <pre>{@code
     * var reader = shop.createReader("orders")
     *         .readerId("fulfillment")          // the Kafka groupId analogue
     *         .readerStartPoint(ReaderStart.Beginning)
     *         .open();
     * }</pre>
     */
    public ReaderBuilder<String, String> createReader(String relative) {
        return client.createReader(this, feed(relative));
    }

    /** {@link #createReader(String)} on an explicit typed handle. */
    public <K, V> ReaderBuilder<K, V> createReader(Feed<K, V> feed) {
        return client.createReader(this, feed);
    }

    /**
     * Fluent declaration + session-open of a durable named Writer on a
     * Feed inside this Domain — {@code shop.createWriter("orders")}
     * binds {@code shop.orders}.
     */
    public WriterBuilder<String, String> createWriter(String relative) {
        return client.createWriter(this, feed(relative));
    }

    /** {@link #createWriter(String)} on an explicit typed handle. */
    public <K, V> WriterBuilder<K, V> createWriter(Feed<K, V> feed) {
        return client.createWriter(this, feed);
    }

    /**
     * A typed {@link Feed} handle on {@code relative} inside this
     * Domain: {@code shop.feed("orders")} binds {@code shop.orders}.
     * Passing an already-qualified name ({@code "shop.orders"}) is
     * accepted; a name outside this Domain is refused locally. Pure
     * name composition — no I/O.
     */
    public Feed<String, String> feed(String relative) {
        return feed(relative, Codecs.utf8(), Codecs.utf8());
    }

    /** Alias of {@link #feed(String)}. */
    public Feed<String, String> getFeed(String relative) {
        return feed(relative);
    }

    /** {@link #feed(String)} with String keys and payloads of {@code valueType}. */
    public <V> Feed<String, V> feed(String relative, Class<V> valueType) {
        return feed(relative, Codecs.forClass(valueType));
    }

    /** {@link #feed(String)} with String keys and a typed payload Codec. */
    public <V> Feed<String, V> feed(String relative, Codec<V> valueCodec) {
        return feed(relative, Codecs.utf8(), valueCodec);
    }

    /** {@link #feed(String)} with a typed key class too. */
    public <K, V> Feed<K, V> feed(String relative, Class<K> keyType, Class<V> valueType) {
        return feed(relative, Codecs.forClass(keyType), Codecs.forClass(valueType));
    }

    /** {@link #feed(String)} with explicit key and payload Codecs. */
    @SuppressWarnings("unchecked")
    public <K, V> Feed<K, V> feed(String relative, Codec<K> keyCodec, Codec<V> valueCodec) {
        var full = resolve(relative);
        return (Feed<K, V>) feedCache.computeIfAbsent(
                cacheKey(full, keyCodec, valueCodec),
                k -> Feed.bound(client, full, keyCodec, valueCodec));
    }

    /**
     * A lifecycle handle on the Pipe {@code relative} inside this
     * Domain — {@code ops.pipe("orders.relay")} binds
     * {@code ops.orders.relay}. {@link #resolve(String)} composes the
     * same name for {@code Commands.of()} calls that take full dotted
     * names.
     */
    public Pipe pipe(String relative) {
        return client.pipe(resolve(relative));
    }

    /**
     * A codec-bound {@link StateStore} facade on {@code relative}
     * inside this Domain — {@code ops.stateStore("customers")} binds
     * {@code ops.customers} with the same default codecs as
     * {@link WhitewaterClient#stateStore(String)}. Pass the full form
     * for typed payloads:
     * {@code client.stateStore(ops.resolve("customers"), Employee.class)}.
     */
    public StateStore<String, String> stateStore(String relative) {
        return client.stateStore(resolve(relative));
    }

    // ---- Writers / Readers ----------------------------------------------------

    /** {@code CREATE WRITER <name> TO <feed>} — both names resolve
     * inside this Domain. */
    public ControlExecution createWriter(String writer, String feed) {
        return client.createWriter(resolve(writer), resolve(feed));
    }

    /** {@code CREATE READER <name> FROM <feed>} — both names resolve
     * inside this Domain; {@code start} applies on first declare. */
    public ControlExecution createReader(String reader, String feed, ReaderStart start) {
        return client.createReader(resolve(reader), resolve(feed), start);
    }

    /** {@link #createReader(String, String, ReaderStart)} starting at BEGINNING. */
    public ControlExecution createReader(String reader, String feed) {
        return createReader(reader, feed, ReaderStart.beginning());
    }

    /** {@code OPEN WRITER SESSION <writer>} — the returned epoch feeds
     * {@link #writer(String, long)}. */
    public ControlExecution openWriterSession(String writer) {
        return client.openWriterSession(resolve(writer));
    }

    /** {@code SEEK READER <reader> TO <start>} — repositions a durable
     * Reader inside this Domain. */
    public ControlExecution seekReader(String reader, ReaderStart start) {
        return client.seekReader(resolve(reader), start);
    }

    /**
     * A session-bound Writer inside this Domain — the fencing epoch
     * binds lazily on the first append, so an idle handle holds no
     * epoch. The surface applications should use.
     */
    public Writer writer(String name) {
        return client.writer(resolve(name));
    }

    /**
     * A Writer session facade under an explicit {@code sessionEpoch}.
     * Prefer {@link #writer(String)}; this form is for callers managing
     * session state themselves.
     */
    public WriterSessionClient writerSession(String writer, long sessionEpoch) {
        return client.writerSession(resolve(writer), sessionEpoch);
    }

    /** A session-bound named Reader inside this Domain — opens the
     * session internally and tracks the delivered Cursor for
     * {@link Reader#ack()}. */
    public Reader reader(String name, int capacity) {
        return client.reader(resolve(name), capacity);
    }

    /** A named-Reader session facade inside this Domain under an
     * explicit epoch. */
    public ReaderSessionClient readerSession(String reader) {
        return client.readerSession(resolve(reader));
    }

    // ---- Subscriptions --------------------------------------------------------

    /** {@code CREATE SUBSCRIPTION <name> FROM <feed>} — both names
     * resolve inside this Domain: {@code pipes.createSubscription("audit",
     * "orders")} declares {@code pipes.audit} on {@code pipes.orders}. */
    public ControlExecution createSubscription(String name, String feed, ReaderStart start) {
        return client.createSubscription(resolve(name), resolve(feed), start);
    }

    /** {@link #createSubscription(String, String, ReaderStart)} starting at BEGINNING. */
    public ControlExecution createSubscription(String name, String feed) {
        return createSubscription(name, feed, ReaderStart.beginning());
    }

    /**
     * A {@link SubscriptionRef} handle on the declared Subscription
     * {@code relative} inside this Domain — open member sessions with
     * {@link SubscriptionRef#member()} or pass it to another Domain's
     * Pipe definition to drive from this Subscription.
     */
    public SubscriptionRef subscription(String relative) {
        return new SubscriptionRef(client, resolve(relative));
    }

    /** A cooperating member-session client on the Subscription
     * {@code relative} inside this Domain (raw epoch plumbing). */
    public SubscriptionMemberClient memberClient(String relative) {
        return client.subscriptionMember(resolve(relative));
    }

    /** A session-bound member of the Subscription {@code relative}
     * inside this Domain — joins internally. */
    public Member member(String relative) {
        return client.member(resolve(relative));
    }

    /** {@link #member(String)} under an explicit member identity
     * (crash-recovery). */
    public Member member(String relative, java.util.UUID memberId) {
        return client.member(resolve(relative), memberId);
    }

    /**
     * Subscribe durably to a Feed inside this Domain: ensures a Reader
     * named {@code reader} exists (creating it at {@code start} when
     * missing), opens a session, and invokes {@code onRecords} per
     * fetched page on a background thread, acknowledging after each
     * batch.
     */
    public Subscription subscribe(
            String reader,
            String feed,
            ReaderStart start,
            java.util.function.Consumer<java.util.List<dev.whitewater.model.FeedRecord>> onRecords,
            java.util.function.Consumer<WhitewaterException> onError) {
        return client.subscribe(resolve(reader), resolve(feed), start, onRecords, onError);
    }

    /** {@link #subscribe} with an explicit page capacity. */
    public Subscription subscribe(
            String reader,
            String feed,
            ReaderStart start,
            int capacity,
            java.util.function.Consumer<java.util.List<dev.whitewater.model.FeedRecord>> onRecords,
            java.util.function.Consumer<WhitewaterException> onError) {
        return client.subscribe(resolve(reader), resolve(feed), start, capacity, onRecords, onError);
    }

    /** {@link #subscribe} without an error callback. */
    public Subscription subscribe(
            String reader,
            String feed,
            ReaderStart start,
            java.util.function.Consumer<java.util.List<dev.whitewater.model.FeedRecord>> onRecords) {
        return client.subscribe(resolve(reader), resolve(feed), start, onRecords, null);
    }

    // ---- Catalog lifecycle -----------------------------------------------------

    /** {@code DESCRIBE FEED <feed>} inside this Domain. */
    public ControlExecution describeFeed(String relative) {
        return client.describeFeed(resolve(relative));
    }

    /** {@code DESCRIBE WRITER <name>} inside this Domain. */
    public ControlExecution describeWriter(String relative) {
        return client.describeWriter(resolve(relative));
    }

    /** {@code DESCRIBE READER <name>} inside this Domain. */
    public ControlExecution describeReader(String relative) {
        return client.describeReader(resolve(relative));
    }

    /** {@code DESCRIBE SUBSCRIPTION <name>} inside this Domain. */
    public ControlExecution describeSubscription(String relative) {
        return client.describeSubscription(resolve(relative));
    }

    /** {@code RENAME FEED <current> TO <new>} — both inside this Domain. */
    public ControlExecution renameFeed(String current, String renamed) {
        return client.renameFeed(resolve(current), resolve(renamed));
    }

    /** {@code RENAME WRITER <current> TO <new>} — both inside this Domain. */
    public ControlExecution renameWriter(String current, String renamed) {
        return client.renameWriter(resolve(current), resolve(renamed));
    }

    /** {@code RENAME READER <current> TO <new>} — both inside this Domain. */
    public ControlExecution renameReader(String current, String renamed) {
        return client.renameReader(resolve(current), resolve(renamed));
    }

    /** {@code RENAME SUBSCRIPTION <current> TO <new>} — both inside this Domain. */
    public ControlExecution renameSubscription(String current, String renamed) {
        return client.renameSubscription(resolve(current), resolve(renamed));
    }

    /** {@code DROP FEED <name>} inside this Domain. */
    public ControlExecution dropFeed(String relative) {
        return client.dropFeed(resolve(relative));
    }

    /** {@code DROP WRITER <name>} inside this Domain. */
    public ControlExecution dropWriter(String relative) {
        return client.dropWriter(resolve(relative));
    }

    /** {@code DROP READER <name>} inside this Domain. */
    public ControlExecution dropReader(String relative) {
        return client.dropReader(resolve(relative));
    }

    /** {@code DROP SUBSCRIPTION <name>} inside this Domain. */
    public ControlExecution dropSubscription(String relative) {
        return client.dropSubscription(resolve(relative));
    }

    // ---- Schemas ---------------------------------------------------------------

    /** {@code SET SCHEMA ON DOMAIN <name>} — this Domain's inherited
     * default. */
    public ControlExecution setSchema(dev.whitewater.model.FeedSchema schema) {
        return client.setDomainSchema(name, schema);
    }

    /** {@code DROP SCHEMA ON DOMAIN <name>} — clears the declared
     * default. */
    public ControlExecution clearSchema() {
        return client.clearDomainSchema(name);
    }

    /** {@code SET SCHEMA ON FEED <feed>} inside this Domain — overrides
     * the named sides. */
    public ControlExecution setFeedSchema(String relative, dev.whitewater.model.FeedSchema schema) {
        return client.setFeedSchema(resolve(relative), schema);
    }

    /** {@code DROP SCHEMA ON FEED <feed>} inside this Domain — reverts
     * to Domain inheritance. */
    public ControlExecution clearFeedSchema(String relative) {
        return client.clearFeedSchema(resolve(relative));
    }

    // ---- Pipes -------------------------------------------------------------------

    /**
     * {@code DEFINE PIPE <name> FROM <subscription> TO FEED <feed>} —
     * every name resolves inside this Domain.
     */
    public ControlExecution defineForwardPipe(
            String name, String subscription, String outputFeed) {
        return defineForwardPipe(
                name, Scoped.relative(subscription), Scoped.relative(outputFeed));
    }

    /**
     * {@link #defineForwardPipe(String, String, String)} with explicit
     * {@link ScopedRef} endpoints — a {@link Feed}/
     * {@link SubscriptionRef} handle or {@link Scoped#absolute} points
     * across Domains.
     */
    public ControlExecution defineForwardPipe(
            String name, ScopedRef subscription, ScopedRef outputFeed) {
        return client.commands(Commands.of().defineForwardPipe(
                resolve(name),
                subscription.resolve(this),
                outputFeed.resolve(this)));
    }

    /**
     * {@code DEFINE PIPE ... ENRICH FROM <store>} — point-lookup
     * enrichment; {@code quarantineFeed} resolves the miss-quarantine
     * target when the operation's miss disposition is {@code QUARANTINE}.
     * String arguments resolve inside this Domain.
     */
    public ControlExecution defineEnrichPipe(
            String name,
            String subscription,
            String outputFeed,
            String store,
            dev.whitewater.model.PipeOperation operation,
            String quarantineFeed) {
        return defineEnrichPipe(
                name,
                Scoped.relative(subscription),
                Scoped.relative(outputFeed),
                Scoped.relative(store),
                operation,
                quarantineFeed == null ? null : Scoped.relative(quarantineFeed));
    }

    /** {@link #defineEnrichPipe} with explicit {@link ScopedRef}
     * endpoints for cross-Domain references. */
    public ControlExecution defineEnrichPipe(
            String name,
            ScopedRef subscription,
            ScopedRef outputFeed,
            ScopedRef store,
            dev.whitewater.model.PipeOperation operation,
            ScopedRef quarantineFeed) {
        return client.commands(Commands.of().defineEnrichPipe(
                resolve(name),
                subscription.resolve(this),
                outputFeed.resolve(this),
                store.resolve(this),
                operation,
                quarantineFeed == null ? null : quarantineFeed.resolve(this)));
    }

    /**
     * {@code DEFINE PIPE ... TO STATESTORE <store>} — rolling-window
     * aggregation; {@code emitFeed} optionally emits closed panes and
     * requires the operation declare a lateness model. String arguments
     * resolve inside this Domain.
     */
    public ControlExecution defineWindowPipe(
            String name,
            String subscription,
            String outputStore,
            dev.whitewater.model.PipeOperation operation,
            String emitFeed) {
        return defineWindowPipe(
                name,
                Scoped.relative(subscription),
                Scoped.relative(outputStore),
                operation,
                emitFeed == null ? null : Scoped.relative(emitFeed));
    }

    /** {@link #defineWindowPipe} with explicit {@link ScopedRef}
     * endpoints for cross-Domain references. */
    public ControlExecution defineWindowPipe(
            String name,
            ScopedRef subscription,
            ScopedRef outputStore,
            dev.whitewater.model.PipeOperation operation,
            ScopedRef emitFeed) {
        return client.commands(Commands.of().defineWindowPipe(
                resolve(name),
                subscription.resolve(this),
                outputStore.resolve(this),
                operation,
                emitFeed == null ? null : emitFeed.resolve(this)));
    }

    /** {@code SHOW PIPES} rows scoped to this Domain. */
    public java.util.List<dev.whitewater.model.PipeInfo> pipeInfos() {
        var prefix = name + ".";
        var rows = new java.util.ArrayList<dev.whitewater.model.PipeInfo>();
        for (var info : client.pipes()) {
            if (info.name().startsWith(prefix)) {
                rows.add(info);
            }
        }
        return List.copyOf(rows);
    }

    // ---- StateStores -----------------------------------------------------------

    /** {@link #stateStore(String)} with String keys and payloads of
     * {@code payloadType}. */
    public <V> StateStore<String, V> stateStore(String relative, Class<V> payloadType) {
        return client.stateStore(resolve(relative), payloadType);
    }

    /** {@link #stateStore(String)} with String keys and a typed payload Codec. */
    public <V> StateStore<String, V> stateStore(String relative, Codec<V> payloadCodec) {
        return client.stateStore(resolve(relative), payloadCodec);
    }

    /** {@link #stateStore(String)} with a typed key class too. */
    public <K, V> StateStore<K, V> stateStore(
            String relative, Class<K> keyType, Class<V> payloadType) {
        return client.stateStore(resolve(relative), keyType, payloadType);
    }

    /** {@link #stateStore(String)} with explicit key and payload Codecs. */
    public <K, V> StateStore<K, V> stateStore(
            String relative, Codec<K> keyCodec, Codec<V> payloadCodec) {
        return client.stateStore(resolve(relative), keyCodec, payloadCodec);
    }

    /**
     * Declare {@code spec} inside this Domain — the spec's name resolves
     * in-domain. {@code source} names the Feed the store materializes
     * from (null = manual); a {@link Feed} handle or
     * {@link Scoped#absolute} sources it across Domains. Returns the
     * bound store facade.
     */
    public StateStore<String, String> defineStateStore(
            dev.whitewater.model.StateStoreSpec spec, ScopedRef source) {
        return client.stateStores().define(
                rescope(spec), source == null ? null : source.resolve(this));
    }

    /** {@link #defineStateStore(StateStoreSpec, ScopedRef)} with typed
     * payload binding. */
    public <K, V> StateStore<K, V> defineStateStore(
            dev.whitewater.model.StateStoreSpec spec,
            ScopedRef source,
            Codec<K> keyCodec,
            Codec<V> payloadCodec) {
        return client.stateStores().define(
                rescope(spec), source == null ? null : source.resolve(this),
                keyCodec, payloadCodec);
    }

    /** {@link #defineStateStore(StateStoreSpec, ScopedRef)} with a
     * plain relative/absolute source string. */
    public StateStore<String, String> defineStateStore(
            dev.whitewater.model.StateStoreSpec spec, String source) {
        return defineStateStore(
                spec, source == null ? null : Scoped.relative(source));
    }

    /** {@code DROP STATESTORE <name>} inside this Domain. */
    public ControlExecution dropStateStore(String relative) {
        return client.stateStores().drop(resolve(relative));
    }

    private dev.whitewater.model.StateStoreSpec rescope(
            dev.whitewater.model.StateStoreSpec spec) {
        var resolved = dev.whitewater.model.StateStoreSpec.named(resolve(spec.name()));
        spec.namedKeys().forEach(
                key -> resolved.addKey(key.name(), key.unique(), key.extractor()));
        return resolved;
    }

    // ---- Indexes -----------------------------------------------------------------

    /** Query the replicated Index {@code relative} inside this Domain
     * by predicate — exact, prefix, or range over typed values. */
    public dev.whitewater.model.IndexPage queryIndex(
            String relative,
            dev.whitewater.model.IndexPredicate predicate,
            Integer limit,
            boolean strict,
            Long waitMs) {
        return client.queryIndex(resolve(relative), predicate, limit, strict, waitMs);
    }

    /** {@link #queryIndex} with defaults. */
    public dev.whitewater.model.IndexPage queryIndex(
            String relative, dev.whitewater.model.IndexPredicate predicate) {
        return client.queryIndex(resolve(relative), predicate);
    }

    /**
     * The raw byte-level feed client — committed records, anonymous
     * temporary fetches, and ad-hoc appends by absolute feed name.
     * The protocol escape hatch under the Domain door; prefer the
     * bound {@link Feed} handles for application work.
     */
    public FeedClient feedOps() {
        return client.feeds();
    }

    /**
     * A {@link Scoped} reference naming a resource inside this Domain —
     * for Pipe endpoints owned elsewhere, pass a handle from the other
     * Domain or {@link Scoped#absolute}.
     */
    public Scoped scoped(String relative) {
        return Scoped.absolute(resolve(relative));
    }

    /**
     * Resolve {@code relative} to a full dotted resource name inside
     * this Domain — accepts either the short form ({@code "orders"})
     * or the already-qualified form ({@code "shop.orders"}). Deeper
     * names compose the same way: {@code ops.resolve("orders.hourly")}
     * → {@code ops.orders.hourly}. Every result lives under the
     * Domain's prefix by construction; a name that fails the dotted
     * grammar is refused here, locally.
     */
    public String resolve(String relative) {
        Objects.requireNonNull(relative, "feed name is required");
        var full = relative.startsWith(name + ".") ? relative : name + "." + relative;
        return WhitewaterClient.dottedName(full);
    }

    /** The name minus this Domain's prefix — {@code shop.orders} → {@code orders}. */
    private String relativeName(String full) {
        return full.startsWith(name + ".") ? full.substring(name.length() + 1) : full;
    }

    private String cacheKey(String full, Codec<?> keyCodec, Codec<?> valueCodec) {
        return full + "|" + System.identityHashCode(keyCodec) + "|" + System.identityHashCode(valueCodec);
    }

    // ---- snapshot loading ------------------------------------------------------

    private Snapshot snapshot() {
        var current = snapshot;
        if (current == null) {
            synchronized (this) {
                current = snapshot;
                if (current == null) {
                    current = load();
                    snapshot = current;
                }
            }
        }
        return current;
    }

    /**
     * One batched control read: {@code DESCRIBE DOMAIN} plus every SHOW
     * listing. Rows join on {@code space_id} — readers and writers join
     * through their Feed's {@code feed_id}. A missing Domain fails the
     * whole batch server-side and surfaces as {@link
     * WhitewaterException.Refused}.
     */
    private Snapshot load() {
        ControlExecution execution = client.commands(Commands.of()
                .describe(ResourceKind.DOMAIN, name)
                .show(ShowKind.FEEDS)
                .show(ShowKind.READERS)
                .show(ShowKind.WRITERS)
                .show(ShowKind.SUBSCRIPTIONS)
                .show(ShowKind.PIPES)
                .show(ShowKind.INDEXES)
                .show(ShowKind.STATE_STORES));
        var results = execution.results();
        var describe = results.get(0).data();
        var idNode = describe == null ? null : describe.get("space_id");
        if (idNode == null || !idNode.isTextual()) {
            throw new WhitewaterException.Refused(404, "domain.unknown",
                    "Domain '" + name + "' is not declared");
        }
        var domainId = UUID.fromString(idNode.asText());

        var feeds = new ArrayList<String>();
        var feedIds = new java.util.HashSet<UUID>();
        var effectiveSchemas = new java.util.HashMap<String, FeedSchema>();
        var declaredSchemas = new java.util.HashMap<String, FeedSchema>();
        for (var row : results.get(1).data()) {
            if (domainId(row.path("space_id"), domainId)) {
                var feedName = row.path("name").asText();
                feeds.add(feedName);
                feedIds.add(UUID.fromString(row.path("feed_id").asText()));
                var effective = FeedSchema.fromWire(row.path("effective_schema"));
                if (effective != null) {
                    effectiveSchemas.put(feedName, effective);
                }
                var declared = FeedSchema.fromWire(row.path("schema"));
                if (declared != null) {
                    declaredSchemas.put(feedName, declared);
                }
            }
        }
        var readers = scopedNames(results.get(2).data(), "feed_id", feedIds);
        var writers = scopedNames(results.get(3).data(), "feed_id", feedIds);
        var subscriptions = scopedNames(results.get(4).data(), "space_id", java.util.Set.of(domainId));
        var pipes = scopedNames(results.get(5).data(), "space_id", java.util.Set.of(domainId));
        var indexes = scopedNames(results.get(6).data(), "space_id", java.util.Set.of(domainId));
        var stores = scopedNames(results.get(7).data(), "space_id", java.util.Set.of(domainId));
        feeds.sort(null);
        return new Snapshot(domainId, describe, execution.revision(),
                List.copyOf(feeds), readers, writers, subscriptions, pipes, indexes, stores,
                effectiveSchemas, declaredSchemas);
    }

    private static boolean domainId(JsonNode node, UUID expected) {
        return node.isTextual() && expected.equals(UUID.fromString(node.asText()));
    }

    private static List<String> scopedNames(
            JsonNode rows, String scopeField, java.util.Set<UUID> scopeIds) {
        var names = new ArrayList<String>();
        if (rows != null && rows.isArray()) {
            for (var row : rows) {
                var scope = row.path(scopeField);
                if (scope.isTextual() && scopeIds.contains(UUID.fromString(scope.asText()))) {
                    names.add(row.path("name").asText());
                }
            }
        }
        names.sort(null);
        return List.copyOf(names);
    }

    private final class Snapshot {
        final UUID domainId;
        final JsonNode describe;
        final long revision;
        final List<String> feeds;
        final List<String> readers;
        final List<String> writers;
        final List<String> subscriptions;
        final List<String> pipes;
        final List<String> indexes;
        final List<String> stateStores;
        final java.util.Map<String, FeedSchema> effectiveSchemas;
        final java.util.Map<String, FeedSchema> declaredSchemas;

        Snapshot(UUID domainId, JsonNode describe, long revision,
                List<String> feeds, List<String> readers, List<String> writers,
                List<String> subscriptions, List<String> pipes,
                List<String> indexes, List<String> stateStores,
                java.util.Map<String, FeedSchema> effectiveSchemas,
                java.util.Map<String, FeedSchema> declaredSchemas) {
            this.domainId = domainId;
            this.describe = describe;
            this.revision = revision;
            this.feeds = feeds;
            this.readers = readers;
            this.writers = writers;
            this.subscriptions = subscriptions;
            this.pipes = pipes;
            this.indexes = indexes;
            this.stateStores = stateStores;
            this.effectiveSchemas = java.util.Map.copyOf(effectiveSchemas);
            this.declaredSchemas = java.util.Map.copyOf(declaredSchemas);
        }
    }
}
