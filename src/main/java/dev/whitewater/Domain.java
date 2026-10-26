package dev.whitewater;

import com.fasterxml.jackson.databind.JsonNode;
import dev.whitewater.admin.Commands;
import dev.whitewater.model.Codec;
import dev.whitewater.model.Codecs;
import dev.whitewater.model.ControlExecution;
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

    // ---- scoped resources ----------------------------------------------------

    /**
     * Every Feed in this Domain as typed {@link Feed} handles, sorted by
     * name. Materializes the snapshot.
     */
    public List<Feed<String, byte[]>> feeds() {
        return feeds(null);
    }

    /** {@link #feeds()} filtered to relative names starting with {@code prefix}. */
    public List<Feed<String, byte[]>> feeds(String prefix) {
        var result = new ArrayList<Feed<String, byte[]>>();
        for (var feed : snapshot().feeds) {
            var relative = relativeName(feed);
            if (prefix == null || relative.startsWith(prefix)) {
                result.add(feed(relative));
            }
        }
        return List.copyOf(result);
    }

    /** Alias of {@link #feeds(String)}. */
    public List<Feed<String, byte[]>> getFeeds(String prefix) {
        return feeds(prefix);
    }

    /** The Feed names in this Domain, sorted — the plain-string view of {@link #feeds()}. */
    public List<String> feedNames() {
        return snapshot().feeds;
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
     * A typed {@link Feed} handle on {@code relative} inside this
     * Domain: {@code shop.feed("orders")} binds {@code shop.orders}.
     * Passing an already-qualified name ({@code "shop.orders"}) is
     * accepted; a name outside this Domain is refused locally. Pure
     * name composition — no I/O.
     */
    public Feed<String, byte[]> feed(String relative) {
        return feed(relative, Codecs.utf8(), Codecs.bytes());
    }

    /** Alias of {@link #feed(String)}. */
    public Feed<String, byte[]> getFeed(String relative) {
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
     * Resolve {@code relative} to a full dotted Feed name inside this
     * Domain — accepts either the short form ({@code "orders"}) or the
     * already-qualified form ({@code "shop.orders"}). Every result lives
     * under the Domain's prefix by construction; a name that fails the
     * dotted grammar is refused here, locally.
     */
    String resolve(String relative) {
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
        for (var row : results.get(1).data()) {
            if (domainId(row.path("space_id"), domainId)) {
                feeds.add(row.path("name").asText());
                feedIds.add(UUID.fromString(row.path("feed_id").asText()));
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
                List.copyOf(feeds), readers, writers, subscriptions, pipes, indexes, stores);
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

        Snapshot(UUID domainId, JsonNode describe, long revision,
                List<String> feeds, List<String> readers, List<String> writers,
                List<String> subscriptions, List<String> pipes,
                List<String> indexes, List<String> stateStores) {
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
        }
    }
}
