package dev.whitewater.admin;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.whitewater.internal.Transport;
import dev.whitewater.model.ExhaustedPolicy;
import dev.whitewater.model.Extractor;
import dev.whitewater.model.GrantEffect;
import dev.whitewater.model.IndexCollation;
import dev.whitewater.model.IndexConsistency;
import dev.whitewater.model.IndexPredicate;
import dev.whitewater.model.NamedKey;
import dev.whitewater.model.NodeCapabilities;
import dev.whitewater.model.NodeCapacity;
import dev.whitewater.model.Permission;
import dev.whitewater.model.PipeOperation;
import dev.whitewater.model.ReaderStart;
import dev.whitewater.model.ResourceKind;
import dev.whitewater.model.ShowKind;
import dev.whitewater.model.Time;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Fluent builder for the typed Control Plane command vocabulary —
 * the same {@code Command} enum WCL lowers to, executed over
 * {@code POST /v1/admin/commands} by
 * {@link dev.whitewater.WhitewaterClient#commands(Commands)}.
 *
 * <pre>{@code
 * var exec = client.commands(Commands.of()
 *         .createDomain("orders")
 *         .createFeed("orders.events")
 *         .createWriter("ingest", "orders.events")
 *         .createSubscription("audit", "orders.events", ReaderStart.beginning()));
 * }</pre>
 *
 * Every method validates names and required fields at build time — a
 * malformed command fails locally, never reaches the Control Plane.
 * Internal replica-authority commands (range cutover, owner movement,
 * commit proofs) are deliberately absent: the public endpoint refuses
 * them and they are not application surface.
 */
public final class Commands {

    private final List<ObjectNode> commands = new ArrayList<>();

    private Commands() {}

    /** A new empty command batch. */
    public static Commands of() {
        return new Commands();
    }

    /** The accumulated commands as wire JSON. */
    public List<JsonNode> build() {
        return List.copyOf(commands);
    }

    private Commands add(ObjectNode node) {
        commands.add(node);
        return this;
    }

    private static ObjectNode node(String command) {
        return Transport.json().createObjectNode().put("command", command);
    }

    private static void putIf(ObjectNode node, String field, Long value) {
        if (value != null) {
            node.put(field, value);
        }
    }

    private static void putIf(ObjectNode node, String field, String value) {
        if (value != null) {
            node.put(field, value);
        }
    }

    // ---- names --------------------------------------------------------------

    /** Dotted lowercase names: Domains, Feeds, Indexes, StateStores. */
    public static String dotted(String name) {
        Objects.requireNonNull(name, "name is required");
        if (name.length() > 512 || name.startsWith(".") || name.endsWith(".")
                || !name.matches("[a-z][a-z0-9]*(\\.[a-z][a-z0-9]*)*")) {
            throw new IllegalArgumentException("invalid dotted name: " + name);
        }
        return name;
    }

    /** Resource names use the same dotted grammar the server validates. */
    public static String resource(String name) {
        return dotted(name);
    }

    // ---- resources ----------------------------------------------------------

    /** {@code CREATE DOMAIN <name>} */
    public Commands createDomain(String name) {
        return add(node("create_domain").put("name", dotted(name)));
    }

    /** Compatibility alias for a Domain under the legacy Space name. */
    public Commands createSpace(String name) {
        return add(node("create_space").put("name", dotted(name)));
    }

    /** {@code CREATE FEED <dotted.name>} */
    public Commands createFeed(String name) {
        return add(node("create_feed").put("name", dotted(name)));
    }

    /** {@code CREATE WRITER <name> TO <feed>} */
    public Commands createWriter(String name, String feed) {
        return add(node("create_writer")
                .put("name", resource(name))
                .put("feed", dotted(feed)));
    }

    /** {@code CREATE READER <name> FROM <feed> START AT <start>} */
    public Commands createReader(String name, String feed, ReaderStart start) {
        return add(node("create_reader")
                .put("name", resource(name))
                .put("feed", dotted(feed))
                .set("start", (start == null ? ReaderStart.beginning() : start).toWire()));
    }

    /** {@code CREATE READER} starting at BEGINNING. */
    public Commands createReader(String name, String feed) {
        return createReader(name, feed, ReaderStart.beginning());
    }

    /**
     * {@code CREATE SUBSCRIPTION <name> FROM <feed> START AT <start>} —
     * a Subscription consuming one exact Feed.
     */
    public Commands createSubscription(String name, String feed, ReaderStart start) {
        return add(subscription(name, feed, null, start, null, null));
    }

    /** A Feed-sourced Subscription starting at BEGINNING. */
    public Commands createSubscription(String name, String feed) {
        return createSubscription(name, feed, ReaderStart.beginning());
    }

    /**
     * {@code CREATE SUBSCRIPTION <name> FROM DOMAIN <domain>} — the
     * Subscription covers every Feed in the named Domain, current and
     * future.
     */
    public Commands createDomainSubscription(String name, String domain, ReaderStart start) {
        return add(subscription(name, null, domain, start, null, null));
    }

    /**
     * Full Subscription declaration: source, start, delivery delay,
     * retry budget. {@code deliveryDelay} is a {@link Time} — the SDK
     * extracts nanoseconds internally for the wire.
     */
    public Commands createSubscription(
            String name,
            String feed,
            String domain,
            ReaderStart start,
            Time deliveryDelay,
            ExhaustedPolicy exhaustedPolicy) {
        return add(subscription(name, feed, domain, start, deliveryDelay, exhaustedPolicy));
    }

    private ObjectNode subscription(
            String name,
            String feed,
            String domain,
            ReaderStart start,
            Time deliveryDelay,
            ExhaustedPolicy exhaustedPolicy) {
        if (feed != null && domain != null) {
            throw new IllegalArgumentException(
                    "a Subscription names one source: feed or domain, not both");
        }
        if (feed == null && domain == null) {
            throw new IllegalArgumentException(
                    "a Subscription requires a source: feed or domain");
        }
        ObjectNode node = node("create_subscription")
                .put("name", resource(name))
                .set("start", (start == null ? ReaderStart.beginning() : start).toWire());
        putIf(node, "feed", feed == null ? null : dotted(feed));
        putIf(node, "domain", domain == null ? null : dotted(domain));
        putIf(node, "delivery_delay_ns",
                deliveryDelay == null ? null : deliveryDelay.nanos());
        if (exhaustedPolicy != null) {
            node.set("exhausted_policy", exhaustedPolicy.toWire());
        }
        return node;
    }

    /** {@code CREATE ROLE <name>} */
    public Commands createRole(String name) {
        return add(node("create_role").put("name", resource(name)));
    }

    // ---- writers / readers / subscriptions ----------------------------------

    /** {@code OPEN WRITER SESSION <writer>} — issues a fresh fencing epoch. */
    public Commands openWriterSession(String writer) {
        return add(node("open_writer_session").put("writer", resource(writer)));
    }

    /** Revoke a Writer session so its epoch fences any further appends. */
    public Commands revokeWriterSession(String writer, long sessionEpoch) {
        return add(node("revoke_writer_session")
                .put("writer", resource(writer))
                .put("session_epoch", sessionEpoch));
    }

    /** {@code SEEK READER <name> TO <start>} — repositions a durable Reader. */
    public Commands seekReader(String reader, ReaderStart start) {
        return add(node("seek_reader")
                .put("reader", resource(reader))
                .set("start", Objects.requireNonNull(start).toWire()));
    }

    /**
     * {@code SEEK SUBSCRIPTION <name> TO <start>} — repositions a shared
     * Subscription frontier; refused while any member work lease is
     * active. Answered by the data-plane seek endpoint.
     */
    public Commands seekSubscription(String subscription, ReaderStart start) {
        return add(node("seek_subscription")
                .put("name", subscription)
                .set("start", Objects.requireNonNull(start).toWire()));
    }

    /** {@code INSPECT PLACEMENT <feed>} — the active-range ownership view. */
    public Commands inspectPlacement(String feed) {
        return add(node("inspect_placement").put("feed", dotted(feed)));
    }

    // ---- introspection --------------------------------------------------------

    /** {@code SHOW <kind>} — rows land in {@code results[0].data}. */
    public Commands show(ShowKind kind) {
        return add(node("show").put("kind", Objects.requireNonNull(kind).wire()));
    }

    /** {@code DESCRIBE <kind> <name>} */
    public Commands describe(ResourceKind kind, String name) {
        return add(node("describe")
                .put("kind", Objects.requireNonNull(kind).wire())
                .put("name", name));
    }

    /** {@code RENAME <kind> <current> TO <new>} */
    public Commands rename(ResourceKind kind, String current, String renamed) {
        return add(node("rename")
                .put("kind", Objects.requireNonNull(kind).wire())
                .put("current", current)
                .put("new", renamed));
    }

    /** {@code DROP <kind> <name>} */
    public Commands drop(ResourceKind kind, String name) {
        return add(node("drop")
                .put("kind", Objects.requireNonNull(kind).wire())
                .put("name", name));
    }

    // ---- authorization --------------------------------------------------------

    /**
     * {@code GRANT <actions> ON <namespace> TO <role>} — {@code effect}
     * {@code DENY} rows fence the action ahead of every covering allow.
     */
    public Commands grant(
            GrantEffect effect,
            Set<Permission> actions,
            String namespace,
            String role) {
        ObjectNode node = node("grant")
                .put("namespace", namespace)
                .put("role", resource(role))
                .put("effect", (effect == null ? GrantEffect.ALLOW : effect).wire());
        ArrayNode array = node.putArray("actions");
        Objects.requireNonNull(actions, "actions").forEach(a -> array.add(a.wire()));
        return add(node);
    }

    /** {@code GRANT <actions> ON <namespace> TO <role>} (allow). */
    public Commands grant(Set<Permission> actions, String namespace, String role) {
        return grant(GrantEffect.ALLOW, actions, namespace, role);
    }

    /**
     * {@code REVOKE <actions> ON <namespace> FROM <role>} — exact
     * subtraction: revoking {@code read} from a {@code read, write}
     * grant leaves {@code write}.
     */
    public Commands revokeGrant(
            GrantEffect effect,
            Set<Permission> actions,
            String namespace,
            String role) {
        ObjectNode node = node("revoke_grant")
                .put("namespace", namespace)
                .put("role", resource(role))
                .put("effect", (effect == null ? GrantEffect.ALLOW : effect).wire());
        ArrayNode array = node.putArray("actions");
        Objects.requireNonNull(actions, "actions").forEach(a -> array.add(a.wire()));
        return add(node);
    }

    /** {@code REVOKE <actions> ON <namespace> FROM <role>} (allow rows). */
    public Commands revokeGrant(Set<Permission> actions, String namespace, String role) {
        return revokeGrant(GrantEffect.ALLOW, actions, namespace, role);
    }

    /**
     * Issue a named API-key identity bound to a Role. {@code digest} is
     * the BLAKE3-256 hex verifier of the plaintext key — the key itself
     * must never appear in a command.
     */
    public Commands issueApiKey(String name, String digest, String role, Long expiresAtNs) {
        ObjectNode node = node("issue_api_key")
                .put("name", name)
                .put("digest", digest)
                .put("role", resource(role));
        putIf(node, "expires_at_ns", expiresAtNs);
        return add(node);
    }

    /** Revoke an issued API-key identity durably. */
    public Commands revokeApiKey(String name) {
        return add(node("revoke_api_key").put("name", name));
    }

    /** Explain whether {@code role} may perform {@code action} on {@code feed}. */
    public Commands explainAccess(String role, Permission action, String feed) {
        return add(node("explain_access")
                .put("role", resource(role))
                .put("action", Objects.requireNonNull(action).wire())
                .put("feed", dotted(feed)));
    }

    // ---- indexes ----------------------------------------------------------------

    /**
     * {@code DEFINE INDEX <name> ON <feed> (<fields>)} — declare a
     * replicated secondary Index; {@code fields} are {@link Extractor}
     * projections over the source record ({@code key}, named Metadata,
     * versioned JSON-path projections).
     */
    public Commands defineIndex(
            String name,
            String feed,
            List<Extractor> fields,
            boolean unique,
            IndexCollation collation,
            IndexConsistency consistency) {
        Objects.requireNonNull(fields, "fields");
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("an Index requires at least one field");
        }
        ObjectNode node = node("define_index")
                .put("name", dotted(name))
                .put("feed", dotted(feed))
                .put("unique", unique)
                .put("collation", (collation == null ? IndexCollation.BINARY : collation).wire())
                .put("consistency", (consistency == null
                        ? IndexConsistency.EVENTUAL : consistency).wire());
        ArrayNode array = node.putArray("fields");
        fields.forEach(field -> array.add(field.toWire()));
        return add(node);
    }

    /** {@code DEFINE INDEX} with binary collation and eventual consistency. */
    public Commands defineIndex(String name, String feed, List<Extractor> fields) {
        return defineIndex(name, feed, fields, false, null, null);
    }

    /** {@code REBUILD INDEX <name>} — re-materialize into a new shadow generation. */
    public Commands rebuildIndex(String name) {
        return add(node("rebuild_index").put("name", dotted(name)));
    }

    /** Rebuild every declared Index whose source Feed lives in the Domain. */
    public Commands rebuildIndexesInDomain(String domain) {
        return add(node("rebuild_indexes_in_domain").put("domain", dotted(domain)));
    }

    /**
     * {@code GET INDEX <index> <predicate>} — a data-plane read against
     * an activated Index generation; the catalog never applies it.
     */
    public Commands getIndex(
            String index, IndexPredicate predicate, Integer limit, boolean strict, Long waitMs) {
        ObjectNode node = node("get_index")
                .put("index", dotted(index))
                .put("strict", strict);
        node.set("predicate", Objects.requireNonNull(predicate).toCommandWire());
        if (limit != null) {
            node.put("limit", limit);
        }
        putIf(node, "wait_ms", waitMs);
        return add(node);
    }

    /** {@code GET INDEX} with defaults. */
    public Commands getIndex(String index, IndexPredicate predicate) {
        return getIndex(index, predicate, null, false, null);
    }

    // ---- state stores -----------------------------------------------------------

    /**
     * {@code DEFINE STATESTORE <name>} — declare a StateStore. {@code
     * sourceFeed} names the same-Domain Feed it materializes from; null
     * declares a manual store. Feed-sourced named keys each require an
     * {@link Extractor}; manual stores take named values on each put.
     */
    public Commands defineStateStore(String name, String sourceFeed, List<NamedKey> namedKeys) {
        ObjectNode node = node("define_state_store").put("name", dotted(name));
        ObjectNode source = node.putObject("source");
        if (sourceFeed != null) {
            source.put("kind", "feed").put("feed", dotted(sourceFeed));
        } else {
            source.put("kind", "manual");
        }
        if (namedKeys != null && !namedKeys.isEmpty()) {
            ArrayNode array = node.putArray("named_keys");
            namedKeys.forEach(key -> {
                ObjectNode entry = Transport.json().createObjectNode();
                entry.put("name", key.name());
                entry.put("unique", key.unique());
                if (key.extractor() != null) {
                    entry.set("extractor", key.extractor().toWire());
                }
                array.add(entry);
            });
        }
        return add(node);
    }

    // ---- pipes ----------------------------------------------------------------

    /**
     * {@code DEFINE PIPE <name> FROM <subscription> TO FEED <feed>} —
     * the copy Pipe.
     */
    public Commands defineForwardPipe(String name, String subscription, String outputFeed) {
        return add(node("define_pipe")
                .put("name", resource(name))
                .put("subscription", resource(subscription))
                .put("output_feed", dotted(outputFeed))
                .set("operation", PipeOperation.forward().toWire()));
    }

    /**
     * {@code DEFINE PIPE ... ENRICH FROM <store>} — point-lookup
     * enrichment; {@code quarantineFeed} resolves the miss-quarantine
     * target when the operation's miss disposition is {@code QUARANTINE}.
     */
    public Commands defineEnrichPipe(
            String name,
            String subscription,
            String outputFeed,
            String store,
            PipeOperation operation,
            String quarantineFeed) {
        if (operation == null || !"enrich".equals(operation.operation())) {
            throw new IllegalArgumentException("enrich pipes take PipeOperation.enrich(...)");
        }
        ObjectNode node = node("define_pipe")
                .put("name", resource(name))
                .put("subscription", resource(subscription))
                .put("output_feed", dotted(outputFeed))
                .put("output_store", dotted(store));
        putIf(node, "quarantine_feed", quarantineFeed == null ? null : dotted(quarantineFeed));
        node.set("operation", operation.toWire());
        return add(node);
    }

    /**
     * {@code DEFINE PIPE ... TO STATESTORE <store>} — rolling-window
     * aggregation; {@code emitFeed} optionally emits closed panes and
     * requires the operation declare a lateness model —
     * {@code maxLateNs} or {@code watermarkNs}.
     */
    public Commands defineWindowPipe(
            String name,
            String subscription,
            String outputStore,
            PipeOperation operation,
            String emitFeed) {
        if (operation == null || !"window".equals(operation.operation())) {
            throw new IllegalArgumentException("window pipes take PipeOperation.window(...)");
        }
        ObjectNode node = node("define_pipe")
                .put("name", resource(name))
                .put("subscription", resource(subscription))
                .put("output_store", dotted(outputStore));
        putIf(node, "emit_feed", emitFeed == null ? null : dotted(emitFeed));
        node.set("operation", operation.toWire());
        return add(node);
    }

    /** {@code PAUSE PIPE <name>} — durable, idempotent; in-flight effects finish. */
    public Commands pausePipe(String name) {
        return add(node("pause_pipe").put("name", resource(name)));
    }

    /** {@code RESUME PIPE <name>} — idempotent. */
    public Commands resumePipe(String name) {
        return add(node("resume_pipe").put("name", resource(name)));
    }

    // ---- storage nodes -----------------------------------------------------------

    /** Register a Storage Node in the placement catalog. */
    public Commands registerStorageNode(
            String node,
            String endpoint,
            Set<String> certPins,
            NodeCapabilities capabilities) {
        ObjectNode command = node("register_storage_node")
                .put("node", node)
                .put("endpoint", endpoint);
        if (certPins != null && !certPins.isEmpty()) {
            ArrayNode pins = command.putArray("cert_pins");
            certPins.forEach(pins::add);
        }
        if (capabilities != null) {
            command.set("capabilities", capabilities.toWire());
        }
        return add(command);
    }

    /** Begin draining a Storage Node ahead of removal. */
    public Commands drainStorageNode(String node) {
        return add(node("drain_storage_node").put("node", node));
    }

    /** Cancel a drain in progress. */
    public Commands undrainStorageNode(String node) {
        return add(node("undrain_storage_node").put("node", node));
    }

    /** Retire a drained Storage Node. */
    public Commands retireStorageNode(String node) {
        return add(node("retire_storage_node").put("node", node));
    }

    /** Inspect a Storage Node's drain progress. */
    public Commands inspectStorageDrain(String node) {
        return add(node("inspect_storage_drain").put("node", node));
    }

    /** Commit a Node's self-reported measured capacity into the catalog. */
    public Commands reportStorageNodeCapacity(String node, NodeCapacity capacity) {
        return add(node("report_storage_node_capacity")
                .put("node", node)
                .set("capacity", Objects.requireNonNull(capacity).toWire()));
    }
}
