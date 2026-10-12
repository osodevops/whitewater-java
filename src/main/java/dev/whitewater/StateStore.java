package dev.whitewater;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.whitewater.internal.Transport;
import dev.whitewater.internal.Wire;
import dev.whitewater.model.ControlExecution;
import dev.whitewater.model.Extractor;
import dev.whitewater.model.MutationResult;
import dev.whitewater.model.NamedKey;
import dev.whitewater.model.StorePage;
import dev.whitewater.model.StoreRow;
import dev.whitewater.model.StoreRows;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * A thin facade over one node-side StateStore — the durable RF3 + Fjall
 * store on the Nodes is the only store; this object holds no rows.
 * Declare it with named keys, then put/get/delete/iterate:
 *
 * <pre>{@code
 * var spec = StateStore.named("accounts.employees")
 *         .addKey("passport", true).addKey("department", false);
 * var store = client.stateStores().define(spec, null);
 *
 * store.put("user-1".getBytes(), "profile-1".getBytes(),
 *         Map.of("passport", "P111", "department", "eng"), null);
 * var row = store.get("user-1".getBytes(), false, 0);       // StoreRow | null
 * var rows = store.getBy("department", "eng", 0, false, 0); // every match
 * store.iterate(256, r -> export(r));                       // key-ordered pages
 * store.clear(null);
 * }</pre>
 *
 * <p>{@link #addKey} records declaration intent used by {@code define};
 * it does not mutate an already-declared store (named keys are declared
 * at creation). Named values are typed — {@link String}, whole
 * {@link Number}s/{@link java.math.BigInteger}, {@link Boolean},
 * {@code byte[]}, or null; a {@link List} value carries one typed value
 * per posting component.
 */
public final class StateStore {

    /** Named secondary keys per store; matches the server declaration budget. */
    public static final int MAX_NAMED_KEYS = NamedKey.MAX_NAMED_KEYS;

    private final List<NamedKey> declaredKeys = new ArrayList<>();

    private Transport transport;
    private final String name;

    private StateStore(Transport transport, String name) {
        this.transport = transport;
        this.name = name;
    }

    /** An unbound declaration spec for {@link StateStoresClient#define}. */
    public static StateStore named(String name) {
        return new StateStore(null, WhitewaterClient.dottedName(name));
    }

    static StateStore bound(Transport transport, String name) {
        return new StateStore(transport, WhitewaterClient.dottedName(name));
    }

    public String name() {
        return name;
    }

    /**
     * Declare a named secondary key for {@code define} (at most
     * {@value #MAX_NAMED_KEYS}). {@code extractor} declares how replay
     * projects each source-Feed record to this key's values — required
     * on feed-sourced stores, refused on manual ones.
     */
    public StateStore addKey(String namedKey, boolean unique, Extractor extractor) {
        if (namedKey == null || namedKey.isBlank()) {
            throw new IllegalArgumentException("named key is required");
        }
        if (declaredKeys.stream().anyMatch(k -> k.name().equals(namedKey))) {
            throw new IllegalArgumentException("duplicate named key: " + namedKey);
        }
        if (declaredKeys.size() >= MAX_NAMED_KEYS) {
            throw new IllegalStateException(
                    "a StateStore declares at most " + MAX_NAMED_KEYS + " named keys");
        }
        declaredKeys.add(new NamedKey(namedKey, unique, extractor));
        return this;
    }

    public StateStore addKey(String namedKey, boolean unique) {
        return addKey(namedKey, unique, null);
    }

    public StateStore addKey(String namedKey) {
        return addKey(namedKey, false);
    }

    public List<NamedKey> namedKeys() {
        return List.copyOf(declaredKeys);
    }

    private Transport boundTransport() {
        if (transport == null) {
            throw new IllegalStateException(
                    "unbound StateStore spec — pass it to client.stateStores().define() "
                            + "or get a bound facade via client.stateStore(name)");
        }
        return transport;
    }

    StateStore bind(Transport transport) {
        this.transport = transport;
        return this;
    }

    /**
     * Insert or replace one row. {@code named} maps a declared named key
     * to a typed value (or a {@link List} for multi-component postings).
     * {@code mutationId} is the caller-owned idempotency identity; null
     * generates one.
     */
    public MutationResult put(byte[] key, byte[] payload, Map<String, ?> named, UUID mutationId) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(payload, "payload");
        ObjectNode op = Transport.json().createObjectNode();
        op.put("kind", "put_row");
        op.put("key_base64", Wire.b64(key));
        op.put("payload_base64", Wire.b64(payload));
        if (named != null && !named.isEmpty()) {
            op.set("named", Wire.namedValues(named));
        }
        ObjectNode body = Transport.json().createObjectNode();
        body.put("store", name);
        body.put("mutation_id", (mutationId == null ? UUID.randomUUID() : mutationId).toString());
        body.set("op", op);
        return Wire.mutationResult(boundTransport().post("/v1/state-stores/mutate", body));
    }

    public MutationResult put(byte[] key, byte[] payload, Map<String, ?> named) {
        return put(key, payload, named, null);
    }

    /**
     * Primary-key read with freshness evidence on the returned row; null
     * when absent. {@code strict} bounded-waits ({@code waitMs}) for the
     * serving replica to apply its committed frontier rather than
     * answering stale.
     */
    public StoreRow get(byte[] key, boolean strict, Long waitMs) {
        Objects.requireNonNull(key, "key");
        ObjectNode body = Transport.json().createObjectNode();
        body.put("store", name);
        body.put("key_base64", Wire.b64(key));
        body.put("strict", strict);
        if (waitMs != null) {
            body.put("wait_ms", waitMs);
        }
        JsonNode response = boundTransport().post("/v1/state-stores/lookup", body);
        if (!response.path("found").asBoolean(false)) {
            return null;
        }
        JsonNode appliedCursor = response.path("applied_cursor");
        return new StoreRow(
                key,
                Wire.b64(response.path("payload_base64").asText()),
                appliedCursor.isNull() || appliedCursor.isMissingNode()
                        ? null
                        : appliedCursor.asText(),
                Wire.storeFreshness(response));
    }

    /**
     * Named-key lookup: every primary row the value points at, plus the
     * serving replica's freshness evidence. {@code value} is a typed
     * scalar or a {@link List} for multi-component postings.
     */
    public StoreRows getBy(String namedKey, Object value, Integer limit, boolean strict, Long waitMs) {
        ObjectNode body = Transport.json().createObjectNode();
        body.put("store", name);
        body.put("named_key", namedKey);
        var values = body.putArray("values");
        if (value instanceof List<?> list) {
            list.forEach(component -> values.add(Wire.indexValue(component)));
        } else {
            values.add(Wire.indexValue(value));
        }
        body.put("strict", strict);
        if (limit != null) {
            body.put("limit", limit);
        }
        if (waitMs != null) {
            body.put("wait_ms", waitMs);
        }
        return Wire.storeRows(boundTransport().post("/v1/state-stores/get-by", body));
    }

    /** Remove the row and its named postings. */
    public MutationResult delete(byte[] key, UUID mutationId) {
        Objects.requireNonNull(key, "key");
        ObjectNode op = Transport.json().createObjectNode();
        op.put("kind", "delete_row");
        op.put("key_base64", Wire.b64(key));
        ObjectNode body = Transport.json().createObjectNode();
        body.put("store", name);
        body.put("mutation_id", (mutationId == null ? UUID.randomUUID() : mutationId).toString());
        body.set("op", op);
        return Wire.mutationResult(boundTransport().post("/v1/state-stores/mutate", body));
    }

    public MutationResult delete(byte[] key) {
        return delete(key, null);
    }

    /**
     * One bounded iteration page in primary-key order — the same row set
     * replica transfer serves, pinned to {@link StorePage#pinnedSeq}.
     */
    public StorePage page(byte[] afterKey, Integer limit) {
        ObjectNode body = Transport.json().createObjectNode();
        body.put("store", name);
        if (afterKey != null) {
            body.put("after_key_base64", Wire.b64(afterKey));
        }
        if (limit != null) {
            body.put("limit", limit);
        }
        return Wire.storePage(boundTransport().post("/v1/state-stores/iterate", body));
    }

    /** Invoke {@code visitor} per row in primary-key order across bounded pages. */
    public void iterate(Integer pageSize, Consumer<StoreRow> visitor) {
        Objects.requireNonNull(visitor, "visitor");
        byte[] after = null;
        while (true) {
            StorePage page = page(after, pageSize);
            page.rows().forEach(visitor);
            if (page.nextKey() == null) {
                return;
            }
            after = page.nextKey();
        }
    }

    /** Empty the store as one journaled mutation — durable and idempotent
     * under {@code mutationId} retries (null generates one). */
    public MutationResult clear(UUID mutationId) {
        ObjectNode body = Transport.json().createObjectNode();
        body.put("store", name);
        body.put("mutation_id", (mutationId == null ? UUID.randomUUID() : mutationId).toString());
        return Wire.mutationResult(boundTransport().post("/v1/state-stores/clear", body));
    }

    /** Tombstone the store: the name cannot be recreated and every data
     * path refuses. Applied rows remain on replicas until reclaim. */
    public ControlExecution drop() {
        ObjectNode body = Transport.json().createObjectNode();
        body.put("request_id", UUID.randomUUID().toString());
        var commands = body.putArray("commands");
        ObjectNode command = Transport.json().createObjectNode();
        command.put("command", "drop");
        command.put("kind", "state_store");
        command.put("name", name);
        commands.add(command);
        return Wire.controlExecution(boundTransport().post("/v1/admin/commands", body));
    }
}
