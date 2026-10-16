package dev.whitewater;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.whitewater.internal.Transport;
import dev.whitewater.internal.Wire;
import dev.whitewater.model.Codec;
import dev.whitewater.model.Codecs;
import dev.whitewater.model.ControlExecution;
import dev.whitewater.model.IndexPredicate.IndexBound;
import dev.whitewater.model.MutationResult;
import dev.whitewater.model.StorePage;
import dev.whitewater.model.StoreRow;
import dev.whitewater.model.StoreRows;
import dev.whitewater.model.StoreSync;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * A typed facade over one node-side StateStore — the durable RF3 + Fjall
 * store on the Nodes is the only store; this object holds no rows.
 * Declare it with {@link dev.whitewater.model.StateStoreSpec}, then work
 * in your own types — the bound {@link Codec}s handle every byte
 * conversion:
 *
 * <pre>{@code
 * var spec = StateStoreSpec.named("accounts.employees")
 *         .addKey("passport", true).addKey("department");
 * client.stateStores().define(spec, null);
 *
 * StateStore<String, Employee> store =
 *         client.stateStore("accounts.employees", Codecs.json(Employee.class));
 *
 * store.put("user-1", new Employee("alice", "eng"),
 *         Map.of("passport", "P111", "department", "eng"));
 * var row = store.get("user-1");                       // StoreRow<String, Employee> | null
 * Employee alice = row.payload();                       // decoded — never getBytes
 * var eng = store.getBy("department", "eng");           // every match
 * store.iterate(r -> export(r));
 * }</pre>
 *
 * <p>{@link WhitewaterClient#stateStore(String)} binds String keys with
 * raw {@code byte[]} payloads; pass a payload {@link Codec} (or key +
 * payload Codecs) for objects. Named-key values are typed —
 * {@link String}, whole {@link Number}s/{@link java.math.BigInteger},
 * {@link Boolean}, {@code byte[]}, or null; a {@link List} value carries
 * one typed value per posting component.
 */
public final class StateStore<K, V> {

    private final Transport transport;
    private final String name;
    private final Codec<K> keyCodec;
    private final Codec<V> payloadCodec;

    private StateStore(Transport transport, String name, Codec<K> keyCodec, Codec<V> payloadCodec) {
        this.transport = Objects.requireNonNull(transport);
        this.name = name;
        this.keyCodec = Objects.requireNonNull(keyCodec, "keyCodec");
        this.payloadCodec = Objects.requireNonNull(payloadCodec, "payloadCodec");
    }

    static <K, V> StateStore<K, V> bound(
            Transport transport, String name, Codec<K> keyCodec, Codec<V> payloadCodec) {
        return new StateStore<>(transport, WhitewaterClient.dottedName(name), keyCodec, payloadCodec);
    }

    public String name() {
        return name;
    }

    /** Rebind the same store under different Codecs — no wire call. */
    public <K2, V2> StateStore<K2, V2> view(Codec<K2> keyCodec, Codec<V2> payloadCodec) {
        return new StateStore<>(transport, name, keyCodec, payloadCodec);
    }

    /**
     * Insert or replace one row. {@code named} maps a declared named key
     * to a typed value (or a {@link List} for multi-component postings).
     * {@code mutationId} is the caller-owned idempotency identity —
     * reuse it on retries; {@link #put(Object, Object, Map)} generates
     * one per call.
     */
    public MutationResult put(K key, V value, Map<String, ?> named, UUID mutationId) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        ObjectNode op = Transport.json().createObjectNode();
        op.put("kind", "put_row");
        op.put("key_base64", Wire.b64(keyCodec.encode(key)));
        op.put("payload_base64", Wire.b64(payloadCodec.encode(value)));
        if (named != null && !named.isEmpty()) {
            op.set("named", Wire.namedValues(named));
        }
        ObjectNode body = Transport.json().createObjectNode();
        body.put("store", name);
        body.put("mutation_id", (mutationId == null ? UUID.randomUUID() : mutationId).toString());
        body.set("op", op);
        return Wire.mutationResult(transport.post("/v1/state-stores/mutate", body));
    }

    public MutationResult put(K key, V value, Map<String, ?> named) {
        return put(key, value, named, null);
    }

    /** Put with no named-key postings. */
    public MutationResult put(K key, V value) {
        return put(key, value, null, null);
    }

    /**
     * Primary-key read from the serving replica immediately — the row's
     * {@link dev.whitewater.model.StoreFreshness} reports whether it
     * answered behind its committed frontier. Use {@link #getStrict}
     * when the read must observe the latest committed write.
     */
    public StoreRow<K, V> get(K key) {
        return get(key, false, null);
    }

    /** Bounded-fresh read: waits for the replica to apply its committed frontier. */
    public StoreRow<K, V> getStrict(K key) {
        return get(key, true, null);
    }

    /** Bounded-fresh read with an explicit wait bound. */
    public StoreRow<K, V> getStrict(K key, Duration wait) {
        return get(key, true, wait.toMillis());
    }

    /**
     * Primary-key read with freshness evidence on the returned row; null
     * when absent. {@code strict} bounded-waits ({@code waitMs}) for the
     * serving replica to apply its committed frontier rather than
     * answering stale.
     */
    public StoreRow<K, V> get(K key, boolean strict, Long waitMs) {
        Objects.requireNonNull(key, "key");
        ObjectNode body = Transport.json().createObjectNode();
        body.put("store", name);
        body.put("key_base64", Wire.b64(keyCodec.encode(key)));
        body.put("strict", strict);
        if (waitMs != null) {
            body.put("wait_ms", waitMs);
        }
        JsonNode response = transport.post("/v1/state-stores/lookup", body);
        if (!response.path("found").asBoolean(false)) {
            return null;
        }
        JsonNode appliedCursor = response.path("applied_cursor");
        return new StoreRow<>(
                key,
                payloadCodec.decode(Wire.b64(response.path("payload_base64").asText())),
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
    public StoreRows<K, V> getBy(String namedKey, Object value) {
        return getBy(namedKey, value, null, null, null, false, null);
    }

    /** {@link #getBy(String, Object)} bounded to {@code limit} rows. */
    public StoreRows<K, V> getBy(String namedKey, Object value, Integer limit) {
        return getBy(namedKey, value, null, null, limit, false, null);
    }

    /** {@link #getBy(String, Object)} that bounded-waits for freshness. */
    public StoreRows<K, V> getByStrict(String namedKey, Object value, Duration wait) {
        return getBy(namedKey, value, null, null, null, true, wait.toMillis());
    }

    /** {@link #getByStrict(String, Object, Duration)} bounded to {@code limit} rows. */
    public StoreRows<K, V> getByStrict(String namedKey, Object value, Integer limit, Duration wait) {
        return getBy(namedKey, value, null, null, limit, true, wait.toMillis());
    }

    /**
     * Bounded named-key lookup: {@code lower}/{@code upper} bound the
     * posting range the lookup scans (inclusive/exclusive), like an
     * Index range predicate.
     */
    public StoreRows<K, V> getBy(String namedKey, Object value,
                                 IndexBound lower, IndexBound upper,
                                 Integer limit, boolean strict, Long waitMs) {
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
        if (lower != null) {
            var node = body.putObject("lower");
            node.set("value", Wire.indexValue(lower.value()));
            node.put("inclusive", lower.inclusive());
        }
        if (upper != null) {
            var node = body.putObject("upper");
            node.set("value", Wire.indexValue(upper.value()));
            node.put("inclusive", upper.inclusive());
        }
        if (limit != null) {
            body.put("limit", limit);
        }
        if (waitMs != null) {
            body.put("wait_ms", waitMs);
        }
        return typed(Wire.storeRows(transport.post("/v1/state-stores/get-by", body)));
    }

    /** Remove the row and its named postings; mutation identity auto-generated. */
    public MutationResult delete(K key) {
        return delete(key, null);
    }

    /** Remove the row under a caller-owned idempotency identity. */
    public MutationResult delete(K key, UUID mutationId) {
        Objects.requireNonNull(key, "key");
        ObjectNode op = Transport.json().createObjectNode();
        op.put("kind", "delete_row");
        op.put("key_base64", Wire.b64(keyCodec.encode(key)));
        ObjectNode body = Transport.json().createObjectNode();
        body.put("store", name);
        body.put("mutation_id", (mutationId == null ? UUID.randomUUID() : mutationId).toString());
        body.set("op", op);
        return Wire.mutationResult(transport.post("/v1/state-stores/mutate", body));
    }

    /** The first iteration page at the default bound. */
    public StorePage<K, V> page() {
        return page(null, null);
    }

    /**
     * One bounded iteration page in primary-key order — the same row set
     * replica transfer serves, pinned to {@link StorePage#pinnedSeq}.
     * Feed {@code page.nextKey()} back in to continue.
     */
    public StorePage<K, V> page(K afterKey, Integer limit) {
        ObjectNode body = Transport.json().createObjectNode();
        body.put("store", name);
        if (afterKey != null) {
            body.put("after_key_base64", Wire.b64(keyCodec.encode(afterKey)));
        }
        if (limit != null) {
            body.put("limit", limit);
        }
        StorePage<byte[], byte[]> raw = Wire.storePage(transport.post("/v1/state-stores/iterate", body));
        List<StoreRow<K, V>> rows = new ArrayList<>(raw.rows().size());
        raw.rows().forEach(row -> rows.add(typed(row)));
        return new StorePage<>(List.copyOf(rows),
                raw.nextKey() == null ? null : keyCodec.decode(raw.nextKey()),
                raw.pinnedSeq(), raw.servedBy());
    }

    /** Invoke {@code visitor} per row in primary-key order across bounded pages. */
    public void iterate(Consumer<StoreRow<K, V>> visitor) {
        iterate(256, visitor);
    }

    /** {@link #iterate(Consumer)} with an explicit page bound. */
    public void iterate(int pageSize, Consumer<StoreRow<K, V>> visitor) {
        Objects.requireNonNull(visitor, "visitor");
        K after = null;
        while (true) {
            StorePage<K, V> page = page(after, pageSize);
            page.rows().forEach(visitor);
            if (page.nextKey() == null) {
                return;
            }
            after = page.nextKey();
        }
    }

    /** Empty the store as one journaled mutation; mutation identity auto-generated. */
    public MutationResult clear() {
        return clear(null);
    }

    /** Empty the store under a caller-owned idempotency identity —
     * durable and deduplicated on retries. */
    public MutationResult clear(UUID mutationId) {
        ObjectNode body = Transport.json().createObjectNode();
        body.put("store", name);
        body.put("mutation_id", (mutationId == null ? UUID.randomUUID() : mutationId).toString());
        return Wire.mutationResult(transport.post("/v1/state-stores/clear", body));
    }

    /**
     * Resume a Feed-derived store's replay from its durable frontier,
     * committing up to the bounded pass limit as journaled mutations.
     * {@code complete} reports the pass finished outstanding history.
     */
    public StoreSync sync() {
        ObjectNode body = Transport.json().createObjectNode();
        body.put("store", name);
        return Wire.storeSync(transport.post("/v1/state-stores/sync", body));
    }

    /** {@link #rebuild(UUID)} with an auto-generated dedup token. */
    public StoreSync rebuild() {
        return rebuild(null);
    }

    /**
     * Re-materialize a Feed-derived store from the beginning of retained
     * source history; {@code token} dedupes a retried rebuild's already
     * committed mutations. Manual stores refuse (they have no source).
     */
    public StoreSync rebuild(UUID token) {
        ObjectNode body = Transport.json().createObjectNode();
        body.put("store", name);
        if (token != null) {
            body.put("token", token.toString());
        }
        return Wire.storeSync(transport.post("/v1/state-stores/rebuild", body));
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
        return Wire.controlExecution(transport.post("/v1/admin/commands", body));
    }

    private StoreRow<K, V> typed(StoreRow<byte[], byte[]> row) {
        return new StoreRow<>(
                keyCodec.decode(row.key()),
                payloadCodec.decode(row.payload()),
                row.appliedCursor(),
                row.freshness());
    }

    private StoreRows<K, V> typed(StoreRows<byte[], byte[]> rows) {
        List<StoreRow<K, V>> mapped = new ArrayList<>(rows.rows().size());
        rows.rows().forEach(row -> mapped.add(typed(row)));
        return new StoreRows<>(List.copyOf(mapped), rows.freshness());
    }

    /** The default binding: String keys, raw byte[] payloads. */
    static StateStore<String, byte[]> defaults(Transport transport, String name) {
        return bound(transport, name, Codecs.utf8(), Codecs.bytes());
    }
}
