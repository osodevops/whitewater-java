package dev.whitewater;

import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.whitewater.internal.Transport;
import dev.whitewater.internal.Wire;
import dev.whitewater.model.Codec;
import dev.whitewater.model.ControlExecution;
import dev.whitewater.model.Extractor;
import dev.whitewater.model.StateStoreSpec;

import java.util.UUID;

/**
 * Collection-level StateStore operations: declaration and drop. Row
 * operations live on the bound {@link StateStore} facade returned by
 * {@link WhitewaterClient#stateStore(String)} or {@link #define}.
 */
public final class StateStoresClient {

    private final Transport transport;

    StateStoresClient(Transport transport) {
        this.transport = transport;
    }

    /**
     * Declare {@code spec} on the Control Plane and return it bound to
     * this client (String keys, {@code byte[]} payloads —
     * {@link StateStore#view} rebinds other {@link Codec}s).
     * {@code source} names a same-Domain Feed the store materializes
     * from; null means a manual store. Feed-sourced named keys must
     * each declare an {@link Extractor} so replay can project typed
     * values; extractors on a manual store are refused.
     */
    public StateStore<String, byte[]> define(StateStoreSpec spec, String source) {
        return define(spec, source, (Codec<String>) null, (Codec<byte[]>) null);
    }

    /**
     * {@link #define(StateStoreSpec, String)} bound by classes —
     * {@code String}, {@code byte[]}, {@link com.fasterxml.jackson.databind.JsonNode},
     * or Jackson-mapped types.
     */
    public <K, V> StateStore<K, V> define(
            StateStoreSpec spec, String source, Class<K> keyType, Class<V> payloadType) {
        return define(spec, source,
                dev.whitewater.model.Codecs.forClass(keyType),
                dev.whitewater.model.Codecs.forClass(payloadType));
    }

    /** {@link #define(StateStoreSpec, String)} bound under explicit Codecs. */
    public <K, V> StateStore<K, V> define(
            StateStoreSpec spec, String source, Codec<K> keyCodec, Codec<V> payloadCodec) {
        if (spec == null) {
            throw new IllegalArgumentException(
                    "define() takes a StateStoreSpec — use StateStoreSpec.named(name)");
        }
        if (source == null
                && spec.namedKeys().stream().anyMatch(k -> k.extractor() != null)) {
            throw new IllegalArgumentException(
                    "named-key extractors apply to feed-sourced StateStores; "
                            + "manual stores take named values on each put");
        }
        if (source != null
                && spec.namedKeys().stream().anyMatch(k -> k.extractor() == null)) {
            throw new IllegalArgumentException(
                    "feed-sourced named keys require an extractor "
                            + "(Extractor.recordKey()/metadata()/json())");
        }
        ObjectNode body = Transport.json().createObjectNode();
        body.put("request_id", UUID.randomUUID().toString());
        var commands = body.putArray("commands");
        ObjectNode command = Transport.json().createObjectNode();
        command.put("command", "define_state_store");
        command.put("name", spec.name());
        ObjectNode sourceNode = command.putObject("source");
        if (source != null) {
            sourceNode.put("kind", "feed");
            sourceNode.put("feed", WhitewaterClient.dottedName(source));
        } else {
            sourceNode.put("kind", "manual");
        }
        var namedKeys = command.putArray("named_keys");
        spec.namedKeys().forEach(key -> {
            ObjectNode entry = Transport.json().createObjectNode();
            entry.put("name", key.name());
            entry.put("unique", key.unique());
            if (key.extractor() != null) {
                entry.set("extractor", key.extractor().toWire());
            }
            namedKeys.add(entry);
        });
        commands.add(command);
        Wire.controlExecution(transport.post("/v1/admin/commands", body));
        @SuppressWarnings("unchecked")
        Codec<K> keys = keyCodec != null ? keyCodec : (Codec<K>) dev.whitewater.model.Codecs.utf8();
        @SuppressWarnings("unchecked")
        Codec<V> values = payloadCodec != null ? payloadCodec : (Codec<V>) dev.whitewater.model.Codecs.bytes();
        return StateStore.bound(transport, spec.name(), keys, values);
    }

    /** Tombstone the named store. */
    public ControlExecution drop(String name) {
        return StateStore.defaults(transport, name).drop();
    }
}
