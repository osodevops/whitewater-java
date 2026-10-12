package dev.whitewater;

import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.whitewater.internal.Transport;
import dev.whitewater.internal.Wire;
import dev.whitewater.model.ControlExecution;
import dev.whitewater.model.Extractor;

import java.util.UUID;

/**
 * Collection-level StateStore operations: declaration and drop. Row
 * operations live on the bound {@link StateStore} facade returned by
 * {@link WhitewaterClient#stateStore(String)}.
 */
public final class StateStoresClient {

    private final Transport transport;

    StateStoresClient(Transport transport) {
        this.transport = transport;
    }

    /**
     * Declare {@code spec} on the Control Plane and return it bound to
     * this client. {@code source} names a same-Domain Feed the store
     * materializes from; null means a manual store. Feed-sourced named
     * keys must each declare an {@link Extractor} so replay can project
     * typed values; extractors on a manual store are refused.
     */
    public StateStore define(StateStore spec, String source) {
        if (spec == null) {
            throw new IllegalArgumentException(
                    "define() takes a StateStore spec — use StateStore.named(name)");
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
        return spec.bind(transport);
    }

    /** Tombstone the named store. */
    public ControlExecution drop(String name) {
        return StateStore.bound(transport, name).drop();
    }
}
