package dev.whitewater.model;

import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.whitewater.internal.Transport;

/**
 * The data-plane roles a registered Storage Node may serve. An absent
 * descriptor keeps a legacy Node fully eligible; declare
 * {@code storage: false} for compute/gateway-only Nodes (the burst shape)
 * that placement must never target.
 */
public record NodeCapabilities(boolean storage, boolean compute, boolean gateway) {

    /** Every data-plane role enabled. */
    public static NodeCapabilities all() {
        return new NodeCapabilities(true, true, true);
    }

    /** Compute + gateway only: work that abandons without durability consequence. */
    public static NodeCapabilities stateless() {
        return new NodeCapabilities(false, true, true);
    }

    /** The wire form: {@code {"storage":true,"compute":true,"gateway":true}}. */
    public ObjectNode toWire() {
        ObjectNode node = Transport.json().createObjectNode();
        node.put("storage", storage);
        node.put("compute", compute);
        node.put("gateway", gateway);
        return node;
    }
}
