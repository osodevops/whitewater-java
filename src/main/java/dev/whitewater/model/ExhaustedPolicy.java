package dev.whitewater.model;

import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.whitewater.internal.Transport;

/**
 * A Subscription's declared per-work retry budget. Attempts count by the
 * claim's durable lease epoch — each expiry or transfer that regrants the
 * work counts one.
 */
public record ExhaustedPolicy(long maxDeliveryAttempts, ExhaustedDisposition disposition) {

    /** Quarantine disposition. */
    public static ExhaustedPolicy attempts(long maxDeliveryAttempts) {
        return new ExhaustedPolicy(maxDeliveryAttempts, ExhaustedDisposition.QUARANTINE);
    }

    /** Explicit disposition. */
    public static ExhaustedPolicy attempts(long maxDeliveryAttempts, ExhaustedDisposition disposition) {
        return new ExhaustedPolicy(maxDeliveryAttempts, disposition);
    }

    /** The wire form. */
    public ObjectNode toWire() {
        if (maxDeliveryAttempts <= 0) {
            throw new IllegalArgumentException("maxDeliveryAttempts must be positive");
        }
        ObjectNode node = Transport.json().createObjectNode();
        node.put("max_delivery_attempts", maxDeliveryAttempts);
        node.put("disposition", (disposition == null
                ? ExhaustedDisposition.QUARANTINE : disposition).wire());
        return node;
    }
}
