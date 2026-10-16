package dev.whitewater.model;

import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.whitewater.internal.Transport;

/**
 * What a {@code window} Pipe adds to each pane's accumulator:
 * {@code count} records, {@code sum} an extracted integer, or
 * {@code average} tracking count+sum (readers divide).
 */
public record WindowAggregation(String aggregation, Extractor value) {

    /** Count records per (group Key, pane). */
    public static WindowAggregation count() {
        return new WindowAggregation("count", null);
    }

    /** Sum one extracted integer value per pane. */
    public static WindowAggregation sum(Extractor value) {
        return new WindowAggregation("sum", java.util.Objects.requireNonNull(value, "value"));
    }

    /** Track count and integer sum per pane; readers divide. */
    public static WindowAggregation average(Extractor value) {
        return new WindowAggregation("average", java.util.Objects.requireNonNull(value, "value"));
    }

    /** The wire form: {@code {"aggregation":"sum","value":{…}}}. */
    public ObjectNode toWire() {
        ObjectNode node = Transport.json().createObjectNode();
        node.put("aggregation", aggregation);
        if (value != null) {
            node.set("value", value.toWire());
        }
        return node;
    }
}
