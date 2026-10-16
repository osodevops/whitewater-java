package dev.whitewater.model;

import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.whitewater.internal.Transport;
import dev.whitewater.internal.Wire;

import java.util.List;
import java.util.Objects;

/**
 * One Index read shape: {@link #exact} supplies every declared field,
 * {@link #prefix} matches leading complete fields plus an optional text
 * prefix on the next, {@link #range} bounds the field after the
 * {@code leading} exact-equals.
 */
public record IndexPredicate(
        String mode,
        List<Object> values,
        List<Object> leading,
        String partialText,
        IndexBound lower,
        IndexBound upper) {

    /** One inclusive/exclusive endpoint of a bounded range. */
    public record IndexBound(Object value, boolean inclusive) {
        public static IndexBound inclusive(Object value) {
            return new IndexBound(value, true);
        }

        public static IndexBound exclusive(Object value) {
            return new IndexBound(value, false);
        }

        ObjectNode toWire() {
            ObjectNode node = Transport.json().createObjectNode();
            node.set("value", Wire.indexValue(value));
            node.put("inclusive", inclusive);
            return node;
        }
    }

    /** Every declared field, exactly. */
    public static IndexPredicate exact(Object... values) {
        return new IndexPredicate("exact", List.of(values), null, null, null, null);
    }

    /** Leading exact fields plus an optional text prefix on the next field. */
    public static IndexPredicate prefix(List<Object> leading, String partialText) {
        return new IndexPredicate("prefix", null,
                leading == null ? List.of() : List.copyOf(leading), partialText, null, null);
    }

    /** Leading exact fields plus {@code lower ≤ field ≤ upper} on the next. */
    public static IndexPredicate range(List<Object> leading, IndexBound lower, IndexBound upper) {
        return new IndexPredicate("range", null,
                leading == null ? List.of() : List.copyOf(leading), null,
                Objects.requireNonNull(lower, "lower"), Objects.requireNonNull(upper, "upper"));
    }

    /**
     * The {@code GET INDEX} command form: externally tagged
     * {@code {"exact":{"values":[…]}}}.
     */
    public ObjectNode toCommandWire() {
        ObjectNode node = Transport.json().createObjectNode();
        node.set(mode, innerWire());
        return node;
    }

    /**
     * The {@code /v1/indexes/query} form: internally tagged
     * {@code {"mode":"exact","values":[…]}} flattened into the request.
     */
    public void flattenInto(ObjectNode request) {
        request.put("mode", mode);
        innerWire().fields().forEachRemaining(entry -> request.set(entry.getKey(), entry.getValue()));
    }

    private ObjectNode innerWire() {
        ObjectNode node = Transport.json().createObjectNode();
        switch (mode) {
            case "exact" -> {
                var values = node.putArray("values");
                this.values.forEach(value -> values.add(Wire.indexValue(value)));
            }
            case "prefix" -> {
                var leading = node.putArray("leading");
                this.leading.forEach(value -> leading.add(Wire.indexValue(value)));
                if (partialText != null) {
                    node.put("partial_text", partialText);
                }
            }
            case "range" -> {
                var leading = node.putArray("leading");
                this.leading.forEach(value -> leading.add(Wire.indexValue(value)));
                node.set("lower", lower.toWire());
                node.set("upper", upper.toWire());
            }
            default -> throw new IllegalStateException("unknown mode " + mode);
        }
        return node;
    }
}
