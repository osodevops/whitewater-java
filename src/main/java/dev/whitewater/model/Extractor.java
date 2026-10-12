package dev.whitewater.model;

import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.whitewater.internal.Transport;

/**
 * How a feed-sourced StateStore projects one committed source record to
 * a named key's typed lookup values — declared on
 * {@code addKey(..., extractor)} and applied by server-side replay.
 * Build with {@link #recordKey()}, {@link #metadata(String)}, or
 * {@link #json(String)}; manual stores take values on each {@code put}
 * instead and must leave the extractor unset.
 */
public record Extractor(String kind, String name, String extractor, Integer version, String field) {

    /** The source record's application Key. */
    public static Extractor recordKey() {
        return new Extractor("key", null, null, null, null);
    }

    /** A named record Metadata entry. */
    public static Extractor metadata(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("metadata extractor name is required");
        }
        return new Extractor("metadata", name, null, null, null);
    }

    /** A dotted JSON-path projection over the record payload. */
    public static Extractor json(String field) {
        if (field == null || field.isBlank()) {
            throw new IllegalArgumentException("json extractor field is required");
        }
        return new Extractor("projection", null, "json", 1, field);
    }

    /** Serialize to the wire {@code IndexFieldSpec} object. */
    public ObjectNode toWire() {
        ObjectNode node = Transport.json().createObjectNode();
        node.put("kind", kind);
        if (name != null) {
            node.put("name", name);
        }
        if (extractor != null) {
            node.put("extractor", extractor);
        }
        if (version != null) {
            node.put("version", version);
        }
        if (field != null) {
            node.put("field", field);
        }
        return node;
    }
}
