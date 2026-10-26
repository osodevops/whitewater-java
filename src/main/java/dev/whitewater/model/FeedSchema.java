package dev.whitewater.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Declared record schema metadata for a Domain or Feed: which format
 * record {@link #key}s and {@link #value} payloads carry. Every side
 * inherits independently — a Feed override supplies only the sides it
 * names and keeps the rest of its Domain's declaration; absent
 * everywhere means schema-agnostic bytes (this record is then
 * {@code null}).
 */
public record FeedSchema(SchemaDecl key, SchemaDecl value) {

    /** Both sides declared. */
    public static FeedSchema of(SchemaDecl key, SchemaDecl value) {
        return new FeedSchema(key, value);
    }

    /** Key side only — the value inherits. */
    public static FeedSchema key(SchemaDecl key) {
        return new FeedSchema(java.util.Objects.requireNonNull(key, "key is required"), null);
    }

    /** Value side only — the common case ({@code VALUE JSON}). */
    public static FeedSchema value(SchemaDecl value) {
        return new FeedSchema(null, java.util.Objects.requireNonNull(value, "value is required"));
    }

    /**
     * Field-level override: {@code lower} supplies every side this
     * schema does not declare — the Domain→Feed layering the catalog
     * applies to resolve {@code effective_schema}.
     */
    public FeedSchema layeredOver(FeedSchema lower) {
        return new FeedSchema(
                key != null ? key : (lower == null ? null : lower.key),
                value != null ? value : (lower == null ? null : lower.value));
    }

    /** The wire shape — absent sides serialize as missing keys. */
    public ObjectNode toWire() {
        var node = JsonNodeFactory.instance.objectNode();
        if (key != null) node.set("key", key.toWire());
        if (value != null) node.set("value", value.toWire());
        return node;
    }

    /** Decode a catalog row — {@code null} in, {@code null} out. */
    public static FeedSchema fromWire(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return null;
        }
        return new FeedSchema(
                SchemaDecl.fromWire(node.path("key")),
                SchemaDecl.fromWire(node.path("value")));
    }
}
