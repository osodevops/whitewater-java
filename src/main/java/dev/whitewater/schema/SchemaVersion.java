package dev.whitewater.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Objects;

/**
 * One immutable version snapshot of a schema entry in {@code schema.registry}.
 */
public record SchemaVersion(
        int version,
        SchemaType type,
        String definition,
        long createdAtNs) {

    public SchemaVersion {
        if (version < 1) {
            throw new IllegalArgumentException("version must be >= 1");
        }
        Objects.requireNonNull(type, "type is required");
        Objects.requireNonNull(definition, "definition is required");
    }

    public ObjectNode toWire() {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("version", version);
        node.put("type", type.wire());
        node.put("definition", definition);
        node.put("created_at_ns", Long.toString(createdAtNs));
        return node;
    }

    public static SchemaVersion fromWire(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return null;
        }
        int v = Math.max(1, node.path("version").asInt(1));
        SchemaType t = SchemaType.fromWire(node.path("type").asText("JSON"));
        String def = node.path("definition").asText("");
        long created = parseNs(node.path("created_at_ns"));
        return new SchemaVersion(v, t, def, created);
    }

    static long parseNs(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return 0L;
        }
        if (node.isNumber()) {
            return node.asLong();
        }
        try {
            return Long.parseLong(node.asText("0").trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
}
