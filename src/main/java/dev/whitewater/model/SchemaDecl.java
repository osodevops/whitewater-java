package dev.whitewater.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Objects;

/**
 * One side of a declared record schema — the wire format applications
 * write and readers decode, plus the registry-style locator tooling
 * resolves definitions through. Declared via
 * {@code SET SCHEMA ON DOMAIN|FEED ...} and surfaced on
 * {@link FeedSchema}. Storage stays schema-neutral bytes; the
 * declaration is descriptive catalog metadata — compatibility gates
 * and a versioned registry are the schema companion roadmap.
 */
public record SchemaDecl(String format, String subject, String definition) {

    /** Built-in formats the catalog validates. */
    public static final String JSON = "json";
    public static final String JSONSCHEMA = "jsonschema";
    public static final String AVRO = "avro";
    public static final String PROTOBUF = "protobuf";
    public static final String TEXT = "text";
    public static final String BYTES = "bytes";

    /** A format-only declaration — {@code VALUE JSON}. */
    public static SchemaDecl of(String format) {
        return new SchemaDecl(Objects.requireNonNull(format, "format is required"), null, null);
    }

    /** A format + registry subject — {@code VALUE JSON SUBJECT 'order-v1'}. */
    public static SchemaDecl of(String format, String subject) {
        return new SchemaDecl(Objects.requireNonNull(format, "format is required"), subject, null);
    }

    /** Format, subject, and an inline definition. */
    public static SchemaDecl of(String format, String subject, String definition) {
        return new SchemaDecl(Objects.requireNonNull(format, "format is required"), subject, definition);
    }

    /** A format outside the built-in set — {@code VALUE CUSTOM msgpack} wires as {@code custom.<name>}. */
    public static SchemaDecl custom(String name) {
        return of("custom." + Objects.requireNonNull(name, "name is required"));
    }

    public SchemaDecl {
        if (format == null || format.isBlank()) {
            throw new IllegalArgumentException("schema format is required");
        }
    }

    /** The wire shape — absent optionals serialize as missing keys. */
    public ObjectNode toWire() {
        var node = JsonNodeFactory.instance.objectNode().put("format", format);
        if (subject != null) node.put("subject", subject);
        if (definition != null) node.put("definition", definition);
        return node;
    }

    /** Decode a catalog row side — {@code null} in, {@code null} out. */
    public static SchemaDecl fromWire(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return null;
        }
        return new SchemaDecl(
                node.path("format").asText(null),
                node.path("subject").isTextual() ? node.path("subject").asText() : null,
                node.path("definition").isTextual() ? node.path("definition").asText() : null);
    }
}
