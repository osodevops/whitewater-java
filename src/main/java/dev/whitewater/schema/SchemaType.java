package dev.whitewater.schema;

import java.util.Locale;

/**
 * Supported schema definition languages in the Whitewater Schema Registry.
 */
public enum SchemaType {
    JSON("JSON"),
    AVRO("AVRO"),
    PROTOBUF("PROTOBUF");

    /** PascalCase aliases for ergonomic SDK calls. */
    public static final SchemaType Json = JSON;
    public static final SchemaType Avro = AVRO;
    public static final SchemaType Protobuf = PROTOBUF;

    private final String wire;

    SchemaType(String wire) {
        this.wire = wire;
    }

    public String wire() {
        return wire;
    }

    public static SchemaType fromWire(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("schema type is required (JSON, AVRO, or PROTOBUF)");
        }
        return switch (raw.trim().toUpperCase(Locale.ROOT)) {
            case "JSON", "JSONSCHEMA", "JSON_SCHEMA" -> JSON;
            case "AVRO" -> AVRO;
            case "PROTOBUF", "PROTO", "PROTO3", "PROTO2" -> PROTOBUF;
            default -> throw new IllegalArgumentException("unknown schema type: " + raw);
        };
    }
}
