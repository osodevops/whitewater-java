package dev.whitewater.schema;

import com.fasterxml.jackson.databind.JsonNode;
import dev.whitewater.internal.Transport;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.apache.avro.SchemaCompatibility.SchemaPairCompatibility;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

/**
 * Apache Avro schema definition validator, payload validator (supporting both
 * JSON-represented records and binary Avro datum payloads), and reader/writer
 * compatibility checker.
 */
final class AvroSchemaEngine implements SchemaEngine {

    static final AvroSchemaEngine INSTANCE = new AvroSchemaEngine();

    private AvroSchemaEngine() {
    }

    @Override
    public SchemaType type() {
        return SchemaType.AVRO;
    }

    @Override
    public List<String> validateDefinition(String definition) {
        if (definition == null || definition.isBlank()) {
            return List.of("Avro schema definition must not be empty");
        }
        try {
            new Schema.Parser().parse(definition);
            return List.of();
        } catch (RuntimeException e) {
            return List.of("Invalid Avro schema: " + e.getMessage());
        }
    }

    @Override
    public CompiledSchema compile(String definition) {
        final Schema schema;
        try {
            schema = new Schema.Parser().parse(definition);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Invalid Avro schema: " + e.getMessage(), e);
        }
        return payload -> {
            if (payload == null || payload.length == 0) {
                if (schema.getType() == Schema.Type.NULL) {
                    return List.of();
                }
                return List.of("Avro payload is empty");
            }
            if (looksLikeJson(payload, schema)) {
                try {
                    JsonNode node = Transport.json().readTree(new String(payload, StandardCharsets.UTF_8));
                    List<String> issues = new ArrayList<>();
                    validateAvroJson(schema, node, "$", issues);
                    return issues;
                } catch (IOException ignored) {
                    // Fall through to binary decoder check
                }
            }
            try {
                ByteArrayInputStream in = new ByteArrayInputStream(payload);
                BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(in, null);
                GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);
                reader.read(null, decoder);
                if (in.available() > 0) {
                    return List.of("Avro binary payload has " + in.available() + " trailing unconsumed bytes");
                }
                return List.of();
            } catch (Exception e) {
                return List.of("Avro payload validation failed: " + e.getMessage());
            }
        };
    }

    private static boolean looksLikeJson(byte[] payload, Schema schema) {
        int i = 0;
        while (i < payload.length && (payload[i] == ' ' || payload[i] == '\t' || payload[i] == '\n' || payload[i] == '\r')) {
            i++;
        }
        if (i >= payload.length) {
            return false;
        }
        byte first = payload[i];
        if (schema.getType() == Schema.Type.RECORD || schema.getType() == Schema.Type.MAP) {
            return first == '{';
        }
        if (schema.getType() == Schema.Type.ARRAY) {
            return first == '[';
        }
        return first == '{' || first == '[' || first == '"';
    }

    private void validateAvroJson(Schema schema, JsonNode node, String path, List<String> issues) {
        switch (schema.getType()) {
            case NULL -> {
                if (!node.isNull()) issues.add(path + ": expected null");
            }
            case BOOLEAN -> {
                if (!node.isBoolean()) issues.add(path + ": expected boolean");
            }
            case INT -> {
                if (!node.isIntegralNumber() || !node.canConvertToInt()) {
                    issues.add(path + ": expected 32-bit int");
                }
            }
            case LONG -> {
                if (!node.isIntegralNumber() || !node.canConvertToLong()) {
                    issues.add(path + ": expected 64-bit long");
                }
            }
            case FLOAT, DOUBLE -> {
                if (!node.isNumber()) issues.add(path + ": expected number");
            }
            case STRING, BYTES, FIXED -> {
                if (!node.isTextual()) issues.add(path + ": expected string");
            }
            case ENUM -> {
                if (!node.isTextual() || !schema.getEnumSymbols().contains(node.asText())) {
                    issues.add(path + ": expected one of enum " + schema.getEnumSymbols() + " but got " + node);
                }
            }
            case ARRAY -> {
                if (!node.isArray()) {
                    issues.add(path + ": expected array");
                } else {
                    for (int i = 0; i < node.size(); i++) {
                        validateAvroJson(schema.getElementType(), node.get(i), path + "[" + i + "]", issues);
                    }
                }
            }
            case MAP -> {
                if (!node.isObject()) {
                    issues.add(path + ": expected map object");
                } else {
                    Iterator<String> names = node.fieldNames();
                    while (names.hasNext()) {
                        String k = names.next();
                        validateAvroJson(schema.getValueType(), node.get(k), path + "." + k, issues);
                    }
                }
            }
            case UNION -> {
                boolean matched = false;
                for (Schema branch : schema.getTypes()) {
                    if (branch.getType() == Schema.Type.NULL && node.isNull()) {
                        matched = true;
                        break;
                    }
                    List<String> branchIssues = new ArrayList<>();
                    validateAvroJson(branch, node, path, branchIssues);
                    if (branchIssues.isEmpty()) {
                        matched = true;
                        break;
                    }
                    if (node.isObject() && node.size() == 1 && node.has(branch.getFullName())) {
                        List<String> wrappedIssues = new ArrayList<>();
                        validateAvroJson(branch, node.get(branch.getFullName()), path, wrappedIssues);
                        if (wrappedIssues.isEmpty()) {
                            matched = true;
                            break;
                        }
                    }
                }
                if (!matched) {
                    issues.add(path + ": value does not match any branch of Avro union " + schema);
                }
            }
            case RECORD -> {
                if (!node.isObject()) {
                    issues.add(path + ": expected record object for " + schema.getName());
                    return;
                }
                Set<String> declared = new HashSet<>();
                for (Schema.Field field : schema.getFields()) {
                    declared.add(field.name());
                    if (!node.has(field.name())) {
                        if (!field.hasDefaultValue() && !allowsNullUnion(field.schema())) {
                            issues.add(path + ": missing required Avro field '" + field.name() + "'");
                        }
                    } else {
                        validateAvroJson(field.schema(), node.get(field.name()), path + "." + field.name(), issues);
                    }
                }
                Iterator<String> actual = node.fieldNames();
                while (actual.hasNext()) {
                    String fn = actual.next();
                    if (!declared.contains(fn)) {
                        issues.add(path + ": undeclared Avro field '" + fn + "' on record " + schema.getName());
                    }
                }
            }
        }
    }

    private static boolean allowsNullUnion(Schema s) {
        if (s.getType() == Schema.Type.NULL) return true;
        if (s.getType() == Schema.Type.UNION) {
            return s.getTypes().stream().anyMatch(b -> b.getType() == Schema.Type.NULL);
        }
        return false;
    }

    @Override
    public List<String> checkBackwardCompatible(String readerDefinition, String writerDefinition) {
        try {
            Schema reader = new Schema.Parser().parse(readerDefinition);
            Schema writer = new Schema.Parser().parse(writerDefinition);
            SchemaPairCompatibility result =
                    org.apache.avro.SchemaCompatibility.checkReaderWriterCompatibility(reader, writer);
            if (result.getType() == SchemaCompatibilityType.COMPATIBLE) {
                return List.of();
            }
            List<String> issues = new ArrayList<>();
            result.getResult().getIncompatibilities().forEach(inc ->
                    issues.add(inc.getType() + " at " + inc.getLocation() + ": " + inc.getMessage()));
            if (issues.isEmpty()) {
                issues.add(result.getDescription());
            }
            return issues;
        } catch (RuntimeException e) {
            return List.of("Invalid Avro schema: " + e.getMessage());
        }
    }
}
