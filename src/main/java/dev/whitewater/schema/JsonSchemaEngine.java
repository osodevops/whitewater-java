package dev.whitewater.schema;

import com.fasterxml.jackson.databind.JsonNode;
import dev.whitewater.internal.Transport;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * JSON Schema definition validator, payload validator, and evolution
 * compatibility checker.
 */
final class JsonSchemaEngine implements SchemaEngine {

    static final JsonSchemaEngine INSTANCE = new JsonSchemaEngine();

    private static final Set<String> VALID_TYPES = Set.of(
            "object", "array", "string", "number", "integer", "boolean", "null");

    private JsonSchemaEngine() {
    }

    @Override
    public SchemaType type() {
        return SchemaType.JSON;
    }

    @Override
    public List<String> validateDefinition(String definition) {
        if (definition == null || definition.isBlank()) {
            return List.of("JSON Schema definition must not be empty");
        }
        JsonNode root;
        try {
            root = Transport.json().readTree(definition);
        } catch (IOException e) {
            return List.of("Invalid JSON in schema definition: " + e.getMessage());
        }
        List<String> issues = new ArrayList<>();
        lintSchemaNode(root, "schema", issues);
        return issues;
    }

    private void lintSchemaNode(JsonNode node, String path, List<String> issues) {
        if (node == null || node.isNull() || (!node.isBoolean() && !node.isObject())) {
            issues.add(path + ": expected a JSON Schema object or boolean");
            return;
        }
        if (node.isBoolean()) {
            return;
        }
        if (node.has("type")) {
            JsonNode t = node.get("type");
            if (t.isTextual()) {
                if (!VALID_TYPES.contains(t.asText())) {
                    issues.add(path + ".type: unknown JSON Schema type '" + t.asText() + "'");
                }
            } else if (t.isArray()) {
                if (t.isEmpty()) {
                    issues.add(path + ".type: type array must not be empty");
                }
                for (JsonNode el : t) {
                    if (!el.isTextual() || !VALID_TYPES.contains(el.asText())) {
                        issues.add(path + ".type: invalid type '" + el + "'");
                    }
                }
            } else {
                issues.add(path + ".type: must be a string or array of strings");
            }
        }
        if (node.has("properties")) {
            JsonNode props = node.get("properties");
            if (!props.isObject()) {
                issues.add(path + ".properties: must be an object");
            } else {
                Iterator<Map.Entry<String, JsonNode>> fields = props.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> entry = fields.next();
                    lintSchemaNode(entry.getValue(), path + ".properties." + entry.getKey(), issues);
                }
            }
        }
        if (node.has("required")) {
            JsonNode req = node.get("required");
            if (!req.isArray()) {
                issues.add(path + ".required: must be an array of strings");
            } else {
                for (JsonNode item : req) {
                    if (!item.isTextual() || item.asText().isBlank()) {
                        issues.add(path + ".required: elements must be non-empty strings");
                    }
                }
            }
        }
        if (node.has("items")) {
            JsonNode items = node.get("items");
            if (!items.isArray()) {
                lintSchemaNode(items, path + ".items", issues);
            }
        }
        if (node.has("enum") && (!node.get("enum").isArray() || node.get("enum").isEmpty())) {
            issues.add(path + ".enum: must be a non-empty array");
        }
    }

    @Override
    public CompiledSchema compile(String definition) {
        List<String> errors = validateDefinition(definition);
        if (!errors.isEmpty()) {
            throw new IllegalArgumentException(String.join("; ", errors));
        }
        final JsonNode schemaRoot;
        try {
            schemaRoot = Transport.json().readTree(definition);
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid JSON Schema: " + e.getMessage(), e);
        }
        return payload -> {
            if (payload == null || payload.length == 0) {
                return List.of("payload is empty");
            }
            String rawText = new String(payload, StandardCharsets.UTF_8);
            JsonNode instance;
            try {
                instance = Transport.json().readTree(rawText);
            } catch (IOException e) {
                if (extractTypes(schemaRoot).contains("string")) {
                    instance = com.fasterxml.jackson.databind.node.TextNode.valueOf(rawText);
                } else {
                    return List.of("payload is not valid JSON: " + e.getMessage());
                }
            }
            List<String> issues = new ArrayList<>();
            validateInstance(schemaRoot, schemaRoot, instance, "$", issues);
            return issues;
        };
    }

    private void validateInstance(
            JsonNode rootSchema, JsonNode schema, JsonNode instance, String path, List<String> issues) {
        if (schema == null || schema.isNull()) return;
        if (schema.isBoolean()) {
            if (!schema.asBoolean()) issues.add(path + ": disallowed by schema=false");
            return;
        }
        if (schema.has("type")) {
            JsonNode typeNode = schema.get("type");
            boolean matched = false;
            if (typeNode.isTextual()) {
                matched = matchesType(typeNode.asText(), instance);
            } else if (typeNode.isArray()) {
                for (JsonNode t : typeNode) {
                    if (t.isTextual() && matchesType(t.asText(), instance)) {
                        matched = true;
                        break;
                    }
                }
            }
            if (!matched) {
                issues.add(path + ": expected type " + typeNode + " but got " + describeJsonType(instance));
                return;
            }
        }
        if (schema.has("enum") && schema.get("enum").isArray()) {
            boolean found = false;
            for (JsonNode allowed : schema.get("enum")) {
                if (allowed.equals(instance)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                issues.add(path + ": value " + instance + " is not in enum " + schema.get("enum"));
            }
        }
        if (instance.isObject()) {
            if (schema.has("required") && schema.get("required").isArray()) {
                for (JsonNode req : schema.get("required")) {
                    String field = req.asText();
                    if (!instance.has(field)) {
                        issues.add(path + ": missing required field '" + field + "'");
                    }
                }
            }
            JsonNode props = schema.get("properties");
            Set<String> knownProps = new HashSet<>();
            if (props != null && props.isObject()) {
                Iterator<Map.Entry<String, JsonNode>> it = props.fields();
                while (it.hasNext()) {
                    Map.Entry<String, JsonNode> prop = it.next();
                    knownProps.add(prop.getKey());
                    if (instance.has(prop.getKey())) {
                        validateInstance(rootSchema, prop.getValue(),
                                instance.get(prop.getKey()), path + "." + prop.getKey(), issues);
                    }
                }
            }
            if (schema.path("additionalProperties").isBoolean()
                    && !schema.path("additionalProperties").asBoolean()) {
                Iterator<String> fieldNames = instance.fieldNames();
                while (fieldNames.hasNext()) {
                    String fn = fieldNames.next();
                    if (!knownProps.contains(fn)) {
                        issues.add(path + ": unexpected additional property '" + fn + "'");
                    }
                }
            }
        } else if (instance.isArray() && schema.has("items") && schema.get("items").isObject()) {
            for (int i = 0; i < instance.size(); i++) {
                validateInstance(rootSchema, schema.get("items"), instance.get(i), path + "[" + i + "]", issues);
            }
        } else if (instance.isTextual()) {
            String s = instance.asText();
            if (schema.has("minLength") && s.length() < schema.get("minLength").asInt()) {
                issues.add(path + ": string length < minLength " + schema.get("minLength").asInt());
            }
            if (schema.has("maxLength") && s.length() > schema.get("maxLength").asInt()) {
                issues.add(path + ": string length > maxLength " + schema.get("maxLength").asInt());
            }
            if (schema.has("pattern") && schema.get("pattern").isTextual()) {
                String regex = schema.get("pattern").asText();
                if (!Pattern.compile(regex).matcher(s).find()) {
                    issues.add(path + ": string does not match pattern '" + regex + "'");
                }
            }
        } else if (instance.isNumber()) {
            double v = instance.asDouble();
            if (schema.has("minimum") && v < schema.get("minimum").asDouble()) {
                issues.add(path + ": value " + v + " < minimum " + schema.get("minimum").asDouble());
            }
            if (schema.has("maximum") && v > schema.get("maximum").asDouble()) {
                issues.add(path + ": value " + v + " > maximum " + schema.get("maximum").asDouble());
            }
        }
    }

    private static boolean matchesType(String expected, JsonNode node) {
        return switch (expected) {
            case "object" -> node.isObject();
            case "array" -> node.isArray();
            case "string" -> node.isTextual();
            case "number" -> node.isNumber();
            case "integer" -> node.isIntegralNumber();
            case "boolean" -> node.isBoolean();
            case "null" -> node.isNull();
            default -> false;
        };
    }

    private static String describeJsonType(JsonNode node) {
        if (node == null || node.isNull()) return "null";
        if (node.isObject()) return "object";
        if (node.isArray()) return "array";
        if (node.isTextual()) return "string";
        if (node.isIntegralNumber()) return "integer";
        if (node.isNumber()) return "number";
        if (node.isBoolean()) return "boolean";
        return node.getNodeType().name().toLowerCase();
    }

    @Override
    public List<String> checkBackwardCompatible(String readerDefinition, String writerDefinition) {
        try {
            JsonNode reader = Transport.json().readTree(readerDefinition);
            JsonNode writer = Transport.json().readTree(writerDefinition);
            List<String> issues = new ArrayList<>();
            compareSchemas(reader, writer, "$", issues);
            return issues;
        } catch (IOException e) {
            return List.of("Invalid JSON Schema: " + e.getMessage());
        }
    }

    private void compareSchemas(JsonNode reader, JsonNode writer, String path, List<String> issues) {
        if (reader == null || writer == null || !reader.isObject() || !writer.isObject()) {
            return;
        }
        Set<String> readerTypes = extractTypes(reader);
        Set<String> writerTypes = extractTypes(writer);
        if (!readerTypes.isEmpty() && !writerTypes.isEmpty()) {
            for (String wt : writerTypes) {
                boolean ok = readerTypes.contains(wt)
                        || ("integer".equals(wt) && readerTypes.contains("number"));
                if (!ok) {
                    issues.add(path + ": reader type " + readerTypes + " cannot read writer type '" + wt + "'");
                }
            }
        }
        Set<String> readerRequired = extractStringSet(reader.get("required"));
        Set<String> writerRequired = extractStringSet(writer.get("required"));
        JsonNode readerProps = reader.path("properties");
        JsonNode writerProps = writer.path("properties");
        for (String req : readerRequired) {
            if (!writerRequired.contains(req)) {
                JsonNode rProp = readerProps.path(req);
                if (!rProp.has("default")) {
                    issues.add(path + ": reader requires field '" + req + "' which is not required by writer and has no default");
                }
            }
        }
        if (reader.path("additionalProperties").isBoolean()
                && !reader.path("additionalProperties").asBoolean()
                && writerProps.isObject()) {
            Iterator<String> wFields = writerProps.fieldNames();
            while (wFields.hasNext()) {
                String wf = wFields.next();
                if (!readerProps.has(wf)) {
                    issues.add(path + ": reader disallows additionalProperties but writer defines property '" + wf + "'");
                }
            }
        }
        if (readerProps.isObject() && writerProps.isObject()) {
            Iterator<String> rFields = readerProps.fieldNames();
            while (rFields.hasNext()) {
                String field = rFields.next();
                if (writerProps.has(field)) {
                    compareSchemas(readerProps.get(field), writerProps.get(field), path + "." + field, issues);
                }
            }
        }
        if (reader.has("enum") && writer.has("enum")) {
            Set<JsonNode> rEnum = new HashSet<>();
            reader.get("enum").forEach(rEnum::add);
            for (JsonNode wVal : writer.get("enum")) {
                if (!rEnum.contains(wVal)) {
                    issues.add(path + ": writer enum symbol " + wVal + " is missing from reader enum");
                }
            }
        }
    }

    private static Set<String> extractTypes(JsonNode schema) {
        Set<String> out = new HashSet<>();
        JsonNode t = schema.get("type");
        if (t == null) return out;
        if (t.isTextual()) out.add(t.asText());
        else if (t.isArray()) t.forEach(el -> { if (el.isTextual()) out.add(el.asText()); });
        return out;
    }

    private static Set<String> extractStringSet(JsonNode arr) {
        Set<String> out = new HashSet<>();
        if (arr != null && arr.isArray()) {
            arr.forEach(el -> { if (el.isTextual()) out.add(el.asText()); });
        }
        return out;
    }
}
