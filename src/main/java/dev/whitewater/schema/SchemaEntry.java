package dev.whitewater.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A schema row stored in the global {@code whitewater.schema.registry}
 * StateStore under primary key {@code <resource>|<target>}
 * (e.g. {@code orders.order.lines|message}).
 *
 * <p>When {@link #readOnly()} is {@code true}, this entry is a materialized
 * read-only linked share of {@link #sourceKey()} (optionally replicated from
 * {@link #sourceCluster()}) and cannot be edited directly.
 */
public record SchemaEntry(
        String key,
        String resource,
        SchemaTarget target,
        SchemaType type,
        SchemaCompatibility compatibility,
        int version,
        String definition,
        long updatedAtNs,
        List<SchemaVersion> versions,
        boolean readOnly,
        String sourceKey,
        String sourceCluster,
        List<String> linkedTargets) {

    public SchemaEntry(
            String key,
            String resource,
            SchemaTarget target,
            SchemaType type,
            SchemaCompatibility compatibility,
            int version,
            String definition,
            long updatedAtNs,
            List<SchemaVersion> versions) {
        this(key, resource, target, type, compatibility, version, definition, updatedAtNs,
                versions, false, null, null, List.of());
    }

    public SchemaEntry {
        Objects.requireNonNull(resource, "resource is required");
        Objects.requireNonNull(target, "target is required");
        Objects.requireNonNull(type, "type is required");
        Objects.requireNonNull(definition, "definition is required");
        key = (key == null || key.isBlank()) ? target.registryKey(resource) : key;
        compatibility = compatibility == null ? SchemaCompatibility.BACKWARD : compatibility;
        if (version < 1) {
            version = 1;
        }
        versions = versions == null || versions.isEmpty()
                ? List.of(new SchemaVersion(version, type, definition, updatedAtNs))
                : List.copyOf(versions);
        sourceKey = (sourceKey != null && sourceKey.isBlank()) ? null : sourceKey;
        sourceCluster = (sourceCluster != null && sourceCluster.isBlank()) ? null : sourceCluster;
        linkedTargets = linkedTargets == null ? List.of() : List.copyOf(linkedTargets);
    }

    public Optional<SchemaVersion> versionEntry(int versionNumber) {
        return versions.stream().filter(v -> v.version() == versionNumber).findFirst();
    }

    public ObjectNode toWire() {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("key", key);
        node.put("resource", resource);
        node.put("target", target.wire());
        node.put("type", type.wire());
        node.put("compatibility", compatibility.wire());
        node.put("version", version);
        node.put("definition", definition);
        node.put("updated_at_ns", Long.toString(updatedAtNs));
        node.put("read_only", readOnly);
        if (sourceKey != null) {
            node.put("source_key", sourceKey);
        }
        if (sourceCluster != null) {
            node.put("source_cluster", sourceCluster);
        }
        ArrayNode linkedArr = node.putArray("linked_targets");
        for (String lt : linkedTargets) {
            linkedArr.add(lt);
        }
        ArrayNode arr = node.putArray("versions");
        for (SchemaVersion v : versions) {
            arr.add(v.toWire());
        }
        return node;
    }

    public static SchemaEntry fromWire(String fallbackKey, JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return null;
        }
        String rawKey = node.path("key").asText(fallbackKey != null ? fallbackKey : "");
        SchemaTarget.ParsedKey parsed = SchemaTarget.parseKey(
                !rawKey.isBlank()
                        ? rawKey
                        : node.path("resource").asText("unknown.resource") + "|" + node.path("target").asText("message"));
        String resource = node.hasNonNull("resource")
                ? SchemaTarget.normalizeResource(node.path("resource").asText())
                : parsed.resource();
        SchemaTarget target = node.hasNonNull("target")
                ? SchemaTarget.fromWire(node.path("target").asText())
                : parsed.target();
        SchemaType type = SchemaType.fromWire(node.path("type").asText(node.path("schemaType").asText("JSON")));
        SchemaCompatibility compat = SchemaCompatibility.fromWire(node.path("compatibility").asText("BACKWARD"));
        int version = Math.max(1, node.path("version").asInt(1));
        String definition = node.path("definition").asText(node.path("schema").asText(""));
        long updatedAtNs = SchemaVersion.parseNs(node.path("updated_at_ns"));
        boolean readOnly = node.path("read_only").asBoolean(false);
        String sourceKey = node.hasNonNull("source_key") ? node.path("source_key").asText(null) : null;
        String sourceCluster = node.hasNonNull("source_cluster") ? node.path("source_cluster").asText(null) : null;
        List<String> linkedTargets = new ArrayList<>();
        JsonNode linkedNode = node.path("linked_targets");
        if (linkedNode.isArray()) {
            for (JsonNode lt : linkedNode) {
                if (lt.isTextual() && !lt.asText().isBlank()) {
                    linkedTargets.add(lt.asText().trim());
                }
            }
        }
        List<SchemaVersion> versions = new ArrayList<>();
        JsonNode versionsNode = node.path("versions");
        if (versionsNode.isArray()) {
            for (JsonNode item : versionsNode) {
                SchemaVersion sv = SchemaVersion.fromWire(item);
                if (sv != null) {
                    versions.add(sv);
                }
            }
        }
        if (versions.isEmpty()) {
            versions.add(new SchemaVersion(version, type, definition, updatedAtNs));
        }
        return new SchemaEntry(
                target.registryKey(resource),
                resource,
                target,
                type,
                compat,
                version,
                definition,
                updatedAtNs,
                versions,
                readOnly,
                sourceKey,
                sourceCluster,
                linkedTargets);
    }
}
