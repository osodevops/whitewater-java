package dev.whitewater.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.whitewater.WhitewaterException;
import dev.whitewater.internal.Transport;
import dev.whitewater.internal.Wire;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Client-side Schema Registry backed by the global replicated
 * {@code whitewater.schema.registry} StateStore.
 *
 * <p>Each schema association is stored at key {@code <resource>|key} or
 * {@code <resource>|message}. Writes to Feeds and StateStores automatically
 * resolve and cache the target's schema in a single lookup per slot and
 * validate payloads client-side before sending bytes over the wire.
 */
public final class SchemaRegistry {

    public static final String STORE_DOMAIN = "whitewater";
    public static final String STORE_NAME = "whitewater.schema.registry";
    public static final java.time.Duration DEFAULT_CACHE_TTL = java.time.Duration.ofMinutes(5);

    private record CachedSlot(SchemaEntry entry, SchemaEngine.CompiledSchema compiled, long expiresAtNs) {}

    private final Transport transport;
    private final ConcurrentMap<String, CachedSlot> slotCache = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, String> writerFeeds = new ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicLong lookupCount = new java.util.concurrent.atomic.AtomicLong();
    private volatile long cacheTtlNs = DEFAULT_CACHE_TTL.toNanos();
    private volatile boolean storeEnsured;

    public SchemaRegistry(Transport transport) {
        this.transport = Objects.requireNonNull(transport, "transport");
    }

    /** True when {@code storeName} names the internal schema registry store. */
    public static boolean isReservedStoreName(String storeName) {
        if (storeName == null) return false;
        String norm = storeName.trim().toLowerCase(Locale.ROOT);
        return STORE_NAME.equals(norm)
                || "schema.registry".equals(norm)
                || "schema-registry".equals(norm)
                || "whitewater.schema-registry".equals(norm);
    }

    public void registerWriterFeed(String writer, String feed) {
        if (writer != null && feed != null) {
            writerFeeds.put(writer, feed);
        }
    }

    public String feedForWriter(String writer) {
        return writer == null ? null : writerFeeds.get(writer);
    }

    public SchemaRegistry setCacheTtl(java.time.Duration ttl) {
        Objects.requireNonNull(ttl, "ttl is required");
        this.cacheTtlNs = Math.max(0L, ttl.toNanos());
        long capExp = System.nanoTime() + this.cacheTtlNs;
        slotCache.replaceAll((k, v) -> (v.entry() != null && v.expiresAtNs() > capExp)
                ? new CachedSlot(v.entry(), v.compiled(), capExp)
                : v);
        return this;
    }

    public java.time.Duration cacheTtl() {
        return java.time.Duration.ofNanos(cacheTtlNs);
    }

    public long lookupCount() {
        return lookupCount.get();
    }

    /** Clear cached schema lookups so subsequent writes re-read from the store. */
    public void refreshCache() {
        slotCache.clear();
    }

    /** Validate a Feed or StateStore write against any associated key/message schemas. */
    public void validateResourceWrite(String resource, byte[] keyBytes, byte[] messageBytes) {
        if (resource == null || resource.isBlank() || isReservedStoreName(resource)) {
            return;
        }
        String normalized = SchemaTarget.normalizeResource(resource);
        List<String> violations = new ArrayList<>();
        CachedSlot keySlot = resolveSlot(normalized, SchemaTarget.KEY);
        if (keySlot.entry() != null && keyBytes != null && keyBytes.length > 0) {
            for (String err : keySlot.compiled().validatePayload(keyBytes)) {
                violations.add(normalized + "|key (" + keySlot.entry().type().wire() + "): " + err);
            }
        }
        CachedSlot msgSlot = resolveSlot(normalized, SchemaTarget.MESSAGE);
        if (msgSlot.entry() != null) {
            byte[] payload = messageBytes == null ? new byte[0] : messageBytes;
            for (String err : msgSlot.compiled().validatePayload(payload)) {
                violations.add(normalized + "|message (" + msgSlot.entry().type().wire() + "): " + err);
            }
        }
        if (!violations.isEmpty()) {
            throw new WhitewaterException.SchemaValidation(
                    "Schema validation failed for " + normalized + ": " + String.join("; ", violations),
                    violations);
        }
    }

    private CachedSlot resolveSlot(String resource, SchemaTarget target) {
        String regKey = target.registryKey(resource);
        long now = System.nanoTime();
        CachedSlot existing = slotCache.get(regKey);
        if (existing != null && (existing.entry() == null || now < existing.expiresAtNs())) {
            return existing;
        }
        Optional<SchemaEntry> fetched = lookupDirect(resource, target);
        if (fetched.isEmpty()) {
            CachedSlot emptySlot = new CachedSlot(null, null, Long.MAX_VALUE);
            slotCache.put(regKey, emptySlot);
            return emptySlot;
        }
        SchemaEntry se = fetched.get();
        if (existing != null && existing.entry() != null
                && existing.entry().version() == se.version()
                && existing.entry().definition().equals(se.definition())) {
            CachedSlot renewed = new CachedSlot(se, existing.compiled(), now + cacheTtlNs);
            slotCache.put(regKey, renewed);
            return renewed;
        }
        SchemaEngine.CompiledSchema compiled =
                SchemaEngine.forType(se.type()).compile(se.definition());
        CachedSlot freshSlot = new CachedSlot(se, compiled, now + cacheTtlNs);
        slotCache.put(regKey, freshSlot);
        return freshSlot;
    }

    /** Single lookup in {@code whitewater.schema.registry} for {@code <resource>|<target>}. */
    public Optional<SchemaEntry> getSchema(String resource, SchemaTarget target) {
        String normalized = SchemaTarget.normalizeResource(resource);
        Objects.requireNonNull(target, "target is required");
        return lookupDirect(normalized, target);
    }

    private Optional<SchemaEntry> lookupDirect(String resource, SchemaTarget target) {
        String regKey = target.registryKey(resource);
        ObjectNode body = Transport.json().createObjectNode();
        body.put("store", STORE_NAME);
        body.put("key_base64", Wire.b64(regKey.getBytes(StandardCharsets.UTF_8)));
        body.put("strict", false);
        lookupCount.incrementAndGet();
        try {
            JsonNode resp = transport.post("/v1/state-stores/lookup", body);
            if (!resp.path("found").asBoolean(false) || !resp.hasNonNull("payload_base64")) {
                return Optional.empty();
            }
            byte[] bytes = java.util.Base64.getDecoder().decode(resp.path("payload_base64").asText());
            JsonNode payloadJson = Transport.json().readTree(bytes);
            return Optional.ofNullable(SchemaEntry.fromWire(regKey, payloadJson));
        } catch (WhitewaterException.Refused | WhitewaterException.Unavailable e) {
            String msg = String.valueOf(e.getMessage()).toLowerCase(Locale.ROOT);
            if (msg.contains("does not exist") || msg.contains("not found") || msg.contains("no mutation-journal")) {
                return Optional.empty();
            }
            throw e;
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    /** Check compatibility of a candidate schema definition against existing versions. */
    public SchemaCompatibilityResult checkCompatibility(
            String resource,
            String definition,
            SchemaTarget target,
            SchemaType type) {
        String normalized = SchemaTarget.normalizeResource(resource);
        Objects.requireNonNull(target, "target is required");
        Objects.requireNonNull(type, "type is required");
        List<String> syntaxErrors = SchemaEngine.forType(type).validateDefinition(definition);
        if (!syntaxErrors.isEmpty()) {
            return SchemaCompatibilityResult.incompatible(syntaxErrors);
        }
        Optional<SchemaEntry> existing = lookupDirect(normalized, target);
        if (existing.isEmpty()) {
            return SchemaCompatibilityResult.ok();
        }
        SchemaEntry current = existing.get();
        return SchemaEngine.evaluateCompatibility(
                type, definition, current.versions(), current.compatibility());
    }

    /**
     * Register or evolve the schema for {@code <resource>|<target>} in
     * {@code whitewater.schema.registry}. Validates definition syntax and
     * evolution compatibility before committing.
     */
    public SchemaEntry updateSchema(
            String resource,
            String definition,
            SchemaTarget target,
            SchemaType type,
            SchemaCompatibility compatibility) {
        String normalized = SchemaTarget.normalizeResource(resource);
        if (isReservedStoreName(normalized)) {
            throw new IllegalArgumentException(
                    STORE_NAME + " is an internal system store and cannot have user schemas attached");
        }
        Objects.requireNonNull(definition, "schema definition is required");
        Objects.requireNonNull(target, "schema target is required");
        Objects.requireNonNull(type, "schema type is required");

        SchemaEngine engine = SchemaEngine.forType(type);
        List<String> defIssues = engine.validateDefinition(definition);
        if (!defIssues.isEmpty()) {
            throw new WhitewaterException.SchemaValidation(
                    "Invalid " + type.wire() + " schema definition: " + String.join("; ", defIssues),
                    defIssues);
        }

        ensureStoreExists();
        Optional<SchemaEntry> existingOpt = lookupDirect(normalized, target);
        long nowNs = System.currentTimeMillis() * 1_000_000L;
        String regKey = target.registryKey(normalized);

        SchemaEntry nextEntry;
        if (existingOpt.isEmpty()) {
            SchemaCompatibility mode = compatibility == null ? SchemaCompatibility.BACKWARD : compatibility;
            SchemaVersion v1 = new SchemaVersion(1, type, definition, nowNs);
            nextEntry = new SchemaEntry(
                    regKey, normalized, target, type, mode, 1, definition, nowNs,
                    List.of(v1), false, null, null, List.of());
        } else {
            SchemaEntry current = existingOpt.get();
            if (current.readOnly()) {
                String origin = current.sourceCluster() != null
                        ? current.sourceCluster() + ":" + current.sourceKey()
                        : String.valueOf(current.sourceKey());
                throw new WhitewaterException.SchemaValidation(
                        regKey + " is a read-only linked schema (source: " + origin + ") — update the source schema instead");
            }
            SchemaCompatibility mode = compatibility != null ? compatibility : current.compatibility();
            if (current.type() == type && current.definition().trim().equals(definition.trim())) {
                if (mode == current.compatibility()) {
                    SchemaEngine.CompiledSchema compiled = engine.compile(current.definition());
                    slotCache.put(regKey, new CachedSlot(current, compiled, System.nanoTime() + cacheTtlNs));
                    return current;
                }
                nextEntry = new SchemaEntry(
                        regKey, normalized, target, current.type(), mode,
                        current.version(), current.definition(), nowNs, current.versions(),
                        false, null, null, current.linkedTargets());
            } else {
                SchemaCompatibilityResult compat = SchemaEngine.evaluateCompatibility(
                        type, definition, current.versions(), mode);
                if (!compat.compatible()) {
                    throw new WhitewaterException.SchemaValidation(
                            "Incompatible schema evolution for " + regKey + " (" + mode.wire() + "): "
                                    + String.join("; ", compat.messages()),
                            compat.messages());
                }
                int nextVersion = current.version() + 1;
                List<SchemaVersion> nextVersions = new ArrayList<>(current.versions());
                nextVersions.add(new SchemaVersion(nextVersion, type, definition, nowNs));
                nextEntry = new SchemaEntry(
                        regKey, normalized, target, type, mode,
                        nextVersion, definition, nowNs, nextVersions,
                        false, null, null, current.linkedTargets());
            }
        }

        putEntry(regKey, nextEntry);
        SchemaEngine.CompiledSchema compiled = engine.compile(nextEntry.definition());
        slotCache.put(regKey, new CachedSlot(nextEntry, compiled, System.nanoTime() + cacheTtlNs));
        materializeLinkedTargets(nextEntry, compiled);
        return nextEntry;
    }

    /** Update the evolution compatibility mode for an existing schema entry. */
    public SchemaEntry updateCompatibility(
            String resource, SchemaTarget target, SchemaCompatibility compatibility) {
        String normalized = SchemaTarget.normalizeResource(resource);
        Objects.requireNonNull(target, "target is required");
        Objects.requireNonNull(compatibility, "compatibility is required");
        SchemaEntry current = lookupDirect(normalized, target)
                .orElseThrow(() -> new WhitewaterException.Refused(
                        404, "schema.not_found",
                        "Schema not found for " + target.registryKey(normalized)));
        if (current.readOnly()) {
            throw new WhitewaterException.SchemaValidation(
                    current.key() + " is a read-only linked schema — update the source schema instead");
        }
        long nowNs = System.currentTimeMillis() * 1_000_000L;
        SchemaEntry updated = new SchemaEntry(
                current.key(), current.resource(), current.target(), current.type(),
                compatibility, current.version(), current.definition(), nowNs, current.versions(),
                false, null, null, current.linkedTargets());
        putEntry(updated.key(), updated);
        SchemaEngine.CompiledSchema compiled =
                SchemaEngine.forType(updated.type()).compile(updated.definition());
        slotCache.put(updated.key(), new CachedSlot(updated, compiled, System.nanoTime() + cacheTtlNs));
        materializeLinkedTargets(updated, compiled);
        return updated;
    }

    /**
     * Attach {@code linkedResource|linkedTarget} as a read-only linked share of
     * {@code sourceResource|sourceTarget}.
     */
    public SchemaEntry linkSchema(
            String sourceResource,
            SchemaTarget sourceTarget,
            String linkedResource,
            SchemaTarget linkedTarget) {
        String normSource = SchemaTarget.normalizeResource(sourceResource);
        String normLinked = SchemaTarget.normalizeResource(linkedResource);
        Objects.requireNonNull(sourceTarget, "sourceTarget is required");
        Objects.requireNonNull(linkedTarget, "linkedTarget is required");
        if (isReservedStoreName(normLinked)) {
            throw new IllegalArgumentException(STORE_NAME + " is an internal system store");
        }
        String sourceKey = sourceTarget.registryKey(normSource);
        String linkedKey = linkedTarget.registryKey(normLinked);
        if (sourceKey.equals(linkedKey)) {
            throw new IllegalArgumentException("Cannot link a schema to itself: " + sourceKey);
        }
        SchemaEntry owner = lookupDirect(normSource, sourceTarget)
                .orElseThrow(() -> new WhitewaterException.Refused(
                        404, "schema.not_found", "Source schema not found: " + sourceKey));
        if (owner.readOnly()) {
            throw new WhitewaterException.SchemaValidation(
                    sourceKey + " is itself a read-only linked schema (source: " + owner.sourceKey() + ")");
        }
        ensureStoreExists();
        List<String> nextTargets = new ArrayList<>(owner.linkedTargets());
        if (!nextTargets.contains(linkedKey)) {
            nextTargets.add(linkedKey);
        }
        long nowNs = System.currentTimeMillis() * 1_000_000L;
        SchemaEntry updatedOwner = new SchemaEntry(
                owner.key(), owner.resource(), owner.target(), owner.type(),
                owner.compatibility(), owner.version(), owner.definition(), nowNs,
                owner.versions(), false, null, null, nextTargets);
        putEntry(updatedOwner.key(), updatedOwner);
        SchemaEngine.CompiledSchema compiled =
                SchemaEngine.forType(updatedOwner.type()).compile(updatedOwner.definition());
        slotCache.put(updatedOwner.key(), new CachedSlot(updatedOwner, compiled, System.nanoTime() + cacheTtlNs));

        SchemaEntry copy = new SchemaEntry(
                linkedKey, normLinked, linkedTarget, updatedOwner.type(),
                updatedOwner.compatibility(), updatedOwner.version(), updatedOwner.definition(),
                nowNs, updatedOwner.versions(), true, updatedOwner.key(), null, List.of());
        putEntry(linkedKey, copy);
        slotCache.put(linkedKey, new CachedSlot(copy, compiled, System.nanoTime() + cacheTtlNs));
        return copy;
    }

    /** Detach a read-only linked target from {@code sourceResource|sourceTarget} and remove its row. */
    public SchemaEntry unlinkSchema(
            String sourceResource,
            SchemaTarget sourceTarget,
            String linkedResource,
            SchemaTarget linkedTarget) {
        String normSource = SchemaTarget.normalizeResource(sourceResource);
        String normLinked = SchemaTarget.normalizeResource(linkedResource);
        Objects.requireNonNull(sourceTarget, "sourceTarget is required");
        Objects.requireNonNull(linkedTarget, "linkedTarget is required");
        String sourceKey = sourceTarget.registryKey(normSource);
        String linkedKey = linkedTarget.registryKey(normLinked);
        SchemaEntry owner = lookupDirect(normSource, sourceTarget)
                .orElseThrow(() -> new WhitewaterException.Refused(
                        404, "schema.not_found", "Source schema not found: " + sourceKey));
        ensureStoreExists();
        List<String> nextTargets = owner.linkedTargets().stream()
                .filter(k -> !k.equals(linkedKey))
                .toList();
        long nowNs = System.currentTimeMillis() * 1_000_000L;
        SchemaEntry updatedOwner = new SchemaEntry(
                owner.key(), owner.resource(), owner.target(), owner.type(),
                owner.compatibility(), owner.version(), owner.definition(), nowNs,
                owner.versions(), false, null, null, nextTargets);
        putEntry(updatedOwner.key(), updatedOwner);
        SchemaEngine.CompiledSchema compiled =
                SchemaEngine.forType(updatedOwner.type()).compile(updatedOwner.definition());
        slotCache.put(updatedOwner.key(), new CachedSlot(updatedOwner, compiled, System.nanoTime() + cacheTtlNs));
        deleteKeyDirect(linkedKey);
        return updatedOwner;
    }

    /** Replicate {@code sourceEntry} into this cluster's {@code whitewater.schema.registry} as a read-only replica. */
    public SchemaEntry replicateEntry(SchemaEntry sourceEntry, String sourceCluster) {
        Objects.requireNonNull(sourceEntry, "sourceEntry is required");
        ensureStoreExists();
        long nowNs = System.currentTimeMillis() * 1_000_000L;
        String originKey = sourceEntry.sourceKey() != null ? sourceEntry.sourceKey() : sourceEntry.key();
        SchemaEntry replica = new SchemaEntry(
                sourceEntry.key(),
                sourceEntry.resource(),
                sourceEntry.target(),
                sourceEntry.type(),
                sourceEntry.compatibility(),
                sourceEntry.version(),
                sourceEntry.definition(),
                nowNs,
                sourceEntry.versions(),
                true,
                originKey,
                sourceCluster,
                List.of());
        putEntry(replica.key(), replica);
        SchemaEngine.CompiledSchema compiled =
                SchemaEngine.forType(replica.type()).compile(replica.definition());
        slotCache.put(replica.key(), new CachedSlot(replica, compiled, System.nanoTime() + cacheTtlNs));
        return replica;
    }

    /** Remove a schema entry (and any materialized read-only linked copies) from {@code whitewater.schema.registry}. */
    public boolean removeSchema(String resource, SchemaTarget target) {
        String normalized = SchemaTarget.normalizeResource(resource);
        Objects.requireNonNull(target, "target is required");
        String regKey = target.registryKey(normalized);
        Optional<SchemaEntry> existingOpt = lookupDirect(normalized, target);
        if (existingOpt.isEmpty()) {
            slotCache.remove(regKey);
            return false;
        }
        ensureStoreExists();
        SchemaEntry existing = existingOpt.get();
        for (String linkedKey : existing.linkedTargets()) {
            deleteKeyDirect(linkedKey);
        }
        deleteKeyDirect(regKey);
        return true;
    }

    private void materializeLinkedTargets(SchemaEntry owner, SchemaEngine.CompiledSchema compiled) {
        for (String linkedKey : owner.linkedTargets()) {
            SchemaTarget.ParsedKey parsed = SchemaTarget.parseKey(linkedKey);
            SchemaEntry copy = new SchemaEntry(
                    parsed.registryKey(),
                    parsed.resource(),
                    parsed.target(),
                    owner.type(),
                    owner.compatibility(),
                    owner.version(),
                    owner.definition(),
                    owner.updatedAtNs(),
                    owner.versions(),
                    true,
                    owner.key(),
                    null,
                    List.of());
            putEntry(copy.key(), copy);
            slotCache.put(copy.key(), new CachedSlot(copy, compiled, System.nanoTime() + cacheTtlNs));
        }
    }

    private void deleteKeyDirect(String regKey) {
        ObjectNode op = Transport.json().createObjectNode();
        op.put("kind", "delete_row");
        op.put("key_base64", Wire.b64(regKey.getBytes(StandardCharsets.UTF_8)));
        ObjectNode body = Transport.json().createObjectNode();
        body.put("store", STORE_NAME);
        body.put("mutation_id", UUID.randomUUID().toString());
        body.set("op", op);
        transport.post("/v1/state-stores/mutate", body);
        slotCache.remove(regKey);
    }

    /** List all schemas registered in {@code whitewater.schema.registry}. */
    public List<SchemaEntry> listSchemas() {
        List<SchemaEntry> entries = new ArrayList<>();
        String afterKeyB64 = null;
        while (true) {
            ObjectNode body = Transport.json().createObjectNode();
            body.put("store", STORE_NAME);
            body.put("limit", 256);
            if (afterKeyB64 != null) {
                body.put("after_key_base64", afterKeyB64);
            }
            JsonNode resp;
            try {
                resp = transport.post("/v1/state-stores/iterate", body);
            } catch (WhitewaterException.Refused | WhitewaterException.Unavailable e) {
                String msg = String.valueOf(e.getMessage()).toLowerCase(Locale.ROOT);
                if (msg.contains("does not exist") || msg.contains("not found") || msg.contains("no mutation-journal")) {
                    return List.of();
                }
                throw e;
            }
            JsonNode rows = resp.path("rows");
            if (!rows.isArray() || rows.isEmpty()) {
                break;
            }
            for (JsonNode row : rows) {
                try {
                    String key = new String(java.util.Base64.getDecoder().decode(row.path("key_base64").asText()), StandardCharsets.UTF_8);
                    byte[] payload = java.util.Base64.getDecoder().decode(row.path("payload_base64").asText());
                    JsonNode json = Transport.json().readTree(payload);
                    SchemaEntry entry = SchemaEntry.fromWire(key, json);
                    if (entry != null) {
                        entries.add(entry);
                    }
                } catch (Exception ignored) {
                }
            }
            JsonNode next = resp.path("next_after_key_base64");
            if (next.isNull() || next.isMissingNode() || next.asText().isBlank()) {
                break;
            }
            afterKeyB64 = next.asText();
        }
        entries.sort(Comparator.comparing(SchemaEntry::resource).thenComparing(e -> e.target().wire()));
        return entries;
    }

    private void putEntry(String regKey, SchemaEntry entry) {
        try {
            byte[] payloadBytes = Transport.json().writeValueAsBytes(entry.toWire());
            ObjectNode op = Transport.json().createObjectNode();
            op.put("kind", "put_row");
            op.put("key_base64", Wire.b64(regKey.getBytes(StandardCharsets.UTF_8)));
            op.put("payload_base64", Wire.b64(payloadBytes));
            ObjectNode body = Transport.json().createObjectNode();
            body.put("store", STORE_NAME);
            body.put("mutation_id", UUID.randomUUID().toString());
            body.set("op", op);
            transport.post("/v1/state-stores/mutate", body);
        } catch (WhitewaterException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to write schema to " + STORE_NAME, e);
        }
    }

    private synchronized void ensureStoreExists() {
        if (storeEnsured) return;
        createDomainIfMissing(STORE_DOMAIN);
        defineStoreIfMissing(STORE_NAME);
        storeEnsured = true;
    }

    private void createDomainIfMissing(String domain) {
        try {
            ObjectNode body = Transport.json().createObjectNode();
            body.put("request_id", UUID.randomUUID().toString());
            body.putArray("commands").addObject().put("command", "create_domain").put("name", domain);
            transport.post("/v1/admin/commands", body);
        } catch (WhitewaterException e) {
            if (!String.valueOf(e.getMessage()).toLowerCase(Locale.ROOT).contains("exist")) {
                throw e;
            }
        }
    }

    private void defineStoreIfMissing(String storeName) {
        try {
            ObjectNode body = Transport.json().createObjectNode();
            body.put("request_id", UUID.randomUUID().toString());
            ObjectNode cmd = body.putArray("commands").addObject();
            cmd.put("command", "define_state_store");
            cmd.put("name", storeName);
            cmd.putObject("source").put("kind", "manual");
            cmd.putArray("named_keys");
            transport.post("/v1/admin/commands", body);
        } catch (WhitewaterException e) {
            if (!String.valueOf(e.getMessage()).toLowerCase(Locale.ROOT).contains("exist")) {
                throw e;
            }
        }
    }
}
