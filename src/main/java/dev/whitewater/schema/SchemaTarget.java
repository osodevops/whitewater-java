package dev.whitewater.schema;

import dev.whitewater.admin.Commands;
import java.util.Locale;
import java.util.Objects;

/**
 * Which side of a Feed record or StateStore entry a schema applies to:
 * {@link #KEY} or {@link #MESSAGE}.
 *
 * <p>In the {@code schema.registry} StateStore, each association is stored
 * under {@code <resource>|<target>} (for example,
 * {@code orders.order.lines|message} or {@code orders.order.lines|key}).
 */
public enum SchemaTarget {
    KEY("key"),
    MESSAGE("message");

    /** PascalCase alias for {@link #KEY}. */
    public static final SchemaTarget Key = KEY;
    /** PascalCase alias for {@link #MESSAGE}. */
    public static final SchemaTarget Message = MESSAGE;

    private final String wire;

    SchemaTarget(String wire) {
        this.wire = wire;
    }

    public String wire() {
        return wire;
    }

    /** Compose the {@code schema.registry} primary key: {@code <resource>|<target>}. */
    public String registryKey(String resource) {
        String normalized = normalizeResource(resource);
        return normalized + "|" + wire;
    }

    public static SchemaTarget fromWire(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("schema target is required (key or message)");
        }
        return switch (raw.trim().toLowerCase(Locale.ROOT)) {
            case "key", "k" -> KEY;
            case "message", "value", "val", "msg", "m", "v" -> MESSAGE;
            default -> throw new IllegalArgumentException("unknown schema target: " + raw);
        };
    }

    public record ParsedKey(String resource, SchemaTarget target) {
        public String registryKey() {
            return target.registryKey(resource);
        }
    }

    /**
     * Parse a {@code <resource>|key} or {@code <resource>|message} key
     * (also accepting {@code .key}/{@code .message} or {@code -key}/{@code -value} suffixes).
     */
    public static ParsedKey parseKey(String rawKey) {
        Objects.requireNonNull(rawKey, "registry key is required");
        String trimmed = rawKey.trim();
        int pipe = trimmed.lastIndexOf('|');
        if (pipe > 0 && pipe < trimmed.length() - 1) {
            String res = normalizeResource(trimmed.substring(0, pipe));
            SchemaTarget target = fromWire(trimmed.substring(pipe + 1));
            return new ParsedKey(res, target);
        }
        String lower = trimmed.toLowerCase(Locale.ROOT);
        if (lower.endsWith(".message") && trimmed.length() > 8) {
            return new ParsedKey(normalizeResource(trimmed.substring(0, trimmed.length() - 8)), MESSAGE);
        }
        if (lower.endsWith(".value") && trimmed.length() > 6) {
            return new ParsedKey(normalizeResource(trimmed.substring(0, trimmed.length() - 6)), MESSAGE);
        }
        if (lower.endsWith(".key") && trimmed.length() > 4) {
            return new ParsedKey(normalizeResource(trimmed.substring(0, trimmed.length() - 4)), KEY);
        }
        if (lower.endsWith("-value") && trimmed.length() > 6) {
            return new ParsedKey(normalizeResource(trimmed.substring(0, trimmed.length() - 6)), MESSAGE);
        }
        if (lower.endsWith("-key") && trimmed.length() > 4) {
            return new ParsedKey(normalizeResource(trimmed.substring(0, trimmed.length() - 4)), KEY);
        }
        return new ParsedKey(normalizeResource(trimmed), MESSAGE);
    }

    static String normalizeResource(String resource) {
        Objects.requireNonNull(resource, "resource is required");
        String trimmed = resource.trim();
        int pipe = trimmed.indexOf('|');
        if (pipe > 0) {
            trimmed = trimmed.substring(0, pipe).trim();
        }
        return Commands.dotted(trimmed);
    }
}
