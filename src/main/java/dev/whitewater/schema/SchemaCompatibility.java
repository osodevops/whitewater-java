package dev.whitewater.schema;

import java.util.Locale;

/**
 * Schema evolution compatibility modes enforced when updating an existing
 * {@code <resource>|<target>} entry in {@code schema.registry}.
 */
public enum SchemaCompatibility {
    NONE,
    BACKWARD,
    BACKWARD_TRANSITIVE,
    FORWARD,
    FORWARD_TRANSITIVE,
    FULL,
    FULL_TRANSITIVE;

    public String wire() {
        return name();
    }

    public boolean isTransitive() {
        return this == BACKWARD_TRANSITIVE || this == FORWARD_TRANSITIVE || this == FULL_TRANSITIVE;
    }

    public boolean checksBackward() {
        return this == BACKWARD || this == BACKWARD_TRANSITIVE || this == FULL || this == FULL_TRANSITIVE;
    }

    public boolean checksForward() {
        return this == FORWARD || this == FORWARD_TRANSITIVE || this == FULL || this == FULL_TRANSITIVE;
    }

    public static SchemaCompatibility fromWire(String raw) {
        if (raw == null || raw.isBlank()) {
            return BACKWARD;
        }
        return switch (raw.trim().toUpperCase(Locale.ROOT)) {
            case "NONE" -> NONE;
            case "BACKWARD" -> BACKWARD;
            case "BACKWARD_TRANSITIVE" -> BACKWARD_TRANSITIVE;
            case "FORWARD" -> FORWARD;
            case "FORWARD_TRANSITIVE" -> FORWARD_TRANSITIVE;
            case "FULL" -> FULL;
            case "FULL_TRANSITIVE" -> FULL_TRANSITIVE;
            default -> throw new IllegalArgumentException("unknown schema compatibility: " + raw);
        };
    }
}
