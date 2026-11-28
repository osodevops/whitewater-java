package dev.whitewater.schema;

import java.util.List;

/**
 * Outcome of checking a candidate schema definition against existing
 * version(s) of a {@code <resource>|<target>} schema entry.
 */
public record SchemaCompatibilityResult(
        boolean compatible,
        List<String> messages) {

    public SchemaCompatibilityResult {
        messages = messages == null ? List.of() : List.copyOf(messages);
    }

    public static SchemaCompatibilityResult ok() {
        return new SchemaCompatibilityResult(true, List.of());
    }

    public static SchemaCompatibilityResult incompatible(List<String> messages) {
        return new SchemaCompatibilityResult(false, messages);
    }
}
