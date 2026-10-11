package dev.whitewater.model;

import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Objects;

/**
 * An opaque stream position. Cursors are never parsed, decomposed, or
 * compared for ordering — they are passed back to the Riverbed verbatim.
 * A Cursor may represent a record position or an opaque per-range delivery
 * frontier; only the Riverbed interprets it.
 */
public record Cursor(String value) {

    public Cursor {
        Objects.requireNonNull(value, "cursor value");
        if (value.isEmpty()) {
            throw new IllegalArgumentException("cursor value is empty");
        }
    }

    public static Cursor of(String value) {
        return value == null ? null : new Cursor(value);
    }

    @JsonValue
    public String value() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }
}
