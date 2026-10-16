package dev.whitewater.model;

/** How an Index collates text values; {@code BINARY} is the only collation today. */
public enum IndexCollation {
    BINARY;

    /** The wire value. */
    public String wire() {
        return name().toLowerCase();
    }
}
