package dev.whitewater.model;

/**
 * The read consistency a declared Index intends to honour once serving.
 * {@code STRICT} is declaration-only today — strict queries stay
 * unavailable until the replicated commit/read protocol lands.
 */
public enum IndexConsistency {
    EVENTUAL,
    STRICT;

    /** The wire value. */
    public String wire() {
        return name().toLowerCase();
    }
}
