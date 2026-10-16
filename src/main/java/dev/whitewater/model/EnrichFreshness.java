package dev.whitewater.model;

/** How fresh an {@code enrich} Pipe lookup row must be before it is journaled. */
public enum EnrichFreshness {
    /** Serve the replica's applied state (default; resolution is journaled either way). */
    LATEST,
    /** Require applied to cover committed at read entry; lag yields a retryable drive failure. */
    STRICT;

    /** The wire value. */
    public String wire() {
        return name().toLowerCase();
    }
}
