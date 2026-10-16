package dev.whitewater.model;

/** What an {@code enrich} Pipe lookup miss does to the output record. */
public enum EnrichMiss {
    /** Forward the record with {@code enrich.matched = false} (default). */
    FORWARD,
    /** Consume without emitting; the input frontier still commits. */
    DROP,
    /** Route the miss to the declared quarantine Feed. */
    QUARANTINE;

    /** The wire value. */
    public String wire() {
        return name().toLowerCase();
    }
}
