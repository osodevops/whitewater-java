package dev.whitewater.model;

/** Which record clock a {@code window} Pipe buckets into panes. */
public enum WindowTime {
    /** {@code event_time_ns} — the application-stamped occurrence clock (default). */
    EVENT,
    /** {@code ingest_time_ns} — the durable-arrival clock. */
    INGEST;

    /** The wire value. */
    public String wire() {
        return name().toLowerCase();
    }
}
