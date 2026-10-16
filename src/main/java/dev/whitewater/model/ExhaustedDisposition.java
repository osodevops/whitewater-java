package dev.whitewater.model;

/**
 * What {@code member.fetch} does to positions an over-budget claim would
 * deliver, applied once per claim epoch.
 */
public enum ExhaustedDisposition {
    /** Park the positions in the durable quarantine set (default). */
    QUARANTINE,
    /** Permanently pass them over; releasable only via an explicit Subscription seek. */
    SKIP;

    /** The wire value. */
    public String wire() {
        return name().toLowerCase();
    }
}
