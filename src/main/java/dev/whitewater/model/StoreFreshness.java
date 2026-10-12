package dev.whitewater.model;

/**
 * Freshness evidence every StateStore read reports: the serving replica's
 * applied and committed journal positions, whether it answers behind its
 * frontier, and which Node served.
 */
public record StoreFreshness(long appliedSeq, long committedSeq, boolean behind, String servedBy) {
}
