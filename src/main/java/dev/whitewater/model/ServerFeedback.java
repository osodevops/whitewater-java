package dev.whitewater.model;

/**
 * Server-side batching guidance returned with every append. When
 * {@code hotKey} is true the appended Key holds sustained dominant share:
 * spread or shed load for that Key rather than scaling batch sizes — a
 * strictly ordered hot Key is isolated, never parallelized.
 */
public record ServerFeedback(
        int recommendedBatchCount,
        int recommendedBatchBytes,
        double pressure,
        long retryAfterMs,
        long maxFrameBytes,
        boolean hotKey) {}
