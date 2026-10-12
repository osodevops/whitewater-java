package dev.whitewater.model;

import java.util.UUID;

/**
 * A committed StateStore mutation: its journal sequence and whether the
 * {@code mutationId} idempotency identity deduplicated a retry.
 */
public record MutationResult(UUID storeId, UUID mutationId, long sequence, boolean deduplicated) {
}
