package dev.whitewater.model;

import java.util.Map;
import java.util.UUID;

/**
 * The committed frontier a Subscription seek installed: the new opaque
 * Cursor plus the per-range positions it resolved to, under the shared
 * ownership epoch that fences it.
 */
public record SeekResult(
        UUID requestId,
        Cursor cursor,
        Map<String, String> positions,
        long ownershipEpoch) {}
