package dev.whitewater.model;

import java.util.UUID;

/**
 * Result of one append. {@code deduplicated} is true when the Riverbed
 * replayed a previously committed response for the same request identity —
 * the record was not written twice.
 */
public record AppendResult(
        UUID messageId,
        Cursor cursor,
        boolean deduplicated,
        String durability,
        ServerFeedback feedback) {}
