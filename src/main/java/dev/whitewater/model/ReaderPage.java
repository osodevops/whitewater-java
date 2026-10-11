package dev.whitewater.model;

import java.util.List;

/**
 * One bounded page of committed records. Acknowledge
 * {@link #deliveredCursor} — not the last record's cursor — to durably
 * commit progress; a multi-range Feed's delivered cursor is an opaque
 * frontier distinct from any record position.
 */
public record ReaderPage(
        List<FeedRecord> records,
        Cursor deliveredCursor,
        Cursor acknowledgedCursor,
        int recommendedCapacity,
        long retryAfterMs) {}
