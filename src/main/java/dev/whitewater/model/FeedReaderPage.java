package dev.whitewater.model;

import java.util.List;

/**
 * One bounded page of committed records, decoded through the bound
 * Codecs. Acknowledge {@link #deliveredCursor} — not the last record's
 * cursor — to durably commit progress; a multi-range Feed's delivered
 * cursor is an opaque frontier distinct from any record position.
 */
public record FeedReaderPage<K, V>(
        List<FeedMessage<K, V>> records,
        Cursor deliveredCursor,
        Cursor acknowledgedCursor,
        int recommendedCapacity,
        long retryAfterMs) {}
