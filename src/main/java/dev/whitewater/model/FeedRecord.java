package dev.whitewater.model;

import java.util.Map;
import java.util.UUID;

/**
 * One committed Feed record. Times are signed 64-bit Unix epoch
 * nanoseconds; {@code key}, {@code payload}, and Metadata values are raw
 * bytes. {@code cursor} is this record's opaque position — pass it back
 * verbatim, never parse it.
 */
public record FeedRecord(
        Cursor cursor,
        UUID messageId,
        UUID producerId,
        long sequence,
        long eventTimeNs,
        long ingestTimeNs,
        byte[] key,
        byte[] payload,
        Map<String, byte[]> metadata) {}
