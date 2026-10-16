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
        Map<String, byte[]> metadata) {

    /** The key decoded as UTF-8 — convenience when keys are text. */
    public String keyText() {
        return new String(key, java.nio.charset.StandardCharsets.UTF_8);
    }

    /** The payload decoded as UTF-8 — convenience when payloads are text/JSON. */
    public String payloadText() {
        return new String(payload, java.nio.charset.StandardCharsets.UTF_8);
    }

    /** The payload Jackson-decoded into {@code type}. */
    public <T> T payloadAs(Class<T> type) {
        return dev.whitewater.model.Codecs.json(type).decode(payload);
    }

    /** A Metadata entry decoded as UTF-8, or null when absent. */
    public String metadataText(String name) {
        byte[] value = metadata == null ? null : metadata.get(name);
        return value == null ? null : new String(value, java.nio.charset.StandardCharsets.UTF_8);
    }
}
