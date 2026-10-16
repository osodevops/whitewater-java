package dev.whitewater.model;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

/**
 * One committed Feed record with its key and payload already decoded
 * through the bound {@link Codec}s — the {@link FeedRecord} fields are
 * identical; only the payloads are typed. Times are signed 64-bit Unix
 * epoch nanoseconds; {@code cursor} is opaque — pass it back verbatim.
 *
 * <pre>{@code
 * FeedMessage<String, Order> m = page.records().get(0);
 * Order order = m.value();              // decoded — never getBytes
 * String tenant = m.metadataText("tenant");
 * }</pre>
 */
public record FeedMessage<K, V>(
        Cursor cursor,
        UUID messageId,
        UUID producerId,
        long sequence,
        long eventTimeNs,
        long ingestTimeNs,
        K key,
        V value,
        Map<String, byte[]> metadata) {

    /** Decode a wire record through the given Codecs. */
    public static <K, V> FeedMessage<K, V> decode(
            FeedRecord record, Codec<K> keyCodec, Codec<V> valueCodec) {
        return new FeedMessage<>(
                record.cursor(),
                record.messageId(),
                record.producerId(),
                record.sequence(),
                record.eventTimeNs(),
                record.ingestTimeNs(),
                record.key() == null ? null : keyCodec.decode(record.key()),
                record.payload() == null ? null : valueCodec.decode(record.payload()),
                record.metadata());
    }

    /** A Metadata entry decoded as UTF-8, or null when absent. */
    public String metadataText(String name) {
        byte[] value = metadata == null ? null : metadata.get(name);
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }
}
