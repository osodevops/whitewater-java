package dev.whitewater.model;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * A typed record to append: request identity, key, payload, optional
 * event time and Metadata. The bound {@link Codec}s serialize
 * {@code key}/{@code value} at write time — application code never
 * touches bytes:
 *
 * <pre>{@code
 * writer.append(new FeedEvent<>("order-1", order));
 * writer.append(new FeedEvent<>(requestId, "order-2", order2)   // pinned identity
 *         .withEventTimeNs(when)
 *         .withTextMetadata(Map.of("tenant", "acme")));
 * }</pre>
 */
public record FeedEvent<K, V>(
        UUID requestId,
        K key,
        V value,
        Long eventTimeNs,
        Map<String, byte[]> metadata) {

    /** Auto-identified event with the current ingest clock as event time. */
    public FeedEvent(K key, V value) {
        this(null, key, value, null, null);
    }

    /** Explicit request identity for retryable writes. */
    public FeedEvent(UUID requestId, K key, V value) {
        this(requestId, key, value, null, null);
    }

    public static <K, V> FeedEvent<K, V> of(K key, V value) {
        return new FeedEvent<>(key, value);
    }

    public FeedEvent<K, V> withEventTimeNs(long eventTimeNs) {
        return new FeedEvent<>(requestId, key, value, eventTimeNs, metadata);
    }

    public FeedEvent<K, V> withRequestId(UUID requestId) {
        return new FeedEvent<>(requestId, key, value, eventTimeNs, metadata);
    }

    /** Metadata as arbitrary bytes. */
    public FeedEvent<K, V> withMetadata(Map<String, byte[]> metadata) {
        return new FeedEvent<>(requestId, key, value, eventTimeNs, metadata);
    }

    /** Metadata as UTF-8 text — the common case. */
    public FeedEvent<K, V> withTextMetadata(Map<String, String> metadata) {
        return new FeedEvent<>(requestId, key, value, eventTimeNs, textMetadata(metadata));
    }

    /** Encode a text Metadata map to wire bytes. */
    public static Map<String, byte[]> textMetadata(Map<String, String> metadata) {
        if (metadata == null) {
            return null;
        }
        Map<String, byte[]> encoded = new LinkedHashMap<>(metadata.size());
        metadata.forEach((name, value) ->
                encoded.put(name, value == null ? null : value.getBytes(StandardCharsets.UTF_8)));
        return Map.copyOf(encoded);
    }
}
