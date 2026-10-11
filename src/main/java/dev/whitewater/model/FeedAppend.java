package dev.whitewater.model;

import java.util.Map;
import java.util.UUID;

/**
 * One ad-hoc append for {@code POST /v1/feeds/append}: the caller supplies
 * the writer identity ({@code writerSessionId} + {@code writerEpoch} +
 * {@code sequence}) directly instead of holding a named Writer session.
 * Intended for interactive clients sending individual records; identical
 * {@code requestId} retries dedupe server-side.
 */
public record FeedAppend(
        UUID requestId,
        UUID writerSessionId,
        long writerEpoch,
        long sequence,
        byte[] key,
        byte[] payload,
        Long eventTimeNs,
        Map<String, byte[]> metadata) {

    public FeedAppend(UUID requestId, UUID writerSessionId, byte[] key, byte[] payload) {
        this(requestId, writerSessionId, 1L, 1L, key, payload, null, Map.of());
    }
}
