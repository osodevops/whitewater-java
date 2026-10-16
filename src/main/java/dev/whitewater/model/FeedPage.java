package dev.whitewater.model;

import java.util.List;

/**
 * A decoded temporary-fetch page: {@link FeedMessage}s plus the opaque
 * continuation frontier. No server-side progress is retained — resume
 * by passing {@link #nextCursor} back as
 * {@link TemporaryFetchOptions#after()}.
 */
public record FeedPage<K, V>(List<FeedMessage<K, V>> records, Cursor nextCursor) {}
