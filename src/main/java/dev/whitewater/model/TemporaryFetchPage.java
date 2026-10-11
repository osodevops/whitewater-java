package dev.whitewater.model;

import java.util.List;

/**
 * One anonymous temporary Reader page: the committed records plus the
 * opaque continuation Cursor. {@code nextCursor} is the frontier after the
 * page — it may advance even on an empty page and may not equal the last
 * record Cursor. Pass it back opaquely; never parse or sort it.
 */
public record TemporaryFetchPage(List<FeedRecord> records, Cursor nextCursor) {}
