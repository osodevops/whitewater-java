package dev.whitewater.model;

import java.util.List;

/**
 * One bounded iteration page in primary-key order. {@code pinnedSeq} is
 * the serving replica's applied journal position the page was pinned to —
 * the iteration's freshness evidence. {@code nextKey} continues the scan.
 */
public record StorePage(List<StoreRow> rows, byte[] nextKey, long pinnedSeq, String servedBy) {
}
