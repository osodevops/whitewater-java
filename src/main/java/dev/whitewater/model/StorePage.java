package dev.whitewater.model;

import java.util.List;

/**
 * One bounded iteration page in primary-key order — the same row set
 * replica transfer serves, pinned to {@link #pinnedSeq}. {@code nextKey}
 * (already codec-decoded) is the continuation: pass it back into
 * {@code page(nextKey, limit)}; null ends the walk.
 */
public record StorePage<K, V>(List<StoreRow<K, V>> rows, K nextKey, long pinnedSeq, String servedBy) {
}
