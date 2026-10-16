package dev.whitewater.model;

import java.util.List;
import java.util.UUID;

/**
 * One {@code GET INDEX} page: the matching rows plus freshness evidence —
 * the generation's applied frontier versus the catalog's committed
 * target, so callers see when the serving Node lags.
 */
public record IndexPage(
        String index,
        UUID indexId,
        long generation,
        List<Row> rows,
        Cursor appliedCursor,
        Cursor targetCursor,
        boolean behind,
        boolean truncated,
        String servedBy) {

    /** One indexed row: the Feed record's key and payload. */
    public record Row(byte[] key, byte[] payload) {}
}
