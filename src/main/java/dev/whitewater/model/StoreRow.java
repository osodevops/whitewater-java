package dev.whitewater.model;

/**
 * One StateStore row: its primary key and payload bytes plus the serving
 * replica's freshness evidence when the read carried it.
 */
public record StoreRow(byte[] key, byte[] payload, String appliedCursor, StoreFreshness freshness) {

    public StoreRow(byte[] key, byte[] payload) {
        this(key, payload, null, null);
    }
}
