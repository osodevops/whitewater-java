package dev.whitewater.model;

/**
 * One StateStore row. Key and payload arrive decoded through the bound
 * {@link Codec}s — a {@code StateStore<String, User>} yields
 * {@code StoreRow<String, User>} with {@code key()} a String and
 * {@code payload()} a {@code User}; the wire bytes never surface.
 * {@code appliedCursor} identifies the journal position the row
 * reflects, and {@code freshness} reports the serving replica's
 * applied/committed positions and whether it answered behind.
 */
public record StoreRow<K, V>(K key, V payload, String appliedCursor, StoreFreshness freshness) {

    public StoreRow(K key, V payload) {
        this(key, payload, null, null);
    }
}
