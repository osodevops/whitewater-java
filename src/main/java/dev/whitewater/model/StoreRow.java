package dev.whitewater.model;

import java.time.Duration;
import java.util.Objects;

/**
 * One StateStore row. Key and payload arrive decoded through the bound
 * {@link Codec}s — a {@code StateStore<String, User>} yields
 * {@code StoreRow<String, User>} with {@code key()} a String and
 * {@code payload()} a {@code User}; the wire bytes never surface.
 * {@code appliedCursor} identifies the journal position the row
 * reflects, and {@code freshness} reports the serving replica's
 * applied/committed positions and whether it answered behind.
 *
 * <p>Rows produced by a {@code StateStore} are bound back to it:
 * {@link #refresh()} re-reads the primary key — a fast read that may
 * answer behind, exactly like {@code store.get(key)} — and
 * {@link #refreshStrict(Duration)} bounded-waits for the serving
 * replica's committed frontier, like {@code store.getStrict}. Both
 * return {@code null} when the row no longer exists.
 */
public final class StoreRow<K, V> {

    /**
     * Re-read contract binding a row to its store. Internal plumbing —
     * the facade injects it; detached rows built by hand carry none.
     */
    @FunctionalInterface
    public interface Reloader<K, V> {
        StoreRow<K, V> load(K key, boolean strict, Long waitMs);
    }

    private final K key;
    private final V payload;
    private final String appliedCursor;
    private final StoreFreshness freshness;
    private final Reloader<K, V> reloader;

    public StoreRow(K key, V payload) {
        this(key, payload, null, null, null);
    }

    public StoreRow(K key, V payload, String appliedCursor, StoreFreshness freshness) {
        this(key, payload, appliedCursor, freshness, null);
    }

    /** A row bound back to its store — gains {@link #refresh()}. */
    public StoreRow(
            K key, V payload, String appliedCursor, StoreFreshness freshness,
            Reloader<K, V> reloader) {
        this.key = key;
        this.payload = payload;
        this.appliedCursor = appliedCursor;
        this.freshness = freshness;
        this.reloader = reloader;
    }

    public K key() {
        return key;
    }

    public V payload() {
        return payload;
    }

    /** The journal position this row reflects, when reported. */
    public String appliedCursor() {
        return appliedCursor;
    }

    /** Serving-replica evidence: applied/committed seq, behind, servedBy. */
    public StoreFreshness freshness() {
        return freshness;
    }

    /**
     * Re-read this row's primary key — the same fast read as
     * {@code store.get(key)}: served immediately, honest about staleness
     * via {@link #freshness()}. {@code null} when the row was deleted
     * meanwhile. Throws when the row is detached (not store-produced).
     */
    public StoreRow<K, V> refresh() {
        return reload(false, null);
    }

    /** {@link #refresh()} bounded-waiting for the committed frontier. */
    public StoreRow<K, V> refreshStrict() {
        return reload(true, null);
    }

    /** {@link #refreshStrict()} with an explicit wait bound. */
    public StoreRow<K, V> refreshStrict(Duration wait) {
        return reload(true, Objects.requireNonNull(wait, "wait").toMillis());
    }

    private StoreRow<K, V> reload(boolean strict, Long waitMs) {
        if (reloader == null) {
            throw new IllegalStateException(
                    "row is detached — only rows returned by a StateStore can refresh; "
                            + "re-read by key with store.get(key)");
        }
        return reloader.load(key, strict, waitMs);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StoreRow<?, ?> row)) {
            return false;
        }
        return Objects.equals(key, row.key)
                && Objects.equals(payload, row.payload)
                && Objects.equals(appliedCursor, row.appliedCursor)
                && Objects.equals(freshness, row.freshness);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, payload, appliedCursor, freshness);
    }

    @Override
    public String toString() {
        return "StoreRow[key=" + key + ", payload=" + payload
                + ", appliedCursor=" + appliedCursor + ", freshness=" + freshness + "]";
    }
}
