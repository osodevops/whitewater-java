package dev.whitewater.model;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Strongly-typed or inline secondary-key postings for a
 * {@link dev.whitewater.StateStore} {@code put}. Subclass
 * {@link AbstractStoreKeySet} to give a store's secondary keys typed
 * fluent setters, static {@code with(...)} factories, and
 * {@code of(k -> ...)} lambda builders, or use the static helpers on
 * this interface directly.
 */
public interface StoreKeySet {

    /**
     * Named-key map emitted into the mutation request ({@code name ->
     * value} where values are {@link String}, whole {@link Number}s,
     * {@link Boolean}, {@code byte[]}, or {@link java.util.List}).
     */
    Map<String, Object> toMap();

    /** Alias for {@link #toMap()}. */
    default Map<String, Object> asMap() {
        return toMap();
    }

    /** Single named secondary key. */
    static StoreKeySet of(String key, Object value) {
        return new Generic().put(key, value);
    }

    /** Two named secondary keys. */
    static StoreKeySet of(String k1, Object v1, String k2, Object v2) {
        return new Generic().put(k1, v1).put(k2, v2);
    }

    /** Three named secondary keys. */
    static StoreKeySet of(String k1, Object v1, String k2, Object v2, String k3, Object v3) {
        return new Generic().put(k1, v1).put(k2, v2).put(k3, v3);
    }

    /** Wrap an existing named-key map. */
    static StoreKeySet of(Map<String, ?> entries) {
        Objects.requireNonNull(entries, "entries");
        var ks = new Generic();
        entries.forEach(ks::put);
        return ks;
    }

    /** Build an ad-hoc key set with a fluent consumer block. */
    static StoreKeySet of(Consumer<AbstractStoreKeySet> builder) {
        Objects.requireNonNull(builder, "builder");
        var ks = new Generic();
        builder.accept(ks);
        return ks;
    }

    /** Concrete general-purpose {@link AbstractStoreKeySet}. */
    final class Generic extends AbstractStoreKeySet {
        public Generic() {}
    }
}
