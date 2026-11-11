package dev.whitewater.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Base class for typed {@link dev.whitewater.StateStore} secondary-key
 * sets. Subclasses declare small fluent setters delegating to
 * {@link #put(String, Object)} — which generic-self-returns the
 * concrete subclass type without casts:
 *
 * <pre>{@code
 * public final class PassportKeySet extends AbstractStoreKeySet {
 *     public PassportKeySet setPassport(String v)   { return put("passport", v); }
 *     public PassportKeySet setDepartment(String v) { return put("department", v); }
 *     public PassportKeySet passport(String v)      { return setPassport(v); }
 *     public PassportKeySet department(String v)    { return setDepartment(v); }
 *
 *     public static PassportKeySet with(String passport, String department) {
 *         return new PassportKeySet().setPassport(passport).setDepartment(department);
 *     }
 *     public static PassportKeySet withPassport(String passport) {
 *         return new PassportKeySet().setPassport(passport);
 *     }
 *     public static PassportKeySet of(Consumer<PassportKeySet> builder) {
 *         var ks = new PassportKeySet();
 *         builder.accept(ks);
 *         return ks;
 *     }
 * }
 * }</pre>
 */
public abstract class AbstractStoreKeySet implements StoreKeySet {

    private final Map<String, Object> keys = new LinkedHashMap<>();

    protected AbstractStoreKeySet() {}

    /**
     * Record a secondary-key value and return {@code this} cast to the
     * concrete subclass {@code S} for fluent chaining.
     */
    @SuppressWarnings("unchecked")
    public <S extends AbstractStoreKeySet> S put(String name, Object value) {
        Objects.requireNonNull(name, "name");
        if (name.isBlank()) {
            throw new IllegalArgumentException("secondary key name must not be blank");
        }
        keys.put(name, value);
        return (S) this;
    }

    /** Read back a previously set secondary-key value, or {@code null}. */
    public Object get(String name) {
        return keys.get(name);
    }

    /** True when no secondary keys have been populated. */
    public boolean isEmpty() {
        return keys.isEmpty();
    }

    @Override
    public Map<String, Object> toMap() {
        return Collections.unmodifiableMap(keys);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AbstractStoreKeySet that)) return false;
        return keys.equals(that.keys);
    }

    @Override
    public int hashCode() {
        return keys.hashCode();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + keys;
    }
}
