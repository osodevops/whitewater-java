package dev.whitewater.model;

import java.util.ArrayList;
import java.util.List;

/**
 * A StateStore declaration: name plus named secondary keys, built
 * fluently and handed to {@code client.stateStores().define(spec, source)}:
 *
 * <pre>{@code
 * var spec = StateStoreSpec.named("accounts.employees")
 *         .addKey("passport", true)                    // unique
 *         .addKey("department");                       // non-unique
 * client.stateStores().define(spec, null);              // manual store
 * }</pre>
 *
 * <p>Named keys are declared at creation and cannot change. On a
 * feed-sourced store each key must declare an {@link Extractor} —
 * how replay projects each committed source record to the key's typed
 * values; extractors on a manual store are refused (puts carry the
 * values directly).
 */
public final class StateStoreSpec {

    /** Named secondary keys per store; matches the server declaration budget. */
    public static final int MAX_NAMED_KEYS = NamedKey.MAX_NAMED_KEYS;

    private final String name;
    private final List<NamedKey> declaredKeys = new ArrayList<>();

    private StateStoreSpec(String name) {
        this.name = name;
    }

    /** Begin a declaration for the dotted store name. */
    public static StateStoreSpec named(String name) {
        return new StateStoreSpec(dev.whitewater.admin.Commands.dotted(name));
    }

    public String name() {
        return name;
    }

    /**
     * Declare a named secondary key (at most {@value #MAX_NAMED_KEYS}).
     * {@code extractor} declares how replay projects each source-Feed
     * record to this key's values — required on feed-sourced stores,
     * refused on manual ones.
     */
    public StateStoreSpec addKey(String namedKey, boolean unique, Extractor extractor) {
        if (namedKey == null || namedKey.isBlank()) {
            throw new IllegalArgumentException("named key is required");
        }
        if (declaredKeys.stream().anyMatch(k -> k.name().equals(namedKey))) {
            throw new IllegalArgumentException("duplicate named key: " + namedKey);
        }
        if (declaredKeys.size() >= MAX_NAMED_KEYS) {
            throw new IllegalStateException(
                    "a StateStore declares at most " + MAX_NAMED_KEYS + " named keys");
        }
        declaredKeys.add(new NamedKey(namedKey, unique, extractor));
        return this;
    }

    public StateStoreSpec addKey(String namedKey, boolean unique) {
        return addKey(namedKey, unique, null);
    }

    public StateStoreSpec addKey(String namedKey) {
        return addKey(namedKey, false);
    }

    public List<NamedKey> namedKeys() {
        return List.copyOf(declaredKeys);
    }
}
