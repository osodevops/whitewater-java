package dev.whitewater;

import java.util.Objects;

/**
 * An explicit {@link ScopedRef}: {@link #relative(String)} resolves
 * inside the Domain the call is scoped to, while
 * {@link #absolute(String)} keeps the name verbatim — the handle-free
 * way to point a Pipe endpoint across Domains.
 */
public final class Scoped implements ScopedRef {

    private final String name;
    private final boolean absolute;

    private Scoped(String name, boolean absolute) {
        this.name = name;
        this.absolute = absolute;
    }

    /**
     * An explicit cross-Domain reference —
     * {@code Scoped.absolute("warehouse.inbound")} stays
     * {@code warehouse.inbound} wherever it is used.
     */
    public static Scoped absolute(String name) {
        return new Scoped(WhitewaterClient.dottedName(name), true);
    }

    /**
     * A name resolved inside the Domain the call is scoped to —
     * {@code Scoped.relative("orders.copy")} inside {@code pipes}
     * means {@code pipes.orders.copy}.
     */
    public static Scoped relative(String name) {
        return new Scoped(
                Objects.requireNonNull(name, "name is required"), false);
    }

    /** The raw name as declared — relative or absolute per the factory. */
    public String name() {
        return name;
    }

    /** True when {@link #absolute(String)} built this reference. */
    public boolean isAbsolute() {
        return absolute;
    }

    @Override
    public String resolve(Domain domain) {
        return absolute ? name : domain.resolve(name);
    }
}
