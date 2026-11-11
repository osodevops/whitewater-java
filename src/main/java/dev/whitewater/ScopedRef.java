package dev.whitewater;

/**
 * A resource reference inside a {@link Domain}-scoped call — mostly
 * Pipe definitions, whose declared name lives in the defining Domain
 * while the Subscription and output Feed/StateStore may point anywhere.
 *
 * <pre>{@code
 * // everything inside one Domain — plain strings compose in-domain:
 * pipes.defineForwardPipe("relay", "audit", "orders.copy");
 *
 * // cross-Domain: a handle or an explicit absolute name carries the
 * // full dotted name — never silently re-prefixed:
 * warehouse.defineForwardPipe("pull",
 *     purchasing.subscription("fulfil"), warehouse.feed("inbound"));
 * warehouse.defineForwardPipe("pull2",
 *     Scoped.absolute("purchasing.fulfil"), Scoped.relative("inbound"));
 * }</pre>
 *
 * <p>{@link Scoped} is the explicit relative/absolute form;
 * {@link Feed}, {@link StateStore}, and {@link SubscriptionRef} handles
 * implement this interface and carry the fully-qualified name they were
 * bound under.
 */
public sealed interface ScopedRef permits Scoped, Feed, StateStore, StateStore.Keyed, SubscriptionRef {

    /** Resolve this reference under {@code domain} — an absolute
     * reference returns its bound name verbatim; a relative
     * {@link Scoped} composes inside the Domain. */
    String resolve(Domain domain);
}
