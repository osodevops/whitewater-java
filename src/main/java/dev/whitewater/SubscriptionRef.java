package dev.whitewater;

/**
 * A handle on one declared Subscription — the durable group-progress
 * declaration Pipes drive from. Obtained via
 * {@link Domain#subscription(String)}; the resolved absolute
 * {@link #name()} travels to other Domains (a Pipe in another Domain can
 * drive from this Subscription), while {@link #member()} opens a member
 * session on it.
 */
public final class SubscriptionRef implements ScopedRef {

    private final WhitewaterClient client;
    private final String name;

    SubscriptionRef(WhitewaterClient client, String name) {
        this.client = client;
        this.name = name;
    }

    /** The declared Subscription's absolute dotted name. */
    public String name() {
        return name;
    }

    /** A member-session client on this Subscription — join, claim
     * work, fetch, commit. */
    public SubscriptionMemberClient memberClient() {
        return client.subscriptionMember(name);
    }

    /** A session-bound member of this Subscription — joins internally
     * and owns the member identity, fencing epoch, and lease plumbing. */
    public Member member() {
        return client.member(name);
    }

    /** {@code DESCRIBE SUBSCRIPTION}: the catalog row. */
    public dev.whitewater.model.ControlExecution describe() {
        return client.describeSubscription(name);
    }

    /**
     * Reposition this Subscription's shared frontier — an operator
     * action, refused while any member work lease is active.
     */
    public dev.whitewater.model.SeekResult seek(
            String position,
            Long timestampNs,
            dev.whitewater.model.Cursor cursor) {
        return client.seekSubscription(name, position, timestampNs, cursor);
    }

    /** {@link #seek} under an explicit request identity — replays
     * idempotently. */
    public dev.whitewater.model.SeekResult seek(
            String position,
            Long timestampNs,
            dev.whitewater.model.Cursor cursor,
            java.util.UUID requestId) {
        return client.seekSubscription(name, position, timestampNs, cursor, requestId);
    }

    /** This Subscription's authoritative member state — epochs and
     * live leases. */
    public dev.whitewater.model.MemberState memberState() {
        return client.subscriptionMemberState(name);
    }

    /** This Subscription's committed-progress view (for operators). */
    public com.fasterxml.jackson.databind.JsonNode progress() {
        return client.subscriptionProgress(name);
    }

    @Override
    public String resolve(Domain domain) {
        return name;
    }
}
