package dev.whitewater;

import com.fasterxml.jackson.databind.JsonNode;
import dev.whitewater.internal.Transport;
import dev.whitewater.internal.Wire;
import dev.whitewater.model.Cursor;
import dev.whitewater.model.FeedRecord;

import java.util.*;

/**
 * One cooperating member of a named Subscription. Member identity is a
 * UUID plus a fencing {@code memberEpoch}; work items are claimed under an
 * epoch-fenced lease. Fetch returns a bounded page plus the opaque
 * {@code cursor}/{@code positions} frontier tokens that {@link #ack}
 * commits back — acknowledgement is fenced at commit, so overlapping
 * delivery is possible and replays are expected semantics.
 */
public final class SubscriptionMemberClient {

    /** A granted work lease. */
    public record WorkLease(UUID workId, UUID memberId, long memberEpoch, long leaseEpoch, long expiresAtTick) {}

    /** Join result: the member's fencing epoch. */
    public record Joined(UUID memberId, long memberEpoch, long ownershipEpoch, long tick) {}

    /**
     * One fetched page plus the frontier tokens ack commits.
     * {@code deliveryAttempt} is the durable lease epoch the work was
     * delivered under; {@code nextDeliveryNs} is when the earliest
     * delivery-delayed head becomes eligible; {@code exhausted} reports
     * positions the claim passed over under the declared retry budget.
     */
    public record MemberPage(
            List<FeedRecord> records,
            Cursor cursor,
            Map<String, String> positions,
            long ownershipEpoch,
            long deliveryAttempt,
            Long nextDeliveryNs,
            JsonNode exhausted) {}

    private final Transport transport;
    private final String subscription;

    SubscriptionMemberClient(Transport transport, String subscription) {
        this.transport = transport;
        this.subscription = Objects.requireNonNull(subscription);
    }

    /** Join the Subscription; returns the fencing member epoch. */
    public Joined join(UUID memberId) {
        var body = Transport.json().createObjectNode();
        body.put("subscription", subscription);
        body.put("request_id", UUID.randomUUID().toString());
        body.put("member_id", memberId.toString());
        JsonNode response = transport.post("/v1/subscriptions/members/join", body);
        return new Joined(
                memberId,
                response.path("member_epoch").asLong(),
                response.path("ownership_epoch").asLong(),
                response.path("tick").asLong());
    }

    /** Claim a work item under a bounded lease. */
    public WorkLease claim(Joined member, UUID workId, Integer leaseTicks) {
        return lease("/v1/subscriptions/members/claim", member, workId, null, leaseTicks);
    }

    /** Renew an outstanding lease. */
    public WorkLease renew(Joined member, WorkLease lease, Integer leaseTicks) {
        return lease("/v1/subscriptions/members/renew", member, lease.workId(), lease.leaseEpoch(), leaseTicks);
    }

    private WorkLease lease(String path, Joined member, UUID workId, Long leaseEpoch, Integer leaseTicks) {
        var body = leaseBody(member, workId, leaseEpoch);
        if (leaseTicks != null) {
            body.put("lease_ticks", leaseTicks);
        }
        JsonNode response = transport.post(path, body);
        JsonNode lease = response.path("lease");
        if (lease.isMissingNode() || lease.isNull()) {
            throw new WhitewaterException.Conflict(409, "lease.refused",
                    "work claim was not granted");
        }
        return new WorkLease(
                UUID.fromString(lease.path("work_id").asText()),
                UUID.fromString(lease.path("member_id").asText()),
                lease.path("member_epoch").asLong(),
                lease.path("lease_epoch").asLong(),
                lease.path("expires_at_tick").asLong());
    }

    /** Release a held lease without acknowledging. */
    public void release(Joined member, WorkLease lease) {
        transport.post("/v1/subscriptions/members/release", leaseBody(member, lease.workId(), lease.leaseEpoch()));
    }

    /**
     * Transfer held work to another member of the same Subscription. The
     * current holder's {@code leaseEpoch} and the target's
     * {@code toMemberEpoch} fence the handoff — a stale transfer cannot
     * clobber a newer assignment.
     */
    public WorkLease transfer(Joined member, WorkLease lease,
                              UUID toMemberId, long toMemberEpoch, Integer leaseTicks) {
        var body = leaseBody(member, lease.workId(), lease.leaseEpoch());
        body.remove("member_epoch");
        body.put("to_member_id", toMemberId.toString());
        body.put("to_member_epoch", toMemberEpoch);
        if (leaseTicks != null) {
            body.put("lease_ticks", leaseTicks);
        }
        JsonNode response = transport.post("/v1/subscriptions/members/transfer", body);
        JsonNode node = response.path("lease");
        if (node.isMissingNode() || node.isNull()) {
            throw new WhitewaterException.Conflict(409, "lease.refused",
                    "work transfer was not granted");
        }
        return new WorkLease(
                UUID.fromString(node.path("work_id").asText()),
                UUID.fromString(node.path("member_id").asText()),
                node.path("member_epoch").asLong(),
                node.path("lease_epoch").asLong(),
                node.path("expires_at_tick").asLong());
    }

    /**
     * The Subscription's authoritative member state: shared ownership
     * epoch, per-member fencing epochs, and live work leases.
     */
    public dev.whitewater.model.MemberState state() {
        String path = "/v1/subscriptions/members/state?subscription="
                + java.net.URLEncoder.encode(subscription, java.nio.charset.StandardCharsets.UTF_8);
        return Wire.memberState(transport.get(path));
    }

    /** Fetch a bounded page after the shared frontier. */
    public MemberPage fetch(Joined member, WorkLease lease, Integer limit) {
        var body = leaseBody(member, lease.workId(), lease.leaseEpoch());
        if (limit != null) {
            body.put("limit", limit);
        }
        JsonNode response = transport.post("/v1/subscriptions/members/fetch", body);
        Map<String, String> positions = new LinkedHashMap<>();
        response.path("positions").fields().forEachRemaining(entry ->
                positions.put(entry.getKey(), entry.getValue().asText()));
        JsonNode nextDelivery = response.path("next_delivery_ns");
        JsonNode exhausted = response.path("exhausted");
        return new MemberPage(
                Wire.records(response.path("records")),
                Cursor.of(response.path("cursor").isNull() ? null : response.path("cursor").asText()),
                Map.copyOf(positions),
                response.path("ownership_epoch").asLong(),
                response.path("delivery_attempt").asLong(),
                nextDelivery.isNull() || nextDelivery.isMissingNode()
                        ? null : nextDelivery.asLong(),
                exhausted.isNull() || exhausted.isMissingNode() ? null : exhausted);
    }

    /** Commit the fetched frontier; overlapping members are fenced here. */
    public void ack(Joined member, WorkLease lease, MemberPage page) {
        var body = leaseBody(member, lease.workId(), lease.leaseEpoch());
        if (page.cursor() != null) {
            body.put("cursor", page.cursor().value());
        }
        var positions = body.putObject("positions");
        page.positions().forEach(positions::put);
        transport.post("/v1/subscriptions/members/ack", body);
    }

    private com.fasterxml.jackson.databind.node.ObjectNode leaseBody(Joined member, UUID workId, Long leaseEpoch) {
        var body = Transport.json().createObjectNode();
        body.put("subscription", subscription);
        body.put("request_id", UUID.randomUUID().toString());
        body.put("member_id", member.memberId().toString());
        body.put("member_epoch", member.memberEpoch());
        body.put("work_id", workId.toString());
        if (leaseEpoch != null) {
            body.put("lease_epoch", leaseEpoch);
        }
        return body;
    }
}
