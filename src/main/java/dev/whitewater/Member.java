package dev.whitewater;

import com.fasterxml.jackson.databind.JsonNode;
import dev.whitewater.internal.Transport;
import dev.whitewater.internal.Wire;
import dev.whitewater.model.Cursor;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * A session-bound Subscription member: join, epoch, and lease plumbing
 * stay inside the handle so a worker loop reads as
 * {@code claim → fetch → ack → release}.
 *
 * <pre>{@code
 * var member = client.member("orders.audit");          // joins internally
 * var work = member.claim();                           // epoch-fenced lease
 * var page = member.fetch(work, 64);
 * handle(page.records());
 * member.ack(work, page);                              // commits the frontier
 * member.release(work);
 * }</pre>
 *
 * <p>Acknowledgement is fenced at commit — overlapping delivery is
 * possible and replays are expected semantics, never an error to hide.
 */
public final class Member {

    /** A granted work lease bound to this member's fencing epoch. */
    public record Work(UUID workId, long leaseEpoch, long expiresAtTick) {}

    /** One fetched page plus the frontier tokens {@link #ack} commits. */
    public record Page(
            java.util.List<dev.whitewater.model.FeedRecord> records,
            Cursor cursor,
            Map<String, String> positions,
            long ownershipEpoch,
            long deliveryAttempt,
            Long nextDeliveryNs,
            JsonNode exhausted) {}

    private final Transport transport;
    private final String subscription;
    private volatile UUID memberId;
    private volatile long memberEpoch;
    private volatile long ownershipEpoch;

    Member(Transport transport, String subscription) {
        this.transport = transport;
        this.subscription = Objects.requireNonNull(subscription);
    }

    /** Join the Subscription under an auto-generated member identity. */
    Member join() {
        return join(UUID.randomUUID());
    }

    /** Join under an explicit member identity. */
    Member join(UUID memberId) {
        var body = Transport.json().createObjectNode();
        body.put("subscription", subscription);
        body.put("request_id", UUID.randomUUID().toString());
        body.put("member_id", memberId.toString());
        JsonNode response = transport.post("/v1/subscriptions/members/join", body);
        this.memberId = memberId;
        this.memberEpoch = response.path("member_epoch").asLong();
        this.ownershipEpoch = response.path("ownership_epoch").asLong();
        return this;
    }

    public String subscription() {
        return subscription;
    }

    public UUID memberId() {
        return memberId;
    }

    /** The member's fencing epoch. */
    public long memberEpoch() {
        return memberEpoch;
    }

    /** Claim a work item under a bounded lease; auto work identity. */
    public Work claim() {
        return claim(UUID.randomUUID(), null);
    }

    /** Claim {@code workId} with the default lease bound. */
    public Work claim(UUID workId) {
        return claim(workId, null);
    }

    /** Claim {@code workId} under an explicit lease bound. */
    public Work claim(UUID workId, Integer leaseTicks) {
        JsonNode response = lease("/v1/subscriptions/members/claim",
                workId, null, leaseTicks);
        return granted(response, workId);
    }

    /** Renew an outstanding lease. */
    public Work renew(Work work, Integer leaseTicks) {
        JsonNode response = lease("/v1/subscriptions/members/renew",
                work.workId(), work.leaseEpoch(), leaseTicks);
        return granted(response, work.workId());
    }

    /** Release a held lease without acknowledging. */
    public void release(Work work) {
        transport.post("/v1/subscriptions/members/release",
                leaseBody(work.workId(), work.leaseEpoch()));
    }

    /** Transfer held work to another member of the same Subscription. */
    public Work transfer(Work work, Member target, Integer leaseTicks) {
        var body = leaseBody(work.workId(), work.leaseEpoch());
        body.put("to_member_id", target.memberId.toString());
        body.put("to_member_epoch", target.memberEpoch);
        if (leaseTicks != null) {
            body.put("lease_ticks", leaseTicks);
        }
        JsonNode response = transport.post("/v1/subscriptions/members/transfer", body);
        return granted(response, work.workId());
    }

    /** Fetch a bounded page after the shared frontier. */
    public Page fetch(Work work, Integer limit) {
        var body = leaseBody(work.workId(), work.leaseEpoch());
        if (limit != null) {
            body.put("limit", limit);
        }
        JsonNode response = transport.post("/v1/subscriptions/members/fetch", body);
        Map<String, String> positions = new LinkedHashMap<>();
        response.path("positions").fields().forEachRemaining(entry ->
                positions.put(entry.getKey(), entry.getValue().asText()));
        JsonNode nextDelivery = response.path("next_delivery_ns");
        return new Page(
                Wire.records(response.path("records")),
                Cursor.of(response.path("cursor").isNull() ? null : response.path("cursor").asText()),
                Map.copyOf(positions),
                response.path("ownership_epoch").asLong(),
                response.path("delivery_attempt").asLong(),
                nextDelivery.isNull() || nextDelivery.isMissingNode()
                        ? null : nextDelivery.asLong(),
                response.path("exhausted").isNull() ? null : response.path("exhausted"));
    }

    /** Commit the fetched frontier; overlapping members are fenced here. */
    public void ack(Work work, Page page) {
        var body = leaseBody(work.workId(), work.leaseEpoch());
        if (page.cursor() != null) {
            body.put("cursor", page.cursor().value());
        }
        var positions = body.putObject("positions");
        page.positions().forEach(positions::put);
        transport.post("/v1/subscriptions/members/ack", body);
    }

    private JsonNode lease(String path, UUID workId, Long leaseEpoch, Integer leaseTicks) {
        var body = leaseBody(workId, leaseEpoch);
        if (leaseTicks != null) {
            body.put("lease_ticks", leaseTicks);
        }
        return transport.post(path, body);
    }

    private Work granted(JsonNode response, UUID workId) {
        JsonNode lease = response.path("lease");
        if (lease.isMissingNode() || lease.isNull()) {
            throw new WhitewaterException.Conflict(409, "lease.refused",
                    "work claim was not granted");
        }
        return new Work(
                UUID.fromString(lease.path("work_id").asText()),
                lease.path("lease_epoch").asLong(),
                lease.path("expires_at_tick").asLong());
    }

    private com.fasterxml.jackson.databind.node.ObjectNode leaseBody(UUID workId, Long leaseEpoch) {
        var body = Transport.json().createObjectNode();
        body.put("subscription", subscription);
        body.put("request_id", UUID.randomUUID().toString());
        body.put("member_id", memberId.toString());
        body.put("member_epoch", memberEpoch);
        body.put("work_id", workId.toString());
        if (leaseEpoch != null) {
            body.put("lease_epoch", leaseEpoch);
        }
        return body;
    }
}
