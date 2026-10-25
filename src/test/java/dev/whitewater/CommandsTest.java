package dev.whitewater;

import com.fasterxml.jackson.databind.JsonNode;
import dev.whitewater.admin.Commands;
import dev.whitewater.model.EnrichFreshness;
import dev.whitewater.model.EnrichMiss;
import dev.whitewater.model.ExhaustedDisposition;
import dev.whitewater.model.ExhaustedPolicy;
import dev.whitewater.model.Extractor;
import dev.whitewater.model.GrantEffect;
import dev.whitewater.model.IndexConsistency;
import dev.whitewater.model.IndexPredicate;
import dev.whitewater.model.NodeCapabilities;
import dev.whitewater.model.NodeCapacity;
import dev.whitewater.model.Permission;
import dev.whitewater.model.PipeOperation;
import dev.whitewater.model.ReaderStart;
import dev.whitewater.model.ResourceKind;
import dev.whitewater.model.ShowKind;
import dev.whitewater.model.WindowAggregation;
import dev.whitewater.model.WindowTime;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Wire-shape assertions for the typed {@link Commands} builder — every
 * command must lower to the exact {@code Command} enum JSON the Control
 * Plane's {@code /v1/admin/commands} accepts.
 */
class CommandsTest {

    private JsonNode first(Commands commands) {
        return commands.build().get(0);
    }

    @Test
    void createsLowerToTypedCommands() {
        var batch = Commands.of()
                .createDomain("orders")
                .createFeed("orders.events")
                .createWriter("ingest", "orders.events")
                .createReader("audit", "orders.events")
                .createSubscription("audit.sub", "orders.events")
                .createRole("auditor");
        List<JsonNode> commands = batch.build();
        assertEquals(6, commands.size());
        assertEquals("create_domain", commands.get(0).path("command").asText());
        assertEquals("orders", commands.get(0).path("name").asText());
        assertEquals("create_feed", commands.get(1).path("command").asText());
        assertEquals("orders.events", commands.get(1).path("name").asText());
        assertEquals("create_writer", commands.get(2).path("command").asText());
        assertEquals("ingest", commands.get(2).path("name").asText());
        assertEquals("orders.events", commands.get(2).path("feed").asText());
        assertEquals("create_reader", commands.get(3).path("command").asText());
        assertEquals("beginning", commands.get(3).path("start").path("kind").asText());
        assertEquals("create_subscription", commands.get(4).path("command").asText());
        assertEquals("orders.events", commands.get(4).path("feed").asText());
        assertEquals("create_role", commands.get(5).path("command").asText());
    }

    @Test
    void readerStartWireShapes() {
        assertEquals("beginning", ReaderStart.beginning().toWire().path("kind").asText());
        assertEquals("now", ReaderStart.now().toWire().path("kind").asText());
        var cursor = ReaderStart.cursor(new dev.whitewater.model.Cursor("opaque-cursor")).toWire();
        assertEquals("cursor", cursor.path("kind").asText());
        assertEquals("opaque-cursor", cursor.path("cursor").asText());
        var timestamp = ReaderStart.timestampNs(1_700_000_000_000_000_000L).toWire();
        assertEquals("timestamp", timestamp.path("kind").asText());
        assertEquals(1_700_000_000_000_000_000L, timestamp.path("cursor").asLong());
    }

    @Test
    void subscriptionWithPolicyAndDelay() {
        var command = first(Commands.of().createSubscription(
                "audit", "orders.events", null, ReaderStart.now(), 5_000_000_000L,
                ExhaustedPolicy.attempts(5, ExhaustedDisposition.SKIP)));
        assertEquals("create_subscription", command.path("command").asText());
        assertEquals(5_000_000_000L, command.path("delivery_delay_ns").asLong());
        assertEquals(5, command.path("exhausted_policy").path("max_delivery_attempts").asInt());
        assertEquals("skip", command.path("exhausted_policy").path("disposition").asText());
        assertEquals("now", command.path("start").path("kind").asText());
    }

    @Test
    void domainSourceSubscription() {
        var command = first(Commands.of().createDomainSubscription(
                "audit", "orders", ReaderStart.beginning()));
        assertEquals("create_subscription", command.path("command").asText());
        assertEquals("orders", command.path("domain").asText());
        assertFalse(command.hasNonNull("feed"));
    }

    @Test
    void everyResourceKindValidatesTheDottedGrammar() {
        // The server runs validate_dotted_name on writers, readers,
        // subscriptions, roles, pipes, indexes, and stores alike — the
        // client must refuse the loose single-segment spellings early.
        assertThrows(IllegalArgumentException.class,
                () -> Commands.of().createWriter("ingest-1", "orders.events"));
        assertThrows(IllegalArgumentException.class,
                () -> Commands.of().createSubscription("Audit.Sub", "orders.events"));
        assertThrows(IllegalArgumentException.class,
                () -> Commands.of().pausePipe("audit_relay"));
        assertThrows(IllegalArgumentException.class,
                () -> Commands.of().createRole("ROLE_admin"));
        assertThrows(IllegalArgumentException.class,
                () -> Commands.of().defineForwardPipe(
                        "orders.relay", "orders-sub", "orders.archive"));
        // Dotted multi-segment spellings are valid on every kind.
        var batch = Commands.of()
                .createWriter("orders.ingest", "orders.events")
                .createSubscription("orders.audit.sub", "orders.events")
                .defineForwardPipe("orders.audit.relay", "orders.audit.sub", "orders.archive")
                .createRole("auditors.emea");
        assertEquals(4, batch.build().size());
    }

    @Test
    void subscriptionRejectsBothOrNeitherSource() {
        assertThrows(IllegalArgumentException.class, () -> Commands.of()
                .createSubscription("x", "a.b", "a", null, null, null));
        assertThrows(IllegalArgumentException.class, () -> Commands.of()
                .createSubscription("x", null, null, null, null, null));
    }

    @Test
    void grantAndRevokeCarryActionsAndEffect() {
        var grant = first(Commands.of().grant(GrantEffect.DENY,
                Set.of(Permission.WRITE), "orders.events", "auditor"));
        assertEquals("grant", grant.path("command").asText());
        assertEquals("deny", grant.path("effect").asText());
        assertEquals("orders.events", grant.path("namespace").asText());
        assertEquals("auditor", grant.path("role").asText());
        assertEquals("write", grant.path("actions").get(0).asText());

        var revoke = first(Commands.of().revokeGrant(
                Set.of(Permission.READ), "orders.events", "auditor"));
        assertEquals("revoke_grant", revoke.path("command").asText());
        assertEquals("allow", revoke.path("effect").asText());
        assertEquals("read", revoke.path("actions").get(0).asText());
    }

    @Test
    void issueApiKeyCarriesDigestNotKey() {
        var command = first(Commands.of().issueApiKey(
                "ci", "deadbeef".repeat(8), "auditor", 1_800_000_000_000_000_000L));
        assertEquals("issue_api_key", command.path("command").asText());
        assertEquals("ci", command.path("name").asText());
        assertEquals("deadbeef".repeat(8), command.path("digest").asText());
        assertEquals(1_800_000_000_000_000_000L, command.path("expires_at_ns").asLong());
        assertFalse(command.hasNonNull("key"), "plaintext key must never appear in a command");
    }

    @Test
    void defineIndexSerializesFieldsAndConsistency() {
        var command = first(Commands.of().defineIndex(
                "orders.events.bystatus", "orders.events",
                List.of(Extractor.recordKey(), Extractor.metadata("status")),
                true, null, IndexConsistency.STRICT));
        assertEquals("define_index", command.path("command").asText());
        assertEquals("orders.events.bystatus", command.path("name").asText());
        assertTrue(command.path("unique").asBoolean());
        assertEquals("binary", command.path("collation").asText());
        assertEquals("strict", command.path("consistency").asText());
        var fields = command.path("fields");
        assertEquals(2, fields.size());
        assertEquals("key", fields.get(0).path("kind").asText());
        assertEquals("metadata", fields.get(1).path("kind").asText());
        assertEquals("status", fields.get(1).path("name").asText());
    }

    @Test
    void getIndexPredicateExternallyTagged() {
        var command = first(Commands.of().getIndex("orders.events.bystatus",
                IndexPredicate.exact("open"), 64, true, 500L));
        assertEquals("get_index", command.path("command").asText());
        var predicate = command.path("predicate");
        assertTrue(predicate.hasNonNull("exact"));
        assertEquals("text", predicate.path("exact").path("values").get(0).path("kind").asText());
        assertEquals("open", predicate.path("exact").path("values").get(0).path("value").asText());
        assertTrue(command.path("strict").asBoolean());
        assertEquals(500L, command.path("wait_ms").asLong());
    }

    @Test
    void indexPredicateRangeFlattensForQueryEndpoint() {
        var predicate = IndexPredicate.range(
                List.of("open"),
                IndexPredicate.IndexBound.inclusive(100L),
                IndexPredicate.IndexBound.exclusive(200L));
        var request = dev.whitewater.internal.Transport.json().createObjectNode();
        predicate.flattenInto(request);
        assertEquals("range", request.path("mode").asText());
        assertEquals("text", request.path("leading").get(0).path("kind").asText());
        assertEquals(100L, request.path("lower").path("value").path("value").asLong());
        assertTrue(request.path("lower").path("inclusive").asBoolean());
        assertFalse(request.path("upper").path("inclusive").asBoolean());
    }

    @Test
    void forwardPipeSerializesOperation() {
        var command = first(Commands.of().defineForwardPipe(
                "copy", "orders.audit", "orders.auditarchive"));
        assertEquals("define_pipe", command.path("command").asText());
        assertEquals("copy", command.path("name").asText());
        assertEquals("orders.audit", command.path("subscription").asText());
        assertEquals("orders.auditarchive", command.path("output_feed").asText());
        assertEquals("forward", command.path("operation").path("operation").asText());
    }

    @Test
    void enrichPipeSerializesKeyFreshnessMiss() {
        var command = first(Commands.of().defineEnrichPipe(
                "enrich", "orders.audit", "orders.enriched", "orders.customers",
                PipeOperation.enrich(
                        Extractor.metadata("customer_id"),
                        EnrichFreshness.STRICT,
                        "by-id",
                        EnrichMiss.QUARANTINE),
                "orders.quarantine"));
        assertEquals("define_pipe", command.path("command").asText());
        assertEquals("orders.enriched", command.path("output_feed").asText());
        assertEquals("orders.customers", command.path("output_store").asText());
        assertEquals("orders.quarantine", command.path("quarantine_feed").asText());
        var op = command.path("operation");
        assertEquals("enrich", op.path("operation").asText());
        assertEquals("metadata", op.path("key").path("kind").asText());
        assertEquals("customer_id", op.path("key").path("name").asText());
        assertEquals("strict", op.path("freshness").asText());
        assertEquals("by-id", op.path("named").asText());
        assertEquals("quarantine", op.path("on_miss").asText());
    }

    @Test
    void enrichPipeSerializesDropMiss() {
        var command = first(Commands.of().defineEnrichPipe(
                "enrich", "orders.audit", "orders.enriched", "orders.customers",
                PipeOperation.enrich(
                        Extractor.metadata("customer_id"),
                        EnrichFreshness.STRICT,
                        "by-id",
                        EnrichMiss.DROP),
                null));
        var op = command.path("operation");
        assertEquals("drop", op.path("on_miss").asText());
        assertTrue(command.path("quarantine_feed").isMissingNode()
                || command.path("quarantine_feed").isNull());
    }

    @Test
    void windowPipeSerializesBoundsAndEmit() {
        var command = first(Commands.of().defineWindowPipe(
                "hourly", "orders.audit", "orders.hourlyspend",
                PipeOperation.window(3_600_000_000_000L,
                        Extractor.recordKey(),
                        WindowAggregation.sum(Extractor.json("total")),
                        WindowTime.INGEST, 60_000_000_000L, 86_400_000_000_000L,
                        10_000L, 4_000_000L),
                "orders.hourlyfeed"));
        assertEquals("define_pipe", command.path("command").asText());
        assertEquals("orders.hourlyspend", command.path("output_store").asText());
        assertEquals("orders.hourlyfeed", command.path("emit_feed").asText());
        var op = command.path("operation");
        assertEquals("window", op.path("operation").asText());
        assertEquals(3_600_000_000_000L, op.path("window_ns").asLong());
        assertEquals("key", op.path("key").path("kind").asText());
        assertEquals("sum", op.path("aggregation").path("aggregation").asText());
        assertEquals("ingest", op.path("time").asText());
        assertEquals(60_000_000_000L, op.path("max_late_ns").asLong());
        assertEquals(86_400_000_000_000L, op.path("expire_ns").asLong());
        assertEquals(10_000L, op.path("key_limit").asLong());
        assertEquals(4_000_000L, op.path("byte_limit").asLong());
    }

    @Test
    void windowPipeSerializesWatermarkAndGrace() {
        var command = first(Commands.of().defineWindowPipe(
                "hourly.wm", "orders.audit", "orders.hourlyspend",
                PipeOperation.window(3_600_000_000_000L,
                        Extractor.recordKey(),
                        WindowAggregation.count(),
                        WindowTime.EVENT, null,
                        300_000_000_000L, 30_000_000_000L,
                        7_200_000_000_000L, null, null),
                "orders.hourlyfeed"));
        assertEquals("define_pipe", command.path("command").asText());
        assertEquals("orders.hourlyfeed", command.path("emit_feed").asText());
        var op = command.path("operation");
        assertEquals("window", op.path("operation").asText());
        assertEquals("event", op.path("time").asText());
        assertEquals("count", op.path("aggregation").path("aggregation").asText());
        assertFalse(op.hasNonNull("max_late_ns"));
        assertEquals(300_000_000_000L, op.path("watermark_ns").asLong());
        assertEquals(30_000_000_000L, op.path("grace_ns").asLong());
        assertEquals(7_200_000_000_000L, op.path("expire_ns").asLong());
    }

    @Test
    void windowWatermarkConvenienceEmitsHorizonAndGrace() {
        var op = PipeOperation.window(3_600_000_000_000L,
                        Extractor.recordKey(), WindowAggregation.count(),
                        300_000_000_000L, 30_000_000_000L)
                .toWire();
        assertEquals("window", op.path("operation").asText());
        assertEquals("event", op.path("time").asText());
        assertEquals(300_000_000_000L, op.path("watermark_ns").asLong());
        assertEquals(30_000_000_000L, op.path("grace_ns").asLong());
        assertFalse(op.hasNonNull("max_late_ns"));
        assertFalse(op.hasNonNull("expire_ns"));

        var horizonOnly = PipeOperation.window(3_600_000_000_000L,
                        Extractor.recordKey(), WindowAggregation.count(),
                        300_000_000_000L)
                .toWire();
        assertEquals(300_000_000_000L, horizonOnly.path("watermark_ns").asLong());
        assertFalse(horizonOnly.hasNonNull("grace_ns"));
    }

    @Test
    void windowLatenessModelValidation() {
        // GRACE requires WATERMARK — revision bounds ride the horizon.
        assertThrows(IllegalArgumentException.class, () -> PipeOperation.window(
                3_600_000_000_000L, Extractor.recordKey(), WindowAggregation.count(),
                WindowTime.EVENT, null, null, 30_000_000_000L, null, null, null));
        // GRACE under MAX_LATE is still a missing watermark.
        assertThrows(IllegalArgumentException.class, () -> PipeOperation.window(
                3_600_000_000_000L, Extractor.recordKey(), WindowAggregation.count(),
                WindowTime.EVENT, 60_000_000_000L, null, 30_000_000_000L, null, null, null));
        // MAX_LATE and WATERMARK are alternative lateness models.
        assertThrows(IllegalArgumentException.class, () -> PipeOperation.window(
                3_600_000_000_000L, Extractor.recordKey(), WindowAggregation.count(),
                WindowTime.EVENT, 60_000_000_000L, 300_000_000_000L, null, null, null, null));
        // WATERMARK follows the event clock only.
        assertThrows(IllegalArgumentException.class, () -> PipeOperation.window(
                3_600_000_000_000L, Extractor.recordKey(), WindowAggregation.count(),
                WindowTime.INGEST, null, 300_000_000_000L, null, null, null, null));
        // The watermark convenience routes through the same checks.
        assertThrows(IllegalArgumentException.class, () -> PipeOperation.window(
                0L, Extractor.recordKey(), WindowAggregation.count(), 1L));
    }

    @Test
    void wrongOperationKindRefused() {
        assertThrows(IllegalArgumentException.class, () -> Commands.of()
                .defineEnrichPipe("p", "s", "a.b", "a.s", PipeOperation.forward(), null));
        assertThrows(IllegalArgumentException.class, () -> Commands.of()
                .defineWindowPipe("p", "s", "a.s", PipeOperation.enrich(Extractor.recordKey()), null));
    }

    @Test
    void introspectionCommands() {
        var batch = Commands.of()
                .show(ShowKind.FEEDS)
                .describe(ResourceKind.FEED, "orders.events")
                .rename(ResourceKind.FEED, "orders.events", "orders.events-v2")
                .drop(ResourceKind.READER, "audit")
                .inspectPlacement("orders.events");
        List<JsonNode> commands = batch.build();
        assertEquals("show", commands.get(0).path("command").asText());
        assertEquals("feeds", commands.get(0).path("kind").asText());
        assertEquals("describe", commands.get(1).path("command").asText());
        assertEquals("rename", commands.get(2).path("command").asText());
        assertEquals("orders.events-v2", commands.get(2).path("new").asText());
        assertEquals("drop", commands.get(3).path("command").asText());
        assertEquals("reader", commands.get(3).path("kind").asText());
        assertEquals("inspect_placement", commands.get(4).path("command").asText());
    }

    @Test
    void storageNodeCommands() {
        var batch = Commands.of()
                .registerStorageNode("storage-9", "https://storage-9:7079",
                        Set.of("sha256:abc"), NodeCapabilities.stateless())
                .drainStorageNode("storage-9")
                .inspectStorageDrain("storage-9")
                .undrainStorageNode("storage-9")
                .retireStorageNode("storage-9")
                .reportStorageNodeCapacity("storage-9",
                        new NodeCapacity(16, 64L << 30, 1L << 40, 512L << 30,
                                25_000, "zone-a"));
        List<JsonNode> commands = batch.build();
        assertEquals("register_storage_node", commands.get(0).path("command").asText());
        assertEquals("storage-9", commands.get(0).path("node").asText());
        assertEquals("sha256:abc", commands.get(0).path("cert_pins").get(0).asText());
        assertFalse(commands.get(0).path("capabilities").path("storage").asBoolean());
        assertEquals("drain_storage_node", commands.get(1).path("command").asText());
        assertEquals("inspect_storage_drain", commands.get(2).path("command").asText());
        assertEquals("undrain_storage_node", commands.get(3).path("command").asText());
        assertEquals("retire_storage_node", commands.get(4).path("command").asText());
        assertEquals("report_storage_node_capacity", commands.get(5).path("command").asText());
        assertEquals(16, commands.get(5).path("capacity").path("cpu_cores").asInt());
        assertEquals("zone-a", commands.get(5).path("capacity").path("failure_domain").asText());
    }

    @Test
    void stateStoreCommandWithNamedKeys() {
        var key = new dev.whitewater.model.NamedKey("passport", true, Extractor.json("passport"));
        var command = first(Commands.of().defineStateStore(
                "orders.employees", "orders.staff", List.of(key)));
        assertEquals("define_state_store", command.path("command").asText());
        assertEquals("orders.employees", command.path("name").asText());
        assertEquals("feed", command.path("source").path("kind").asText());
        assertEquals("orders.staff", command.path("source").path("feed").asText());
        var keys = command.path("named_keys");
        assertEquals(1, keys.size());
        assertEquals("passport", keys.get(0).path("name").asText());
        assertTrue(keys.get(0).path("unique").asBoolean());
        assertEquals("projection", keys.get(0).path("extractor").path("kind").asText());
    }

    @Test
    void invalidNamesRejectedLocally() {
        assertThrows(IllegalArgumentException.class, () -> Commands.of().createFeed("UPPER.Bad"));
        assertThrows(IllegalArgumentException.class, () -> Commands.of().createFeed(".leading"));
        assertThrows(IllegalArgumentException.class, () -> Commands.of().createFeed("trailing."));
        assertThrows(IllegalArgumentException.class, () -> Commands.of().createWriter("bad name!", "a.b"));
        assertThrows(IllegalArgumentException.class, () -> Commands.of().defineIndex("x.y", "a.b", List.of()));
    }
}
