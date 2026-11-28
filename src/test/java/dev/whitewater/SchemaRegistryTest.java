package dev.whitewater;

import dev.whitewater.model.StateStoreSpec;
import dev.whitewater.schema.SchemaCompatibility;
import dev.whitewater.schema.SchemaCompatibilityResult;
import dev.whitewater.schema.SchemaEntry;
import dev.whitewater.schema.SchemaRegistry;
import dev.whitewater.schema.SchemaTarget;
import dev.whitewater.schema.SchemaType;
import dev.whitewater.testing.FakeRiverbed;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaRegistryTest {

    private FakeRiverbed riverbed;
    private WhitewaterClient client;

    @BeforeEach
    void setUp() {
        riverbed = FakeRiverbed.start();
        client = riverbed.client();
    }

    @AfterEach
    void tearDown() {
        client.close();
        riverbed.close();
    }

    @Test
    void internalRegistryStoreCannotBeOpenedOrRegisteredDirectly() {
        Domain ww = client.createDomain("whitewater");
        assertThrows(IllegalArgumentException.class,
                () -> ww.stateStore("schema.registry"));
        assertThrows(IllegalArgumentException.class,
                () -> ww.defineStateStore(StateStoreSpec.named("schema.registry"), (String) null));
    }

    @Test
    void feedMessageAndKeySchemasValidateSyncAndAsyncWrites() {
        Domain orders = client.createDomain("orders");
        orders.createFeed("events");
        Feed<String, String> feed = orders.feed("events");

        String keySchema = """
                {"type":"string","minLength":3,"pattern":"^ord-"}
                """;
        String msgSchema = """
                {
                  "type": "object",
                  "required": ["orderId", "total"],
                  "properties": {
                    "orderId": {"type": "string"},
                    "total": {"type": "number", "minimum": 0}
                  },
                  "additionalProperties": false
                }
                """;

        SchemaEntry keyEntry = feed.updateSchema(keySchema, SchemaTarget.KEY, SchemaType.JSON);
        SchemaEntry msgEntry = feed.updateSchema(msgSchema, SchemaTarget.MESSAGE, SchemaType.JSON);
        assertEquals("orders.events|key", keyEntry.key());
        assertEquals("orders.events|message", msgEntry.key());
        assertEquals(1, msgEntry.version());

        try (FeedWriter<String, String> writer = feed.writer("ingest")) {
            var ok = writer.append("ord-1", "{\"orderId\":\"ord-1\",\"total\":42.5}");
            assertFalse(ok.messageId().toString().isBlank());

            // Sync append throws SchemaValidation on bad payload or bad key
            var badMsg = assertThrows(WhitewaterException.SchemaValidation.class,
                    () -> writer.append("ord-2", "{\"orderId\":\"ord-2\",\"total\":-5}"));
            assertTrue(badMsg.getMessage().contains("orders.events|message"));

            var badKey = assertThrows(WhitewaterException.SchemaValidation.class,
                    () -> writer.append("bad-key", "{\"orderId\":\"ord-2\",\"total\":10}"));
            assertTrue(badKey.getMessage().contains("orders.events|key"));

            // Async write completes future exceptionally with SchemaValidation without poisoning batch
            var failedFuture = writer.write("ord-3", "{\"orderId\":\"ord-3\"}");
            CompletionException ce = assertThrows(CompletionException.class, failedFuture::join);
            assertInstanceOf(WhitewaterException.SchemaValidation.class, ce.getCause());

            // Subsequent valid async write still succeeds
            var validFuture = writer.write("ord-4", "{\"orderId\":\"ord-4\",\"total\":99}");
            assertFalse(validFuture.join().messageId().toString().isBlank());
        }
    }

    @Test
    void stateStoreAssociatedSchemasValidatePutOperations() {
        Domain shop = client.createDomain("shop");
        StateStore<String, String> customers = shop.defineStateStore(
                StateStoreSpec.named("customers"), (String) null);

        String protoSchema = """
                syntax = "proto3";
                message Customer {
                  string id = 1;
                  string email = 2;
                  int32 tier = 3;
                }
                """;
        customers.updateSchema(protoSchema, SchemaTarget.Message, SchemaType.Protobuf);

        customers.put("cust-1", "{\"id\":\"cust-1\",\"email\":\"a@b.com\",\"tier\":2}");
        assertEquals("{\"id\":\"cust-1\",\"email\":\"a@b.com\",\"tier\":2}",
                customers.find("cust-1").orElseThrow());

        assertThrows(WhitewaterException.SchemaValidation.class,
                () -> customers.put("cust-2", "{\"id\":\"cust-2\",\"unknownField\":true}"));
        assertThrows(WhitewaterException.SchemaValidation.class,
                () -> customers.put("cust-2", "{\"id\":\"cust-2\",\"tier\":\"not-an-int\"}"));
    }

    @Test
    void avroBackwardCompatibilityAndVersionEvolution() {
        Domain billing = client.createDomain("billing");
        billing.createFeed("invoices");

        String v1 = """
                {
                  "type": "record",
                  "name": "Invoice",
                  "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "amount", "type": "long"}
                  ]
                }
                """;
        SchemaEntry e1 = billing.updateSchema("invoices", v1, SchemaTarget.MESSAGE, SchemaType.AVRO);
        assertEquals(1, e1.version());

        // Incompatible v2: adds required field without default under BACKWARD
        String badV2 = """
                {
                  "type": "record",
                  "name": "Invoice",
                  "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "amount", "type": "long"},
                    {"name": "currency", "type": "string"}
                  ]
                }
                """;
        SchemaCompatibilityResult checkBad = client.checkSchemaCompatibility(
                "billing.invoices", badV2, SchemaTarget.MESSAGE, SchemaType.AVRO);
        assertFalse(checkBad.compatible());
        assertThrows(WhitewaterException.SchemaValidation.class,
                () -> billing.updateSchema("invoices", badV2, SchemaTarget.MESSAGE, SchemaType.AVRO));

        // Compatible v2: adds field with default
        String goodV2 = """
                {
                  "type": "record",
                  "name": "Invoice",
                  "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "amount", "type": "long"},
                    {"name": "currency", "type": "string", "default": "EUR"}
                  ]
                }
                """;
        SchemaCompatibilityResult checkGood = client.checkSchemaCompatibility(
                "billing.invoices", goodV2, SchemaTarget.MESSAGE, SchemaType.AVRO);
        assertTrue(checkGood.compatible());

        SchemaEntry e2 = billing.updateSchema(
                "invoices", goodV2, SchemaTarget.MESSAGE, SchemaType.AVRO, SchemaCompatibility.BACKWARD);
        assertEquals(2, e2.version());
        assertEquals(2, e2.versions().size());

        List<SchemaEntry> listed = billing.schemas();
        assertEquals(1, listed.size());
        assertEquals("billing.invoices|message", listed.get(0).key());
        assertEquals(SchemaRegistry.STORE_NAME, "whitewater.schema.registry");
    }

    @Test
    void newSchemaAddedFromAnotherClientTakesEffectOnNewClientOrRefresh() {
        Domain ops = client.createDomain("ops");
        ops.createFeed("alerts");
        Feed<String, String> alertsClient1 = ops.feed("alerts");
        try (FeedWriter<String, String> writer1 = alertsClient1.writer("w1")) {
            // First write caches 'no schema' on client 1
            writer1.append("k1", "free-form-text");

            // Another client registers a JSON schema on ops.alerts|message
            try (WhitewaterClient adminClient = riverbed.client()) {
                adminClient.updateSchema(
                        "ops.alerts",
                        "{\"type\":\"object\",\"required\":[\"severity\"]}",
                        SchemaTarget.MESSAGE,
                        SchemaType.JSON);
            }

            // Client 1 still uses its cached lookup until restarted/refreshed
            writer1.append("k2", "still-allowed-before-restart");

            // A newly started client immediately enforces the schema on first write
            try (WhitewaterClient restartedClient = riverbed.client();
                 FeedWriter<String, String> writer2 = restartedClient.domain("ops").feed("alerts").writer("w2")) {
                assertThrows(WhitewaterException.SchemaValidation.class,
                        () -> writer2.append("k3", "not-json-object"));
                writer2.append("k3", "{\"severity\":\"HIGH\"}");
            }
        }
    }

    @Test
    void jsonSchemaBackwardForwardFullAndTransitiveCompatibility() {
        Domain d = client.createDomain("compat");
        d.createFeed("jsonfeed");

        String v1 = """
                {
                  "type": "object",
                  "required": ["id", "name"],
                  "properties": {
                    "id": {"type": "string"},
                    "name": {"type": "string"}
                  }
                }
                """;
        d.updateSchema("jsonfeed", v1, SchemaTarget.MESSAGE, SchemaType.JSON, SchemaCompatibility.BACKWARD);

        // BACKWARD: adding optional field is compatible; adding required field without default fails
        String addRequired = """
                {
                  "type": "object",
                  "required": ["id", "name", "email"],
                  "properties": {
                    "id": {"type": "string"},
                    "name": {"type": "string"},
                    "email": {"type": "string"}
                  }
                }
                """;
        assertFalse(client.checkSchemaCompatibility("compat.jsonfeed", addRequired, SchemaTarget.MESSAGE, SchemaType.JSON).compatible());

        // Switch to FORWARD: adding a new required field is FORWARD-compatible (old reader ignores it),
        // but removing 'name' (which old reader requires) fails FORWARD compatibility!
        client.updateSchemaCompatibility("compat.jsonfeed", SchemaTarget.MESSAGE, SchemaCompatibility.FORWARD);
        assertTrue(client.checkSchemaCompatibility("compat.jsonfeed", addRequired, SchemaTarget.MESSAGE, SchemaType.JSON).compatible());

        String dropRequired = """
                {
                  "type": "object",
                  "required": ["id"],
                  "properties": {
                    "id": {"type": "string"}
                  }
                }
                """;
        assertFalse(client.checkSchemaCompatibility("compat.jsonfeed", dropRequired, SchemaTarget.MESSAGE, SchemaType.JSON).compatible());

        // Switch to FULL: both adding a required field without default and dropping a required field fail;
        // adding an optional field (or field with default) succeeds!
        client.updateSchemaCompatibility("compat.jsonfeed", SchemaTarget.MESSAGE, SchemaCompatibility.FULL);
        assertFalse(client.checkSchemaCompatibility("compat.jsonfeed", addRequired, SchemaTarget.MESSAGE, SchemaType.JSON).compatible());
        assertFalse(client.checkSchemaCompatibility("compat.jsonfeed", dropRequired, SchemaTarget.MESSAGE, SchemaType.JSON).compatible());

        String addOptional = """
                {
                  "type": "object",
                  "required": ["id", "name"],
                  "properties": {
                    "id": {"type": "string"},
                    "name": {"type": "string"},
                    "nickname": {"type": "string"}
                  }
                }
                """;
        assertTrue(client.checkSchemaCompatibility("compat.jsonfeed", addOptional, SchemaTarget.MESSAGE, SchemaType.JSON).compatible());
        SchemaEntry v2 = d.updateSchema("jsonfeed", addOptional, SchemaTarget.MESSAGE, SchemaType.JSON, SchemaCompatibility.FULL);
        assertEquals(2, v2.version());
    }


    @Test
    void avroForwardAndFullCompatibilityAndPayloadValidation() {
        Domain d = client.createDomain("avrodom");
        d.createFeed("events");
        Feed<String, String> feed = d.feed("events");

        String v1 = """
                {
                  "type": "record",
                  "name": "AccountEvent",
                  "fields": [
                    {"name": "accountId", "type": "string"},
                    {"name": "balance", "type": "long"}
                  ]
                }
                """;
        d.updateSchema("events", v1, SchemaTarget.MESSAGE, SchemaType.AVRO, SchemaCompatibility.FORWARD);

        String dropBalance = """
                {
                  "type": "record",
                  "name": "AccountEvent",
                  "fields": [
                    {"name": "accountId", "type": "string"}
                  ]
                }
                """;
        assertFalse(client.checkSchemaCompatibility("avrodom.events", dropBalance, SchemaTarget.MESSAGE, SchemaType.AVRO).compatible());

        client.updateSchemaCompatibility("avrodom.events", SchemaTarget.MESSAGE, SchemaCompatibility.FULL);
        String fullV2 = """
                {
                  "type": "record",
                  "name": "AccountEvent",
                  "fields": [
                    {"name": "accountId", "type": "string"},
                    {"name": "balance", "type": "long"},
                    {"name": "region", "type": "string", "default": "EU"}
                  ]
                }
                """;
        SchemaEntry e2 = d.updateSchema("events", fullV2, SchemaTarget.MESSAGE, SchemaType.AVRO, SchemaCompatibility.FULL);
        assertEquals(2, e2.version());

        try (FeedWriter<String, String> writer = feed.writer("w")) {
            writer.append("k1", "{\"accountId\":\"a1\",\"balance\":500}");
            assertThrows(WhitewaterException.SchemaValidation.class,
                    () -> writer.append("k2", "{\"accountId\":\"a2\",\"balance\":\"not-a-long\"}"));
            assertThrows(WhitewaterException.SchemaValidation.class,
                    () -> writer.append("k3", "{\"accountId\":\"a3\",\"balance\":10,\"undeclared\":true}"));
        }
    }


    @Test
    void protobufBackwardForwardFullCompatibilityAndValidation() {
        Domain d = client.createDomain("protodom");
        d.createFeed("shipments");

        String v1 = """
                syntax = "proto2";
                enum Status {
                  PENDING = 0;
                  SHIPPED = 1;
                }
                message Shipment {
                  required string id = 1;
                  optional int32 weight = 2;
                  optional Status status = 3;
                }
                """;
        d.updateSchema("shipments", v1, SchemaTarget.MESSAGE, SchemaType.PROTOBUF, SchemaCompatibility.BACKWARD);

        String addRequiredV2 = """
                syntax = "proto2";
                enum Status {
                  PENDING = 0;
                  SHIPPED = 1;
                }
                message Shipment {
                  required string id = 1;
                  optional int32 weight = 2;
                  optional Status status = 3;
                  required string carrier = 4;
                }
                """;
        assertFalse(client.checkSchemaCompatibility("protodom.shipments", addRequiredV2, SchemaTarget.MESSAGE, SchemaType.PROTOBUF).compatible());

        client.updateSchemaCompatibility("protodom.shipments", SchemaTarget.MESSAGE, SchemaCompatibility.FORWARD);
        String dropRequiredV2 = """
                syntax = "proto2";
                enum Status {
                  PENDING = 0;
                  SHIPPED = 1;
                }
                message Shipment {
                  optional int32 weight = 2;
                  optional Status status = 3;
                }
                """;
        assertFalse(client.checkSchemaCompatibility("protodom.shipments", dropRequiredV2, SchemaTarget.MESSAGE, SchemaType.PROTOBUF).compatible());

        client.updateSchemaCompatibility("protodom.shipments", SchemaTarget.MESSAGE, SchemaCompatibility.FULL);
        String renameField = """
                syntax = "proto2";
                enum Status {
                  PENDING = 0;
                  SHIPPED = 1;
                }
                message Shipment {
                  required string id = 1;
                  optional int32 mass = 2;
                  optional Status status = 3;
                }
                """;
        assertFalse(client.checkSchemaCompatibility("protodom.shipments", renameField, SchemaTarget.MESSAGE, SchemaType.PROTOBUF).compatible());

        String removeEnumSymbol = """
                syntax = "proto2";
                enum Status {
                  PENDING = 0;
                }
                message Shipment {
                  required string id = 1;
                  optional int32 weight = 2;
                  optional Status status = 3;
                }
                """;
        assertFalse(client.checkSchemaCompatibility("protodom.shipments", removeEnumSymbol, SchemaTarget.MESSAGE, SchemaType.PROTOBUF).compatible());

        String goodFullV2 = """
                syntax = "proto2";
                enum Status {
                  PENDING = 0;
                  SHIPPED = 1;
                }
                message Shipment {
                  required string id = 1;
                  optional int32 weight = 2;
                  optional Status status = 3;
                  optional string carrier = 4;
                }
                """;
        SchemaEntry e2 = d.updateSchema("shipments", goodFullV2, SchemaTarget.MESSAGE, SchemaType.PROTOBUF, SchemaCompatibility.FULL);
        assertEquals(2, e2.version());
    }


    @Test
    void cacheAvoidsRepeatedLookupsAndRefreshesVersionAfterTtlExpires() throws Exception {
        Domain d = client.createDomain("cachedom");
        d.createFeed("orders");

        String v1 = """
                {
                  "type": "object",
                  "required": ["id"],
                  "properties": {
                    "id": {"type": "string"},
                    "amount": {"type": "number"}
                  },
                  "additionalProperties": false
                }
                """;
        try (WhitewaterClient admin = riverbed.client()) {
            admin.updateSchema("cachedom.orders", v1, SchemaTarget.MESSAGE, SchemaType.JSON, SchemaCompatibility.NONE);
        }

        try (WhitewaterClient producer = riverbed.client();
             FeedWriter<String, String> writer = producer.domain("cachedom").feed("orders").writer("p1")) {
            producer.schemas().setCacheTtl(java.time.Duration.ofMillis(40));
            long before = producer.schemas().lookupCount();
            writer.append("k1", "{\"id\":\"1\"}");
            long afterFirst = producer.schemas().lookupCount();
            assertEquals(before + 2, afterFirst);

            // Subsequent writes within TTL hit the in-memory cache with 0 additional lookups
            writer.append("k2", "{\"id\":\"2\"}");
            writer.append("k3", "{\"id\":\"3\"}");
            assertEquals(afterFirst, producer.schemas().lookupCount());

            String v2 = """
                    {
                      "type": "object",
                      "required": ["id", "amount"],
                      "properties": {
                        "id": {"type": "string"},
                        "amount": {"type": "number"}
                      },
                      "additionalProperties": false
                    }
                    """;
            try (WhitewaterClient admin = riverbed.client()) {
                SchemaEntry evolved = admin.updateSchema("cachedom.orders", v2, SchemaTarget.MESSAGE, SchemaType.JSON, SchemaCompatibility.NONE);
                assertEquals(2, evolved.version());
            }

            // Wait for the 40ms TTL to expire so the next write checks for a newer schema version
            Thread.sleep(60);

            // Now v2 is automatically fetched and enforced: missing 'amount' is rejected!
            assertThrows(WhitewaterException.SchemaValidation.class,
                    () -> writer.append("k4", "{\"id\":\"4\"}"));
            writer.append("k4", "{\"id\":\"4\",\"amount\":99.5}");
        }
    }


    @Test
    void linkedReadOnlyTargetsAndCrossClusterReplicationPropagateAndEnforceReadOnly() {
        Domain common = client.createDomain("common");
        Domain billing = client.createDomain("billing");
        billing.createFeed("orders");
        Domain audit = client.createDomain("audit");
        audit.createFeed("orders");

        // Standalone Apicurio-style primary schema (not even requiring a feed named common.order)
        String v1 = """
                {
                  "type": "object",
                  "required": ["orderId"],
                  "properties": {
                    "orderId": {"type": "string"},
                    "total": {"type": "number"}
                  },
                  "additionalProperties": false
                }
                """;
        SchemaEntry primary = common.updateSchema("order", v1, SchemaTarget.MESSAGE, SchemaType.JSON, SchemaCompatibility.NONE);
        assertFalse(primary.readOnly());

        // Link billing.orders|message and audit.orders|message as read-only shares
        SchemaEntry linkedBilling = client.linkSchema("common.order", SchemaTarget.MESSAGE, "billing.orders", SchemaTarget.MESSAGE);
        SchemaEntry linkedAudit = client.linkSchema("common.order", SchemaTarget.MESSAGE, "audit.orders", SchemaTarget.MESSAGE);
        assertTrue(linkedBilling.readOnly());
        assertEquals("common.order|message", linkedBilling.sourceKey());
        assertTrue(linkedAudit.readOnly());

        // Direct edit on a read-only linked target is rejected
        assertThrows(WhitewaterException.SchemaValidation.class,
                () -> client.updateSchema("billing.orders", v1, SchemaTarget.MESSAGE, SchemaType.JSON));

        // Evolving the primary owner (common.order|message) automatically updates all linked feeds to v2!
        String v2 = """
                {
                  "type": "object",
                  "required": ["orderId", "total"],
                  "properties": {
                    "orderId": {"type": "string"},
                    "total": {"type": "number"}
                  },
                  "additionalProperties": false
                }
                """;
        SchemaEntry ownerV2 = common.updateSchema("order", v2, SchemaTarget.MESSAGE, SchemaType.JSON, SchemaCompatibility.NONE);
        assertEquals(2, ownerV2.version());
        assertEquals(List.of("billing.orders|message", "audit.orders|message"), ownerV2.linkedTargets());

        SchemaEntry billingAfter = client.getSchema("billing.orders", SchemaTarget.MESSAGE).orElseThrow();
        assertEquals(2, billingAfter.version());
        assertTrue(billingAfter.readOnly());

        // Writing to billing.orders enforces v2 (requires 'total') in 1 lookup
        try (FeedWriter<String, String> w = billing.feed("orders").writer("bw")) {
            assertThrows(WhitewaterException.SchemaValidation.class,
                    () -> w.append("k1", "{\"orderId\":\"o1\"}"));
            w.append("k1", "{\"orderId\":\"o1\",\"total\":50}");
        }

        // Replicate to a second Whitewater cluster as a read-only replica
        try (FakeRiverbed drRiverbed = FakeRiverbed.start();
             WhitewaterClient drClient = drRiverbed.client()) {
            drClient.createDomain("billing").createFeed("orders");
            SchemaEntry replica = client.replicateSchemaTo(drClient, "prod-eu", "billing.orders", SchemaTarget.MESSAGE);
            assertTrue(replica.readOnly());
            assertEquals("prod-eu", replica.sourceCluster());
            assertEquals("common.order|message", replica.sourceKey());
            assertEquals(2, replica.version());

            // Direct edit on DR cluster is blocked because it's a read-only replica
            assertThrows(WhitewaterException.SchemaValidation.class,
                    () -> drClient.updateSchema("billing.orders", v1, SchemaTarget.MESSAGE, SchemaType.JSON));

            // Producers on DR cluster validate locally against the replicated schema
            try (FeedWriter<String, String> drWriter = drClient.domain("billing").feed("orders").writer("drw")) {
                assertThrows(WhitewaterException.SchemaValidation.class,
                        () -> drWriter.append("k1", "{\"orderId\":\"o1\"}"));
                drWriter.append("k1", "{\"orderId\":\"o1\",\"total\":75}");
            }
        }
    }

}
