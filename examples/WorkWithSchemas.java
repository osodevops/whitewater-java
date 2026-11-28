import dev.whitewater.Domain;
import dev.whitewater.Feed;
import dev.whitewater.FeedWriter;
import dev.whitewater.StateStore;
import dev.whitewater.WhitewaterClient;
import dev.whitewater.WhitewaterException;
import dev.whitewater.model.StateStoreSpec;
import dev.whitewater.schema.SchemaCompatibility;
import dev.whitewater.schema.SchemaCompatibilityResult;
import dev.whitewater.schema.SchemaEntry;
import dev.whitewater.schema.SchemaTarget;
import dev.whitewater.schema.SchemaType;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletionException;

/**
 * End-to-end walkthrough of the Whitewater Schema Registry
 * ({@code whitewater.schema.registry}):
 * <ul>
 *   <li>Associating JSON Schema, Avro, and Protobuf schemas with Feeds
 *       (key and message) and StateStores</li>
 *   <li>Automatic client-side validation on synchronous {@code append},
 *       asynchronous {@code write}, and {@code StateStore.put}, including
 *       where {@link WhitewaterException.SchemaValidation} errors surface</li>
 *   <li>{@code BACKWARD}, {@code FORWARD}, and {@code FULL} schema
 *       evolution compatibility checks</li>
 *   <li>Sharing a schema across Feeds with read-only linked targets</li>
 * </ul>
 */
public class WorkWithSchemas {

    public static void main(String[] args) {
        try (var client = WhitewaterClient.builder()
                .endpoints(
                        URI.create("http://localhost:7371"),
                        URI.create("http://localhost:7372"),
                        URI.create("http://localhost:7373"))
                .apiKey("whitewater-local-development-admin-key")
                .connectTimeout(Duration.ofSeconds(10))
                .build()) {
            runDemo(client);
        }
    }

    public static void runDemo(WhitewaterClient client) {
        Domain shop = client.createDomain("shop");
        shop.createFeed("orders");
        shop.createFeed("invoices");
        Feed<String, String> orders = shop.feed("orders");
        Feed<String, String> invoices = shop.feed("invoices");
        StateStore<String, String> customers = shop.defineStateStore(
                StateStoreSpec.named("customers"), (String) null);

        // 1. Invalid schema definition is rejected immediately on registration
        try {
            orders.updateSchema("{not-valid-json", SchemaTarget.MESSAGE, SchemaType.JSON);
        } catch (WhitewaterException.SchemaValidation e) {
            System.out.println("Caught invalid schema definition: " + e.getMessage());
        }

        // 2. Register JSON Schema on shop.orders (both KEY and MESSAGE)
        String orderKeySchema = """
                {"type":"string","pattern":"^ord-[0-9]+$"}
                """;
        String orderMsgV1 = """
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
        orders.updateSchema(orderKeySchema, SchemaTarget.KEY, SchemaType.JSON);
        SchemaEntry orderEntry = orders.updateSchema(
                orderMsgV1, SchemaTarget.MESSAGE, SchemaType.JSON, SchemaCompatibility.BACKWARD);
        System.out.println("Registered " + orderEntry.key() + " v" + orderEntry.version());

        // 3. Write valid and invalid records on shop.orders (sync append & async write)
        try (FeedWriter<String, String> writer = orders.writer("order-ingest")) {
            writer.append("ord-100", "{\"orderId\":\"ord-100\",\"total\":49.95}");

            // Invalid key ("bad-key" does not match ^ord-[0-9]+$) throws SchemaValidation
            try {
                writer.append("bad-key", "{\"orderId\":\"ord-101\",\"total\":10}");
            } catch (WhitewaterException.SchemaValidation e) {
                System.out.println("Caught invalid key on append: " + e.violations());
            }

            // Invalid message payload (negative total) throws SchemaValidation
            try {
                writer.append("ord-102", "{\"orderId\":\"ord-102\",\"total\":-5}");
            } catch (WhitewaterException.SchemaValidation e) {
                System.out.println("Caught invalid message on append: " + e.violations());
            }

            // Async write(): invalid record fails its CompletableFuture without poisoning the batch
            var badFuture = writer.write("ord-103", "{\"orderId\":\"ord-103\"}");
            var goodFuture = writer.write("ord-104", "{\"orderId\":\"ord-104\",\"total\":120}");
            try {
                badFuture.join();
            } catch (CompletionException ce) {
                if (ce.getCause() instanceof WhitewaterException.SchemaValidation sv) {
                    System.out.println("Caught invalid async write on join(): " + sv.violations());
                }
            }
            System.out.println("Valid async write committed messageId=" + goodFuture.join().messageId());
        }

        // 4. Register Avro schema on shop.invoices & demonstrate BACKWARD / FORWARD / FULL compatibility
        String avroV1 = """
                {
                  "type": "record",
                  "name": "Invoice",
                  "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "amount", "type": "long"}
                  ]
                }
                """;
        invoices.updateSchema(avroV1, SchemaTarget.MESSAGE, SchemaType.AVRO, SchemaCompatibility.BACKWARD);

        // BACKWARD: adding a required field without a default is rejected
        String avroAddRequiredNoDefault = """
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
        SchemaCompatibilityResult backCheck = client.checkSchemaCompatibility(
                "shop.invoices", avroAddRequiredNoDefault, SchemaTarget.MESSAGE, SchemaType.AVRO);
        System.out.println("Avro BACKWARD check (new required field without default): compatible="
                + backCheck.compatible() + " messages=" + backCheck.messages());

        // FORWARD: switch to FORWARD — adding a required field is FORWARD-compatible,
        // whereas dropping 'amount' (which v1 readers require) is rejected!
        client.updateSchemaCompatibility("shop.invoices", SchemaTarget.MESSAGE, SchemaCompatibility.FORWARD);
        String avroDropRequired = """
                {
                  "type": "record",
                  "name": "Invoice",
                  "fields": [
                    {"name": "id", "type": "string"}
                  ]
                }
                """;
        SchemaCompatibilityResult fwdCheck = client.checkSchemaCompatibility(
                "shop.invoices", avroDropRequired, SchemaTarget.MESSAGE, SchemaType.AVRO);
        System.out.println("Avro FORWARD check (dropping required field): compatible="
                + fwdCheck.compatible() + " messages=" + fwdCheck.messages());

        // FULL: switch to FULL — adding a field with a default is both BACKWARD and FORWARD compatible
        client.updateSchemaCompatibility("shop.invoices", SchemaTarget.MESSAGE, SchemaCompatibility.FULL);
        String avroGoodV2 = """
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
        SchemaEntry invoiceV2 = invoices.updateSchema(
                avroGoodV2, SchemaTarget.MESSAGE, SchemaType.AVRO, SchemaCompatibility.FULL);
        System.out.println("Evolved shop.invoices|message to v" + invoiceV2.version());

        // 5. Register Protobuf schema on StateStore shop.customers & validate puts
        String protoV1 = """
                syntax = "proto3";
                message Customer {
                  string id = 1;
                  string email = 2;
                  int32 tier = 3;
                }
                """;
        customers.updateSchema(protoV1, SchemaTarget.MESSAGE, SchemaType.PROTOBUF, SchemaCompatibility.BACKWARD);

        customers.put("cust-1", "{\"id\":\"cust-1\",\"email\":\"alice@example.com\",\"tier\":2}");
        try {
            customers.put("cust-2", "{\"id\":\"cust-2\",\"tier\":\"not-an-int\"}");
        } catch (WhitewaterException.SchemaValidation e) {
            System.out.println("Caught invalid StateStore.put payload: " + e.violations());
        }

        // Incompatible Protobuf evolution (changing field #3 type from int32 to string) fails
        String protoBadV2 = """
                syntax = "proto3";
                message Customer {
                  string id = 1;
                  string email = 2;
                  string tier = 3;
                }
                """;
        try {
            customers.updateSchema(protoBadV2, SchemaTarget.MESSAGE, SchemaType.PROTOBUF);
        } catch (WhitewaterException.SchemaValidation e) {
            System.out.println("Caught incompatible Protobuf evolution: " + e.getMessage());
        }

        // Compatible Protobuf evolution (adding optional field #4) bumps to v2
        String protoGoodV2 = """
                syntax = "proto3";
                message Customer {
                  string id = 1;
                  string email = 2;
                  int32 tier = 3;
                  string region = 4;
                }
                """;
        SchemaEntry custV2 = customers.updateSchema(protoGoodV2, SchemaTarget.MESSAGE, SchemaType.PROTOBUF);
        System.out.println("Evolved shop.customers|message to v" + custV2.version());

        // 6. Share shop.orders|message with audit.orders|message as a read-only linked target
        Domain audit = client.createDomain("audit");
        audit.createFeed("orders");
        SchemaEntry linked = client.linkSchema(
                "shop.orders", SchemaTarget.MESSAGE,
                "audit.orders", SchemaTarget.MESSAGE);
        System.out.println("Linked read-only schema " + linked.key() + " -> " + linked.sourceKey());
    }
}
