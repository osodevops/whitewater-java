package dev.whitewater;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import dev.whitewater.internal.Transport;
import dev.whitewater.model.IndexPredicate;
import dev.whitewater.model.ReaderStart;
import dev.whitewater.model.StateStoreSpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The Domain is the only front door: every resource name passed to a
 * Domain method resolves inside that Domain ("orders" inside "pipes"
 * is pipes.orders), and Pipe endpoints can cross Domains only through
 * an explicit absolute reference — a handle
 * (Feed/StateStore/SubscriptionRef) or Scoped.absolute. These tests pin
 * the exact wire shapes both directions.
 */
class DomainSurfaceTest {

    private HttpServer server;
    private WhitewaterClient client;
    private final List<String[]> calls = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() throws IOException {
        calls.clear();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            var path = exchange.getRequestURI().toString();
            var body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            calls.add(new String[]{path, body});
            var response = "/v1/readers/open".equals(path)
                    ? ("{\"reader_id\":\"018f0000-0000-7000-8000-000000000001\","
                            + "\"feed_id\":\"018f0000-0000-7000-8000-000000000002\","
                            + "\"session_epoch\":3,\"capacity\":64,"
                            + "\"delivered_cursor\":null,\"acknowledged_cursor\":null}")
                    : ("{\"request_id\":\"019a0000-0000-7000-8000-000000000006\","
                            + "\"revision\":9,\"authority\":\"control_plane\",\"warning\":null,"
                            + "\"results\":[{\"statement\":\"s\",\"message\":\"ok\","
                            + "\"data\":{\"session_epoch\":3}}]}");
            byte[] out = response.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("content-type", "application/json");
            exchange.sendResponseHeaders(200, out.length);
            exchange.getResponseBody().write(out);
            exchange.close();
        });
        server.start();
        client = WhitewaterClient.builder()
                .endpoints(URI.create("http://127.0.0.1:" + server.getAddress().getPort()))
                .apiKey("test-key")
                .build();
    }

    @AfterEach
    void tearDown() {
        server.stop(0);
    }

    private String scriptAt(int index) {
        var call = calls.get(index);
        assertEquals("/v1/admin/wcl", call[0]);
        try {
            return Transport.json().readTree(call[1]).get("script").asText();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private JsonNode commandAt(int index) {
        var call = calls.get(index);
        assertEquals("/v1/admin/commands", call[0]);
        try {
            return Transport.json().readTree(call[1]).get("commands").get(0);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void createDomainReturnsAUsableDomainHandle() {
        var pipes = client.createDomain("pipes");
        assertEquals("CREATE DOMAIN pipes", scriptAt(0));

        assertEquals("pipes", pipes.name());
        assertEquals("pipes.orders", pipes.feed("orders").name());
        assertEquals("pipes.orders", pipes.feed("pipes.orders").name());
    }

    @Test
    void domainScopedResourcesResolveInsideTheDomain() {
        var pipes = client.domain("pipes");

        pipes.createFeed("orders");
        assertEquals("CREATE FEED pipes.orders", scriptAt(0));

        pipes.createFeed("pipes.orders.deep");
        assertEquals("CREATE FEED pipes.orders.deep", scriptAt(1));

        pipes.createSubscription("audit", "orders");
        assertEquals(
                "CREATE SUBSCRIPTION pipes.audit FROM pipes.orders START AT BEGINNING",
                scriptAt(2));

        pipes.describeReader("audit");
        assertEquals("DESCRIBE READER pipes.audit", scriptAt(3));

        pipes.renameFeed("orders", "orders.v2");
        assertEquals("RENAME FEED pipes.orders TO pipes.orders.v2", scriptAt(4));

        pipes.dropSubscription("audit");
        assertEquals("DROP SUBSCRIPTION pipes.audit", scriptAt(5));

        pipes.seekReader("audit", ReaderStart.now());
        assertEquals("SEEK READER pipes.audit TO NOW", scriptAt(6));

        pipes.openWriterSession("ingest");
        assertEquals("OPEN WRITER SESSION pipes.ingest", scriptAt(7));
    }

    @Test
    void sessionHandlesBindResolvedNames() throws Exception {
        var shop = client.domain("shop");

        var session = shop.readerSession("fulfil").open(8);
        var open = Transport.json().readTree(calls.get(0)[1]);
        assertEquals("shop.fulfil", open.get("reader").asText());
        assertEquals(3, session.sessionEpoch());

        assertEquals("shop.fulfil", shop.subscription("fulfil").name());
        assertEquals("shop.fulfil", shop.scoped("fulfil").name());
    }

    @Test
    void forwardPipeResolvesEveryNameInsideTheDomain() {
        var pipes = client.domain("pipes");

        pipes.defineForwardPipe("relay", "audit", "orders.copy");
        var command = commandAt(0);
        assertEquals("define_pipe", command.get("command").asText());
        assertEquals("pipes.relay", command.get("name").asText());
        assertEquals("pipes.audit", command.get("subscription").asText());
        assertEquals("pipes.orders.copy", command.get("output_feed").asText());
    }

    @Test
    void crossDomainPipeKeepsAbsoluteReferences() {
        var purchasing = client.domain("purchasing");
        var warehouse = client.domain("warehouse");

        // A SubscriptionRef from another Domain travels with its
        // absolute name — the Pipe lives in warehouse but drives from
        // purchasing.fulfil.
        warehouse.defineForwardPipe(
                "pull", purchasing.subscription("fulfil"), Scoped.relative("inbound"));
        var pull = commandAt(0);
        assertEquals("warehouse.pull", pull.get("name").asText());
        assertEquals("purchasing.fulfil", pull.get("subscription").asText());
        assertEquals("warehouse.inbound", pull.get("output_feed").asText());

        // A Feed handle does the same for the output side.
        purchasing.defineForwardPipe(
                "relay", Scoped.relative("fulfil"), warehouse.feed("inbound"));
        var relay = commandAt(1);
        assertEquals("purchasing.relay", relay.get("name").asText());
        assertEquals("purchasing.fulfil", relay.get("subscription").asText());
        assertEquals("warehouse.inbound", relay.get("output_feed").asText());

        // Scoped.absolute is the handle-free explicit form.
        purchasing.defineForwardPipe("relay2",
                Scoped.relative("fulfil"), Scoped.absolute("warehouse.inbound"));
        var relay2 = commandAt(2);
        assertEquals("warehouse.inbound", relay2.get("output_feed").asText());
    }

    @Test
    void enrichAndWindowPipeEndpointsScopeTheSameWay() {
        var ops = client.domain("ops");
        var accounts = client.domain("accounts");

        ops.defineEnrichPipe("enrich",
                Scoped.relative("audit"),
                Scoped.relative("orders.enriched"),
                accounts.stateStore("customers"),
                dev.whitewater.model.PipeOperation.enrich(
                        dev.whitewater.model.Extractor.metadata("customer_id"),
                        null, null,
                        dev.whitewater.model.EnrichMiss.QUARANTINE),
                Scoped.relative("orders.quarantine"));
        var enrich = commandAt(0);
        assertEquals("ops.enrich", enrich.get("name").asText());
        assertEquals("ops.audit", enrich.get("subscription").asText());
        assertEquals("ops.orders.enriched", enrich.get("output_feed").asText());
        assertEquals("accounts.customers", enrich.get("output_store").asText());
        assertEquals("ops.orders.quarantine", enrich.get("quarantine_feed").asText());

        ops.defineWindowPipe("hourly",
                Scoped.relative("audit"),
                Scoped.relative("hourlyspend"),
                dev.whitewater.model.PipeOperation.window(
                        dev.whitewater.model.Time.h(1),
                        dev.whitewater.model.Extractor.recordKey(),
                        dev.whitewater.model.WindowAggregation.count(),
                        null, null, null, null, null, null, null),
                accounts.feed("emitted"));
        var window = commandAt(1);
        assertEquals("ops.hourly", window.get("name").asText());
        assertEquals("ops.hourlyspend", window.get("output_store").asText());
        assertEquals("accounts.emitted", window.get("emit_feed").asText());
    }

    @Test
    void stateStoreDeclarationResolvesTheSpecNameInsideTheDomain() {
        var app = client.domain("app");

        app.defineStateStore(
                StateStoreSpec.named("profiles").addKey("user_id", true),
                (String) null);
        var define = commandAt(0);
        assertEquals("define_state_store", define.get("command").asText());
        assertEquals("app.profiles", define.get("name").asText());
        assertEquals("manual", define.get("source").get("kind").asText());

        assertEquals("app.profiles", app.stateStore("profiles").name());
    }

    @Test
    void schemaAndIndexCallsResolveInsideTheDomain() {
        var app = client.domain("app");

        app.setFeedSchema("orders",
                new dev.whitewater.model.FeedSchema(null,
                        new dev.whitewater.model.SchemaDecl("json", null, null)));
        var setSchema = commandAt(0);
        assertEquals("set_schema", setSchema.get("command").asText());
        assertEquals("app.orders", setSchema.get("scope").get("name").asText());

        app.queryIndex("bycustomer", IndexPredicate.exact("u-1"));
        var call = calls.get(1);
        assertEquals("/v1/indexes/query", call[0]);
        try {
            assertEquals("app.bycustomer",
                    Transport.json().readTree(call[1]).get("index").asText());
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
