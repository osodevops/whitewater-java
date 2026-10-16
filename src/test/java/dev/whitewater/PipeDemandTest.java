package dev.whitewater;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import dev.whitewater.model.IndexPredicate;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Scripted stub covering the Pipe lifecycle facade, typed demand
 * observability, and index queries against recorded wire shapes.
 */
class PipeDemandTest {

    private HttpServer server;
    private WhitewaterClient client;
    private final List<String> paths = new CopyOnWriteArrayList<>();
    private final List<String> bodies = new CopyOnWriteArrayList<>();

    private static final String PIPE_ROW = """
            {
              "name": "orders.relay",
              "stage": "paused",
              "subscription_id": "019a0000-0000-7000-8000-000000000020",
              "operation": {"operation": "forward"},
              "output_feed_id": "019a0000-0000-7000-8000-000000000021",
              "output_store_id": null,
              "status": "active",
              "created_at_ns": 42
            }""";

    private static String commandsWith(String dataJson) {
        return """
                {
                  "request_id": "019a0000-0000-7000-8000-000000000006",
                  "revision": 9,
                  "authority": "control_plane",
                  "warning": null,
                  "results": [{"statement": "s", "message": "ok", "data": %s}]
                }""".formatted(dataJson);
    }

    private String route(String path, String body) {
        if (path.equals("/v1/admin/commands")) {
            if (body.contains("\"describe\"")) {
                return commandsWith(PIPE_ROW);
            }
            if (body.contains("\"show\"")) {
                return commandsWith("[" + PIPE_ROW + "]");
            }
            return commandsWith("{}");
        }
        return switch (path) {
            case "/v1/admin/demand/feeds" -> """
                    {
                      "feeds": [{
                        "feed": "orders.events", "domain": "orders",
                        "appended_records": 1500, "appended_bytes": 2048000,
                        "read_pages": 42
                      }],
                      "domains": {"orders": {"feeds": 1, "appended_records": 1500}}
                    }""";
            case "/v1/admin/demand/hot-keys" -> """
                    {
                      "hot_keys": [{
                        "feed": "orders.events", "share_bps": 9000,
                        "window_records": 900, "window_bytes": 12000
                      }]
                    }""";
            case "/v1/admin/demand/readers", "/v1/admin/demand/subscriptions",
                    "/v1/admin/demand/indexes", "/v1/admin/demand/pipes" -> """
                    {"%s": [], "domains": {}}"""
                    .formatted(path.substring(path.lastIndexOf('/') + 1));
            case "/v1/indexes/query" -> """
                    {
                      "index": "crm.byregion",
                      "index_id": "019a0000-0000-7000-8000-000000000030",
                      "generation": 2,
                      "rows": [{
                        "key_base64": "YWNjdC0x",
                        "payload_base64": "eyJ0aWVyIjoiZ29sZCJ9"
                      }],
                      "applied_cursor": "idx-cursor",
                      "target_cursor": "tip-cursor",
                      "behind": false, "truncated": false,
                      "served_by": "node-1"
                    }""";
            default -> "{}";
        };
    }

    @BeforeEach
    void setUp() throws IOException {
        paths.clear();
        bodies.clear();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            String requestBody = new String(
                    exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            paths.add(path);
            bodies.add(requestBody);
            byte[] out = route(path, requestBody).getBytes(StandardCharsets.UTF_8);
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

    // ---- Pipe lifecycle -----------------------------------------------------

    @Test
    void pipeDescribeDecodesTheCatalogRow() {
        var info = client.pipe("orders.relay").describe();
        assertEquals("orders.relay", info.name());
        assertEquals("paused", info.stage());
        assertEquals("forward", info.operation().path("operation").asText());
        assertNotNull(info.outputFeedId());
        assertNull(info.outputStoreId());
        assertEquals("active", info.status());
        assertEquals(42, info.createdAtNs());
        var command = bodies.get(0);
        assertTrue(command.contains("\"describe\""));
        assertTrue(command.contains("\"pipe\""));
    }

    @Test
    void pipeLifecycleCommandsPostTheTypedVerbs() {
        var pipe = client.pipe("orders.relay");
        pipe.pause();
        pipe.resume();
        pipe.drop();
        assertEquals(3, bodies.size());
        assertTrue(bodies.get(0).contains("pause_pipe"));
        assertTrue(bodies.get(1).contains("resume_pipe"));
        assertTrue(bodies.get(2).contains("\"drop\""));
        assertTrue(bodies.get(2).contains("\"pipe\""));
        assertTrue(paths.stream().allMatch("/v1/admin/commands"::equals));
    }

    @Test
    void pipesListsTheCatalog() {
        var pipes = client.pipes();
        assertEquals(1, pipes.size());
        assertEquals("orders.relay", pipes.get(0).name());
        assertEquals("paused", pipes.get(0).stage());
        assertTrue(bodies.get(0).contains("\"show\""));
        assertTrue(bodies.get(0).contains("\"pipes\""));
    }

    // ---- demand observability ------------------------------------------------

    @Test
    void demandFeedsDecodesRowsAndDomainRollups() {
        var report = client.demandFeeds();
        assertEquals(1, report.rows().size());
        var row = report.rows().get(0);
        assertEquals("orders.events", row.name());
        assertEquals("orders", row.domain());
        assertEquals(1500L, row.counter("appended_records"));
        assertEquals(42L, row.counter("read_pages"));
        assertNull(row.counter("nonsense"));
        assertEquals(1, report.domain("orders").path("feeds").asInt());
        assertEquals("/v1/admin/demand/feeds", paths.get(0));
    }

    @Test
    void demandEndpointsCoverEveryResourceKind() {
        assertTrue(client.demandReaders().rows().isEmpty());
        assertTrue(client.demandSubscriptions().rows().isEmpty());
        assertTrue(client.demandIndexes().rows().isEmpty());
        assertTrue(client.demandPipes().rows().isEmpty());
        assertEquals("/v1/admin/demand/readers", paths.get(0));
        assertEquals("/v1/admin/demand/subscriptions", paths.get(1));
        assertEquals("/v1/admin/demand/indexes", paths.get(2));
        assertEquals("/v1/admin/demand/pipes", paths.get(3));
    }

    @Test
    void hotKeysDecodeDominantKeyEvidence() {
        var keys = client.hotKeys();
        assertEquals(1, keys.size());
        assertEquals("orders.events", keys.get(0).feed());
        assertEquals(9000L, keys.get(0).shareBps());
        assertEquals(900L, keys.get(0).windowRecords());
        assertEquals("/v1/admin/demand/hot-keys", paths.get(0));
    }

    // ---- index queries --------------------------------------------------------

    @Test
    void queryIndexFlattensModeBoundsAndDecodesRows() {
        var page = client.queryIndex(
                "crm.byregion",
                IndexPredicate.range(
                        List.of("emea"),
                        IndexPredicate.IndexBound.inclusive(0),
                        IndexPredicate.IndexBound.exclusive(99)),
                50,
                true,
                null);
        assertFalse(page.behind());
        assertEquals(2, page.generation());
        assertEquals(1, page.rows().size());
        assertEquals("acct-1", new String(page.rows().get(0).key(), StandardCharsets.UTF_8));
        assertEquals("node-1", page.servedBy());
        var body = bodies.get(0);
        assertTrue(body.contains("\"mode\":\"range\""));
        assertTrue(body.contains("\"value\":\"emea\"") || body.contains("\"kind\":\"text\""));
        assertTrue(body.contains("\"inclusive\":false"));
        assertEquals("/v1/indexes/query", paths.get(0));
    }

    @Test
    void queryIndexExactCarriesEveryField() {
        client.queryIndex(
                "crm.byregion",
                IndexPredicate.exact("emea", 42),
                null,
                false,
                null);
        var body = bodies.get(0);
        assertTrue(body.contains("\"mode\":\"exact\""));
        assertTrue(body.contains("\"values\""));
        assertTrue(body.contains("\"strict\":false"));
    }
}
