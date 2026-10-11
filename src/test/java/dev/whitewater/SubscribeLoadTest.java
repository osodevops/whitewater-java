package dev.whitewater;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import dev.whitewater.internal.Transport;
import dev.whitewater.model.Cursor;
import dev.whitewater.model.FeedRecord;
import dev.whitewater.model.ReaderStart;
import dev.whitewater.model.RecordStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Scripted multi-path stub covering subscribe()'s session loop and load()'s paging. */
class SubscribeLoadTest {

    private HttpServer server;
    private WhitewaterClient client;

    private final List<String> paths = new CopyOnWriteArrayList<>();
    private final List<String> bodies = new CopyOnWriteArrayList<>();
    private final ConcurrentLinkedQueue<String> fetchPages = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<String> temporaryPages = new ConcurrentLinkedQueue<>();

    private static final String SESSION = """
            {
              "reader_id": "019a0000-0000-7000-8000-000000000004",
              "feed_id": "019a0000-0000-7000-8000-000000000005",
              "session_epoch": 3,
              "capacity": 128,
              "delivered_cursor": null,
              "acknowledged_cursor": null
            }""";

    private static final String WCL_OK = """
            {
              "request_id": "019a0000-0000-7000-8000-000000000006",
              "revision": 9,
              "authority": "control_plane",
              "warning": null,
              "results": [{"statement": "s", "message": "ok", "data": {}}]
            }""";

    private static String recordJson(String cursor) {
        return recordJson(cursor, "aw==");
    }

    private static String recordJson(String cursor, String keyBase64) {
        return """
                {
                  "cursor": "%s",
                  "message_id": "019a0000-0000-7000-8000-000000000002",
                  "producer_id": "019a0000-0000-7000-8000-000000000003",
                  "sequence": 7,
                  "event_time_ns": "1697040000123456789",
                  "ingest_time_ns": "1697040000223456789",
                  "key_base64": "%s",
                  "payload_base64": "dmFs",
                  "metadata_base64": {}
                }
                """.formatted(cursor, keyBase64);
    }

    private static String pageJson(String deliveredCursor, String... cursors) {
        var records = new StringBuilder();
        for (int i = 0; i < cursors.length; i++) {
            if (records.length() > 0) {
                records.append(',');
            }
            // Distinct keys so keyed stores keep every record in the page.
            records.append(recordJson(cursors[i],
                    java.util.Base64.getEncoder().encodeToString(("k-" + cursors[i]).getBytes())));
        }
        return """
                {"records": [%s], "delivered_cursor": %s, "acknowledged_cursor": null,
                 "recommended_capacity": 0, "retry_after_ms": 0}
                """.formatted(
                records, deliveredCursor == null ? "null" : "\"" + deliveredCursor + "\"");
    }

    private static String temporaryPageJson(String nextCursor, String... cursors) {
        var records = new StringBuilder();
        for (int i = 0; i < cursors.length; i++) {
            if (records.length() > 0) {
                records.append(',');
            }
            records.append(recordJson(cursors[i],
                    java.util.Base64.getEncoder().encodeToString(("k-" + cursors[i]).getBytes())));
        }
        return """
                {"records": [%s], "next_cursor": %s}
                """.formatted(
                records, nextCursor == null ? "null" : "\"" + nextCursor + "\"");
    }

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            paths.add(path);
            bodies.add(body);
            String response = route(path);
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

    private String route(String path) {
        return switch (path) {
            case "/v1/admin/wcl" -> WCL_OK;
            case "/v1/readers/open", "/v1/readers/ack", "/v1/readers/close" -> SESSION;
            case "/v1/readers/fetch" -> fetchPages.isEmpty()
                    ? pageJson(null)
                    : fetchPages.poll();
            case "/v1/readers/temporary/fetch" -> temporaryPages.isEmpty()
                    ? temporaryPageJson(null)
                    : temporaryPages.poll();
            default -> "{}";
        };
    }

    @AfterEach
    void tearDown() {
        server.stop(0);
    }

    @Test
    void loadPagesUntilEmptyAndFillsTheStore() throws Exception {
        temporaryPages.add(temporaryPageJson("c1", "r1"));
        temporaryPages.add(temporaryPageJson("c2", "r2"));
        temporaryPages.add(temporaryPageJson(null));

        var store = RecordStore.named("employee-store");
        long loaded = client.feeds().load("hr.employees", ReaderStart.beginning(), store);

        assertEquals(2, loaded);
        assertEquals(2, store.size());
        assertTrue(store.get("k-r1".getBytes(StandardCharsets.UTF_8)).isPresent());
        assertEquals(3, temporaryBodies().size());
        assertNull(firstBody().get("after"), "first page reads from the beginning");
        assertEquals("c1", bodyAt(1).get("after").asText(), "continues on next_cursor");
    }

    @Test
    void loadSeeksByEventTimeThenContinuesOnCursor() throws Exception {
        temporaryPages.add(temporaryPageJson("c1", "r1"));
        temporaryPages.add(temporaryPageJson(null));

        var store = RecordStore.named("s");
        client.feeds().load("hr.employees", ReaderStart.timestampNs(1697040000000000000L), store);

        assertEquals("1697040000000000000", firstBody().get("after_event_time_ns").asText());
        assertFalse(firstBody().has("after"), "start seek never combines after+event time");
        assertEquals("c1", bodyAt(1).get("after").asText());
    }

    @Test
    void loadFromNowYieldsNothing() throws Exception {
        temporaryPages.add(temporaryPageJson("tip-9"));
        var store = RecordStore.named("s");
        assertEquals(0, client.feeds().load("hr.employees", ReaderStart.now(), store));
        assertTrue(firstBody().get("new_only").asBoolean());
        assertEquals(1, temporaryBodies().size());
    }

    @Test
    void subscribeCreatesReaderDeliversAcksAndCloses() throws Exception {
        fetchPages.add(pageJson("d1", "r1"));
        var delivered = new CountDownLatch(1);
        var batches = new CopyOnWriteArrayList<List<FeedRecord>>();

        var sub = client.subscribe("hr-view", "hr.employees", ReaderStart.beginning(),
                records -> {
                    batches.add(records);
                    delivered.countDown();
                });
        assertTrue(delivered.await(5, TimeUnit.SECONDS), "handler receives the page");
        // The ack posts after the handler returns — wait for it before close.
        for (int i = 0; i < 100 && !paths.contains("/v1/readers/ack"); i++) {
            Thread.sleep(50);
        }
        sub.close();

        assertEquals(1, batches.get(0).size());
        long wclCalls = paths.stream().filter(p -> p.equals("/v1/admin/wcl")).count();
        assertEquals(1, wclCalls, "reader auto-provisioned once");
        assertTrue(paths.contains("/v1/readers/open"));
        assertTrue(paths.stream().anyMatch(p -> p.equals("/v1/readers/ack")),
                "delivered cursor acked after handling");
        assertTrue(paths.contains("/v1/readers/close"), "session released on close");
        JsonNode ack = Transport.json().readTree(
                bodies.get(paths.indexOf("/v1/readers/ack")));
        assertEquals("d1", ack.get("cursor").asText());
    }

    @Test
    void subscribeReusesAnExistingReaderAndReportsStaleSession() throws Exception {
        // Reader already declared: the CREATE call refuses with "exists".
        HttpServer refused = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var staleLatch = new CountDownLatch(1);
        var errors = new CopyOnWriteArrayList<WhitewaterException>();
        refused.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            String response;
            int status = 200;
            if (path.equals("/v1/admin/wcl")) {
                status = 400;
                response = "{\"error\": \"Reader hr-view already exists\"}";
            } else if (path.equals("/v1/readers/open")) {
                status = 400;
                response = "{\"error\": \"stale session epoch\", \"code\": \"stale_epoch\"}";
            } else {
                response = SESSION;
            }
            byte[] out = response.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, out.length);
            exchange.getResponseBody().write(out);
            exchange.close();
        });
        refused.start();
        try {
            var refusedClient = WhitewaterClient.builder()
                    .endpoints(URI.create("http://127.0.0.1:" + refused.getAddress().getPort()))
                    .apiKey("test-key")
                    .build();
            var sub = refusedClient.subscribe("hr-view", "hr.employees", ReaderStart.now(),
                    records -> {}, error -> {
                        errors.add(error);
                        staleLatch.countDown();
                    });
            assertTrue(staleLatch.await(5, TimeUnit.SECONDS));
            assertInstanceOf(WhitewaterException.StaleSession.class, sub.failure());
            assertFalse(sub.isRunning());
            sub.close();
        } finally {
            refused.stop(0);
        }
    }

    private JsonNode firstBody() throws IOException {
        return Transport.json().readTree(temporaryBodies().get(0));
    }

    private JsonNode bodyAt(int index) throws IOException {
        return Transport.json().readTree(temporaryBodies().get(index));
    }

    private List<String> temporaryBodies() {
        var result = new java.util.ArrayList<String>();
        for (int i = 0; i < paths.size(); i++) {
            if (paths.get(i).equals("/v1/readers/temporary/fetch")) {
                result.add(bodies.get(i));
            }
        }
        return result;
    }
}
