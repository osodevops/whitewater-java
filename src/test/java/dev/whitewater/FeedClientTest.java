package dev.whitewater;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import dev.whitewater.internal.Transport;
import dev.whitewater.model.Cursor;
import dev.whitewater.model.FeedAppend;
import dev.whitewater.model.FeedRecord;
import dev.whitewater.model.TemporaryFetchOptions;
import dev.whitewater.model.TemporaryFetchPage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class FeedClientTest {

    private HttpServer server;
    private WhitewaterClient client;
    private volatile String capturedPath;
    private volatile String capturedBody;
    private volatile String capturedAuth;
    private volatile String cannedResponse = "{}";
    private volatile int cannedStatus = 200;

    @BeforeEach
    void setUp() throws IOException {
        capturedPath = null;
        capturedBody = null;
        capturedAuth = null;
        cannedResponse = "{}";
        cannedStatus = 200;
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            capturedPath = exchange.getRequestURI().toString();
            capturedAuth = exchange.getRequestHeaders().getFirst("authorization");
            capturedBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            byte[] out = cannedResponse.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("content-type", "application/json");
            exchange.sendResponseHeaders(cannedStatus, out.length);
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

    private static String recordJson(String cursor) {
        return """
                {
                  "cursor": "%s",
                  "message_id": "019a0000-0000-7000-8000-000000000002",
                  "producer_id": "019a0000-0000-7000-8000-000000000003",
                  "sequence": 7,
                  "event_time_ns": "1697040000123456789",
                  "ingest_time_ns": "1697040000223456789",
                  "key_base64": "aw==",
                  "payload_base64": "dmFs",
                  "metadata_base64": {"src": "dGVzdA=="}
                }
                """.formatted(cursor);
    }

    @Test
    void recordsBuildsEncodedGetPathAndParsesArray() throws Exception {
        cannedResponse = "[" + recordJson("cursor-1") + "]";
        List<FeedRecord> records = client.feeds().records("orders.created", 50, Cursor.of("prev cursor"));
        assertEquals(1, records.size());
        assertEquals("cursor-1", records.get(0).cursor().value());
        assertEquals(1697040000223456789L, records.get(0).ingestTimeNs());
        assertEquals("Bearer test-key", capturedAuth);
        assertTrue(capturedPath.startsWith("/v1/feeds/records?feed=orders.created"),
                capturedPath);
        assertTrue(capturedPath.contains("limit=50"), capturedPath);
        assertTrue(capturedPath.contains("after=prev+cursor"), capturedPath);
    }

    @Test
    void recordsOmitsOptionalQueryParams() {
        cannedResponse = "[]";
        client.feeds().records("orders.created", null, null);
        assertEquals("/v1/feeds/records?feed=orders.created", capturedPath);
    }

    @Test
    void temporaryFetchSerializesFlagsAndParsesPage() throws Exception {
        cannedResponse = """
                {"records": [%s], "next_cursor": "frontier-9"}
                """.formatted(recordJson("rec-1"));
        TemporaryFetchPage page = client.feeds().temporaryFetch("orders.created",
                new TemporaryFetchOptions(Cursor.of("after-1"), 100, null, true, 1500L, null));
        JsonNode body = Transport.json().readTree(capturedBody);
        assertEquals("/v1/readers/temporary/fetch", capturedPath);
        assertEquals("orders.created", body.get("feed").asText());
        assertEquals("after-1", body.get("after").asText());
        assertEquals(100, body.get("limit").asInt());
        assertTrue(body.get("tail").asBoolean());
        assertEquals(1500, body.get("wait_ms").asLong());
        assertFalse(body.has("new_only"), "absent options must not be sent");
        assertFalse(body.has("after_event_time_ns"));
        assertEquals("frontier-9", page.nextCursor().value());
        assertEquals(1, page.records().size());
        assertEquals("rec-1", page.records().get(0).cursor().value());
    }

    @Test
    void temporaryFetchParsesNullNextCursor() {
        cannedResponse = """
                {"records": [], "next_cursor": null}
                """;
        TemporaryFetchPage page = client.feeds().temporaryFetch("orders.created",
                TemporaryFetchOptions.atEnd());
        assertNull(page.nextCursor());
        assertTrue(page.records().isEmpty());
    }

    @Test
    void temporaryFetchSendsEventTimeSeekAsDecimalString() throws Exception {
        cannedResponse = """
                {"records": [], "next_cursor": "c"}
                """;
        client.feeds().temporaryFetch("orders.created",
                new TemporaryFetchOptions(null, null, null, null, null, -9223372036854775808L));
        JsonNode body = Transport.json().readTree(capturedBody);
        assertEquals("-9223372036854775808", body.get("after_event_time_ns").asText());
    }

    @Test
    void appendSendsClientAppendShapeAndParsesResult() throws Exception {
        cannedResponse = """
                {"message_id": "019a0000-0000-7000-8000-0000000000aa",
                 "cursor": "appended-1", "deduplicated": true, "durability": "committed"}
                """;
        UUID session = UUID.fromString("019a0000-0000-7000-8000-0000000000bb");
        UUID requestId = UUID.fromString("019a0000-0000-7000-8000-0000000000cc");
        var result = client.feeds().append("orders.created", new FeedAppend(
                requestId, session, 3L, 9L,
                "key".getBytes(), "val".getBytes(), 1697040000123456789L,
                Map.of("h", "v".getBytes())));
        JsonNode body = Transport.json().readTree(capturedBody);
        assertEquals("/v1/feeds/append", capturedPath);
        assertEquals(requestId.toString(), body.get("request_id").asText());
        assertEquals("orders.created", body.get("feed").asText());
        assertEquals(session.toString(), body.get("writer_session_id").asText());
        assertEquals(3, body.get("writer_epoch").asLong());
        assertEquals(9, body.get("sequence").asLong());
        assertEquals("1697040000123456789", body.get("event_time_ns").asText());
        assertEquals("a2V5", body.get("key_base64").asText());
        assertEquals("dg==", body.get("metadata_base64").get("h").asText());
        assertEquals("appended-1", result.cursor().value());
        assertTrue(result.deduplicated());
    }

    @Test
    void commandsPostTypedCommandArray() throws Exception {
        cannedResponse = """
                {"request_id": "019a0000-0000-7000-8000-0000000000dd",
                 "revision": 4, "authority": "control_plane", "warning": "",
                 "results": [{"statement": "show domains", "message": "ok",
                              "data": [{"name": "ops", "space_id": "d1"}]}]}
                """;
        var execution = client.admin().commands(List.of(
                Map.of("command", "show", "kind", "domains")));
        JsonNode body = Transport.json().readTree(capturedBody);
        assertEquals("/v1/admin/commands", capturedPath);
        assertEquals("show", body.get("commands").get(0).get("command").asText());
        assertEquals("domains", body.get("commands").get(0).get("kind").asText());
        assertTrue(body.has("request_id"));
        assertEquals(4, execution.revision());
        assertEquals("control_plane", execution.authority());
        assertEquals("ops", execution.results().get(0).data().get(0).get("name").asText());
    }

    @Test
    void refusalMapsToTypedException() {
        cannedStatus = 400;
        cannedResponse = """
                {"error": "Feed does not exist", "code": "not_found"}
                """;
        var ex = assertThrows(WhitewaterException.Refused.class,
                () -> client.feeds().records("missing.feed", null, null));
        assertEquals(400, ex.status());
        assertEquals("not_found", ex.code());
    }
}
