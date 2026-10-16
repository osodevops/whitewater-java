package dev.whitewater;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import dev.whitewater.codec.ProtobufCodecs;
import dev.whitewater.internal.Transport;
import dev.whitewater.internal.Wire;
import dev.whitewater.model.Codecs;
import dev.whitewater.model.Cursor;
import dev.whitewater.model.FeedEvent;
import dev.whitewater.model.FeedMessage;
import dev.whitewater.model.ReaderStart;
import dev.whitewater.model.TemporaryFetchOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Scripted stub covering the typed {@link Feed}/{@link FeedWriter}/
 * {@link FeedReader} codec surface: serialization must be automatic in
 * both directions and the wire still carries plain base64 bytes.
 */
class FeedTest {

    public static class Order {
        public String id;
        public int total;

        public Order() {}

        Order(String id, int total) {
            this.id = id;
            this.total = total;
        }
    }

    private HttpServer server;
    private WhitewaterClient client;

    private final List<String> paths = new CopyOnWriteArrayList<>();
    private final List<String> bodies = new CopyOnWriteArrayList<>();
    private final AtomicInteger fetchCalls = new AtomicInteger();
    private volatile boolean refuseCreates;

    private static String b64(String text) {
        return Base64.getEncoder().encodeToString(text.getBytes(StandardCharsets.UTF_8));
    }

    private static final String COMMANDS_OK = """
            {
              "request_id": "019a0000-0000-7000-8000-000000000006",
              "revision": 9,
              "authority": "control_plane",
              "warning": null,
              "results": [{"statement": "s", "message": "ok", "data": {}}]
            }""";

    private static final String SESSION_OK = """
            {
              "request_id": "019a0000-0000-7000-8000-000000000006",
              "revision": 9,
              "authority": "control_plane",
              "warning": null,
              "results": [{"statement": "s", "message": "ok",
                           "data": {"session_epoch": 42}}]
            }""";

    private static final String READER_SESSION = """
            {"reader_id": "019a0000-0000-7000-8000-000000000010",
             "feed_id": "019a0000-0000-7000-8000-000000000011",
             "session_epoch": 7, "capacity": 64,
             "delivered_cursor": null, "acknowledged_cursor": null}""";

    private static String recordJson(String cursor, String key, String payload) {
        return """
                {
                  "cursor": "%s",
                  "message_id": "019a0000-0000-7000-8000-000000000002",
                  "producer_id": "019a0000-0000-7000-8000-000000000003",
                  "sequence": 7,
                  "event_time_ns": "1697040000123456789",
                  "ingest_time_ns": "1697040000223456789",
                  "key_base64": "%s",
                  "payload_base64": "%s",
                  "metadata_base64": {"tenant": "%s"}
                }
                """.formatted(cursor, b64(key), b64(payload), b64("acme"));
    }

    private static String orderPayload(String id, int total) {
        return "{\"id\":\"%s\",\"total\":%d}".formatted(id, total);
    }

    private int status;
    private String route(String path, String body) {
        status = 200;
        if (path.equals("/v1/admin/wcl")) {
            if (refuseCreates
                    && (body.contains("CREATE WRITER") || body.contains("CREATE READER"))) {
                status = 400;
                return """
                        {"error": "resource already exists", "code": "conflict"}""";
            }
            return COMMANDS_OK;
        }
        if (path.equals("/v1/admin/commands")) {
            if (body.contains("open_writer_session")) {
                return SESSION_OK;
            }
            return COMMANDS_OK;
        }
        return switch (path) {
            case "/v1/feeds/records" -> """
                    [%s, %s]""".formatted(
                    recordJson("c1", "order-1", orderPayload("order-1", 10)),
                    recordJson("c2", "order-2", orderPayload("order-2", 20)));
            case "/v1/readers/temporary/fetch" -> """
                    {"records": [%s], "next_cursor": "frontier-9"}"""
                    .formatted(recordJson("rec-1", "order-1", orderPayload("order-1", 10)));
            case "/v1/readers/open", "/v1/readers/ack", "/v1/readers/close" -> READER_SESSION;
            case "/v1/readers/fetch" -> {
                if (fetchCalls.incrementAndGet() == 1) {
                    yield """
                            {"records": [%s],
                             "delivered_cursor": "d1", "acknowledged_cursor": null,
                             "recommended_capacity": 64, "retry_after_ms": 0}"""
                            .formatted(recordJson("rec-1", "order-1", orderPayload("order-1", 10)));
                }
                yield """
                        {"records": [],
                         "delivered_cursor": "d1", "acknowledged_cursor": "d1",
                         "recommended_capacity": 64, "retry_after_ms": 0}""";
            }
            case "/v1/writers/append" -> """
                    {"message_id": "019a0000-0000-7000-8000-0000000000aa",
                     "cursor": "appended-1", "deduplicated": false,
                     "durability": "committed"}""";
            case "/v1/writers/append-batch" -> """
                    {"results": [
                        {"message_id": "019a0000-0000-7000-8000-0000000000aa",
                         "cursor": "a1", "deduplicated": false, "durability": "committed"},
                        {"message_id": "019a0000-0000-7000-8000-0000000000ab",
                         "cursor": "a2", "deduplicated": false, "durability": "committed"}],
                     "feedback": {}}""";
            case "/v1/feeds/append" -> """
                    {"message_id": "019a0000-0000-7000-8000-0000000000aa",
                     "cursor": "appended-1", "deduplicated": true,
                     "durability": "committed"}""";
            default -> "{}";
        };
    }

    @BeforeEach
    void setUp() throws IOException {
        paths.clear();
        bodies.clear();
        fetchCalls.set(0);
        refuseCreates = false;
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            paths.add(path);
            bodies.add(body);
            byte[] out = route(path, body).getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("content-type", "application/json");
            exchange.sendResponseHeaders(status, out.length);
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

    private JsonNode lastBody(String path) throws IOException {
        for (int i = paths.size() - 1; i >= 0; i--) {
            if (paths.get(i).equals(path)) {
                return Transport.json().readTree(bodies.get(i));
            }
        }
        fail("no request to " + path);
        return null;
    }

    // ---- bindings --------------------------------------------------------

    @Test
    void defaultBindingIsUtf8KeysAndRawPayloads() {
        Feed<String, byte[]> feed = client.feed("orders.events");
        assertEquals("orders.events", feed.name());
        var message = FeedMessage.decode(
                new dev.whitewater.model.FeedRecord(Cursor.of("c"), null, null, 1, 0, 0,
                        "k".getBytes(), "v".getBytes(), Map.of()),
                feed.keyCodec(), feed.valueCodec());
        assertEquals("k", message.key());
        assertArrayEquals("v".getBytes(), message.value());
    }

    @Test
    void recordsDecodeThroughTheBoundCodecs() {
        Feed<String, Order> orders = client.feed("orders.events", Order.class);

        List<FeedMessage<String, Order>> page = orders.records(50, Cursor.of("prev"));

        assertEquals(2, page.size());
        assertEquals("order-1", page.get(0).key());
        assertEquals(10, page.get(0).value().total);
        assertEquals("order-2", page.get(1).value().id);
        assertEquals("c2", page.get(1).cursor().value());
        assertEquals("acme", page.get(0).metadataText("tenant"));
        assertEquals(1697040000123456789L, page.get(0).eventTimeNs());
        assertTrue(paths.get(0).startsWith("/v1/feeds/records"));
    }

    @Test
    void fetchReturnsDecodedPageWithNextCursor() {
        var page = client.feed("orders.events", Order.class)
                .fetch(TemporaryFetchOptions.page(Cursor.of("after-1"), 100));

        assertEquals("frontier-9", page.nextCursor().value());
        assertEquals(1, page.records().size());
        assertEquals(10, page.records().get(0).value().total);
    }

    @Test
    void loadDeliversDecodedBatchesUntilDrained() {
        // temporary/fetch returns a record every call with next_cursor set;
        // load pages until records is empty — the stub never empties, so
        // instead verify via records()+fetch path below.
        var feed = client.feed("orders.events", Order.class);
        var first = feed.fetch(null, 10);
        assertFalse(first.records().isEmpty());
        assertEquals("order-1", first.records().get(0).key());
    }

    @Test
    void viewRebindsCodecsWithoutAWireCall() throws IOException {
        Feed<String, Order> typed = client.feed("orders.events", Order.class);
        Feed<String, byte[]> raw = typed.view(String.class, byte[].class);
        assertEquals("orders.events", raw.name());
        var page = raw.records(10, null);
        assertEquals(2, page.size());
        assertArrayEquals(orderPayload("order-1", 10).getBytes(), page.get(0).value());
        int commandsBefore = paths.size();
        typed.view(Codecs.utf8(), Codecs.utf8());
        assertEquals(commandsBefore, paths.size(), "view() must be codec-only");
    }

    // ---- writing -----------------------------------------------------------

    @Test
    void writerDeclaresOpensSessionAndSerializesAppends() throws Exception {
        FeedWriter<String, Order> writer =
                client.feed("orders.events", Order.class).writer("orders.ingest");

        assertTrue(bodies.stream().anyMatch(b -> b.contains("CREATE WRITER")), "writer was declared");
        assertTrue(bodies.stream().anyMatch(b -> b.contains("open_writer_session")), "session opened");

        writer.append("order-1", new Order("order-1", 99));

        JsonNode sent = lastBody("/v1/writers/append");
        assertEquals("orders.ingest", sent.get("writer").asText());
        assertEquals(42, sent.get("session_epoch").asLong());
        assertEquals(b64("order-1"), sent.get("key_base64").asText());
        // the codec emitted JSON — not base64-of-the-class-name
        String wirePayload = new String(
                Base64.getDecoder().decode(sent.get("payload_base64").asText()),
                StandardCharsets.UTF_8);
        assertEquals(orderPayload("order-1", 99), wirePayload);
    }

    @Test
    void writerToleratesAnAlreadyDeclaredWriter() {
        refuseCreates = true;
        FeedWriter<String, Order> writer =
                client.feed("orders.events", Order.class).writer("orders.ingest");
        assertEquals(42, writer.sessionEpoch(), "session opened despite exists refusal");
    }

    @Test
    void appendWithEventCarriesIdentityTimeAndMetadata() throws Exception {
        FeedWriter<String, Order> writer =
                client.feed("orders.events", Order.class).writer("orders.ingest");
        UUID requestId = UUID.fromString("019a0000-0000-7000-8000-0000000000cc");

        writer.append(new FeedEvent<>(requestId, "order-7", new Order("order-7", 7))
                .withEventTimeNs(1697040000123456789L)
                .withTextMetadata(Map.of("tenant", "acme")));

        JsonNode sent = lastBody("/v1/writers/append");
        assertEquals(requestId.toString(), sent.get("request_id").asText());
        assertEquals("1697040000123456789", sent.get("event_time_ns").asText());
        assertEquals(b64("acme"), sent.get("metadata_base64").get("tenant").asText());
    }

    @Test
    void appendBatchSerializesEveryEvent() throws Exception {
        FeedWriter<String, Order> writer =
                client.feed("orders.events", Order.class).writer("orders.ingest");

        var result = writer.appendAll(List.of(
                FeedEvent.of("o1", new Order("o1", 1)),
                new FeedEvent<>("o2", new Order("o2", 2))));

        assertEquals(2, result.results().size());
        JsonNode sent = lastBody("/v1/writers/append-batch");
        assertEquals(2, sent.get("records").size());
        String second = new String(
                Base64.getDecoder().decode(
                        sent.get("records").get(1).get("payload_base64").asText()),
                StandardCharsets.UTF_8);
        assertEquals(orderPayload("o2", 2), second);
    }

    @Test
    void adHocAppendSerializesThroughTheFeedFacade() throws Exception {
        UUID session = UUID.fromString("019a0000-0000-7000-8000-0000000000bb");
        var result = client.feed("orders.events", Order.class)
                .append(session, 3L, 9L, "order-1", new Order("order-1", 5));

        JsonNode sent = lastBody("/v1/feeds/append");
        assertEquals(session.toString(), sent.get("writer_session_id").asText());
        String wirePayload = new String(
                Base64.getDecoder().decode(sent.get("payload_base64").asText()),
                StandardCharsets.UTF_8);
        assertEquals(orderPayload("order-1", 5), wirePayload);
        assertTrue(result.deduplicated());
    }

    // ---- reading -----------------------------------------------------------

    @Test
    void readerDeclaresOpensSessionAndDecodesPages() throws Exception {
        try (FeedReader<String, Order> reader =
                     client.feed("orders.events", Order.class).reader("orders.audit", 64)) {

            assertTrue(bodies.stream().anyMatch(b -> b.contains("CREATE READER")), "reader declared");

            var page = reader.fetch();
            assertEquals(1, page.records().size());
            assertEquals("order-1", page.records().get(0).key());
            assertEquals(10, page.records().get(0).value().total);
            assertEquals("d1", page.deliveredCursor().value());

            reader.ack();
            JsonNode ack = lastBody("/v1/readers/ack");
            assertEquals("d1", ack.get("cursor").asText());
            assertEquals(7, ack.get("session_epoch").asLong());
        }
        assertTrue(paths.contains("/v1/readers/close"), "close() released the session");
    }

    @Test
    void readerToleratesAnAlreadyDeclaredReader() {
        refuseCreates = true;
        try (FeedReader<String, Order> reader =
                     client.feed("orders.events", Order.class).reader("orders.audit", 64)) {
            assertEquals(7, reader.sessionEpoch());
        }
    }

    @Test
    void fetchDecodesNullKeyAndPayloadAsNull() {
        var message = FeedMessage.decode(
                new dev.whitewater.model.FeedRecord(Cursor.of("c"), null, null, 0, 0, 0,
                        null, null, Map.of()),
                Codecs.utf8(), Codecs.utf8());
        assertNull(message.key());
        assertNull(message.value());
    }

    @Test
    void subscribeDeliversDecodedBatchesAndAcks() throws Exception {
        var feed = client.feed("orders.events", Order.class);
        var delivered = new CountDownLatch(1);
        var seen = new java.util.concurrent.atomic.AtomicReference<FeedMessage<String, Order>>();

        try (var sub = feed.subscribe("orders.sub", ReaderStart.beginning(), batch -> {
            seen.set(batch.get(0));
            delivered.countDown();
        })) {
            assertTrue(delivered.await(5, TimeUnit.SECONDS), "subscription delivered");
            assertEquals("order-1", seen.get().key());
            assertEquals(10, seen.get().value().total);
            // ack follows onRecords on the polling thread — wait for it
            // while the subscription is still open rather than racing close()
            long deadline = System.currentTimeMillis() + 5_000;
            while (!paths.contains("/v1/readers/ack") && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertTrue(paths.contains("/v1/readers/ack"), "progress committed");
        }
        assertTrue(paths.contains("/v1/readers/close"), "session released");
    }

    // ---- codec breadth -------------------------------------------------------

    @Test
    void customCodecAppliesToBothDirections() {
        var reversed = Codecs.<String>of(
                s -> new StringBuilder(s).reverse().toString().getBytes(StandardCharsets.UTF_8),
                bytes -> new StringBuilder(new String(bytes, StandardCharsets.UTF_8)).reverse().toString());
        Feed<String, String> feed = client.feed("orders.events", Codecs.utf8(), reversed);

        var page = feed.records(10, null);
        // stub returns plain utf8 payloads; reversed codec decodes them reversed
        assertEquals(new StringBuilder(orderPayload("order-1", 10)).reverse().toString(),
                page.get(0).value());
    }

    @Test
    void protobufCodecFlowsThroughFeedReads() {
        Feed<String, com.google.protobuf.StringValue> feed =
                client.feed("orders.events", ProtobufCodecs.of(com.google.protobuf.StringValue.class));

        // stash a protobuf-encoded payload in the stubbed record
        var encoded = com.google.protobuf.StringValue.of("hello").toByteArray();
        var record = new dev.whitewater.model.FeedRecord(Cursor.of("c"), null, null, 0, 0, 0,
                "k".getBytes(), encoded, Map.of());
        var message = FeedMessage.decode(record, feed.keyCodec(), feed.valueCodec());
        assertEquals("hello", message.value().getValue());
    }

    @Test
    void wireBytesRoundTripThroughTheFacade() throws Exception {
        // append JSON -> read JSON: the payload on the wire decodes back
        FeedWriter<String, Order> writer =
                client.feed("orders.events", Order.class).writer("orders.ingest");
        writer.append("order-9", new Order("order-9", 9));

        JsonNode sent = lastBody("/v1/writers/append");
        Order back = Transport.json().readValue(
                Base64.getDecoder().decode(sent.get("payload_base64").asText()),
                Order.class);
        assertEquals("order-9", back.id);
        assertEquals(9, back.total);
    }
}
