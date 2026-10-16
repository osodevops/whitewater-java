package dev.whitewater;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import dev.whitewater.internal.Transport;
import dev.whitewater.model.Extractor;
import dev.whitewater.model.Codecs;
import dev.whitewater.model.StateStoreSpec;
import dev.whitewater.model.StoreRow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Scripted stub covering the StateStore facade endpoints. */
class StateStoreTest {

    private HttpServer server;
    private WhitewaterClient client;

    private final List<String> paths = new CopyOnWriteArrayList<>();
    private final List<String> bodies = new CopyOnWriteArrayList<>();

    private static final String COMMANDS_OK = """
            {
              "request_id": "019a0000-0000-7000-8000-000000000006",
              "revision": 9,
              "authority": "control_plane",
              "warning": null,
              "results": [{"statement": "s", "message": "ok", "data": {}}]
            }""";

    private static final String MUTATE_OK = """
            {
              "store_id": "019a0000-0000-7000-8000-000000000007",
              "mutation_id": "019a0000-0000-7000-8000-000000000008",
              "sequence": 4,
              "deduplicated": false
            }""";

    private static String rowJson(String key, String payload) {
        return """
                {"key_base64": "%s", "payload_base64": "%s"}
                """.formatted(
                java.util.Base64.getEncoder().encodeToString(key.getBytes(StandardCharsets.UTF_8)),
                java.util.Base64.getEncoder().encodeToString(payload.getBytes(StandardCharsets.UTF_8)));
    }

    private String route(String path) {
        return switch (path) {
            case "/v1/admin/commands" -> COMMANDS_OK;
            case "/v1/state-stores/mutate", "/v1/state-stores/clear" -> MUTATE_OK;
            case "/v1/state-stores/lookup" -> """
                    {"found": true, "payload_base64": "cGF5bG9hZC0x",
                     "applied_cursor": "41", "applied_seq": 4, "committed_seq": 5,
                     "behind": true, "served_by": "n1"}""";
            case "/v1/state-stores/get-by" -> """
                    {"rows": [%s, %s], "applied_seq": 4, "committed_seq": 4,
                     "behind": false, "served_by": "n1"}"""
                    .formatted(rowJson("user-1", "p1"), rowJson("user-2", "p2"));
            case "/v1/state-stores/iterate" -> """
                    {"rows": [%s, %s], "pinned_seq": 4, "served_by": "n1"}"""
                    .formatted(rowJson("a", "pa"), rowJson("b", "pb"));
            default -> "{}";
        };
    }

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            paths.add(path);
            bodies.add(body);
            byte[] out = route(path).getBytes(StandardCharsets.UTF_8);
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

    private JsonNode lastBody(String path) throws IOException {
        for (int i = paths.size() - 1; i >= 0; i--) {
            if (paths.get(i).equals(path)) {
                return Transport.json().readTree(bodies.get(i));
            }
        }
        fail("no request to " + path);
        return null;
    }

    @Test
    void defineSendsTheTypedCommandWithDeclaredNamedKeys() throws Exception {
        var spec = StateStoreSpec.named("accounts.employees")
                .addKey("passport", true)
                .addKey("department", false);
        var store = client.stateStores().define(spec, null);

        assertEquals("accounts.employees", store.name());
        JsonNode sent = lastBody("/v1/admin/commands").get("commands").get(0);
        assertEquals("define_state_store", sent.get("command").asText());
        assertEquals("accounts.employees", sent.get("name").asText());
        assertEquals("manual", sent.get("source").get("kind").asText());
        assertEquals("passport", sent.get("named_keys").get(0).get("name").asText());
        assertTrue(sent.get("named_keys").get(0).get("unique").asBoolean());
        assertFalse(sent.get("named_keys").get(1).get("unique").asBoolean());
    }

    @Test
    void boundsNamedKeysAndRejectsDuplicates() {
        var spec = StateStoreSpec.named("accounts.ten");
        for (int i = 0; i < 10; i++) {
            spec.addKey("k" + i);
        }
        assertThrows(IllegalStateException.class, () -> spec.addKey("k10"));
        assertThrows(IllegalArgumentException.class,
                () -> StateStoreSpec.named("accounts.d").addKey("a").addKey("a"));
        assertThrows(IllegalArgumentException.class, () -> StateStoreSpec.named("accounts.b").addKey(" "));
    }

    @Test
    void defineFeedSourcedNamedKeysNeedExtractors() {
        // A feed-sourced named key without a projection is a declaration bug.
        var bare = StateStoreSpec.named("accounts.src").addKey("k");
        var refused = assertThrows(IllegalArgumentException.class,
                () -> client.stateStores().define(bare, "accounts.events"));
        assertTrue(refused.getMessage().contains("require an extractor"));

        // An extractor on a manual store is refused — puts carry the values.
        var manual = StateStoreSpec.named("accounts.m").addKey("k", false, Extractor.metadata("pp"));
        var refusedManual = assertThrows(IllegalArgumentException.class,
                () -> client.stateStores().define(manual, null));
        assertTrue(refusedManual.getMessage().contains("manual stores"));
    }

    @Test
    void defineFeedSourcedEmitsExtractors() throws Exception {
        var spec = StateStoreSpec.named("accounts.tagged")
                .addKey("passport", true, Extractor.metadata("pp"))
                .addKey("department", false, Extractor.json("dept"));
        client.stateStores().define(spec, "accounts.events");

        JsonNode sent = lastBody("/v1/admin/commands").get("commands").get(0);
        assertEquals("feed", sent.get("source").get("kind").asText());
        assertEquals("accounts.events", sent.get("source").get("feed").asText());
        JsonNode passport = sent.get("named_keys").get(0).get("extractor");
        assertEquals("metadata", passport.get("kind").asText());
        assertEquals("pp", passport.get("name").asText());
        JsonNode department = sent.get("named_keys").get(1).get("extractor");
        assertEquals("projection", department.get("kind").asText());
        assertEquals("json", department.get("extractor").asText());
        assertEquals(1, department.get("version").asInt());
        assertEquals("dept", department.get("field").asText());
    }

    @Test
    void specIsDeclarationOnly() {
        // StateStoreSpec carries name + named keys — data operations
        // live on the bound StateStore facade, so an unbound spec has
        // nothing to call (compile-time enforced, no runtime check needed).
        var spec = StateStoreSpec.named("accounts.unbound").addKey("k");
        assertEquals(1, spec.namedKeys().size());
    }

    @Test
    void putEncodesTypedNamedValuesAndDeletePostsTheOp() throws Exception {
        var store = client.stateStore("accounts.employees", Codecs.utf8());
        var put = store.put("user-1", "profile-1", Map.of(
                "passport", "P111",
                "salary", 92000L,
                "big", BigInteger.valueOf(7),
                "active", true,
                "badge", new byte[]{1, 2}));
        assertEquals(4, put.sequence());
        assertFalse(put.deduplicated());

        JsonNode sent = lastBody("/v1/state-stores/mutate");
        assertEquals("accounts.employees", sent.get("store").asText());
        assertFalse(sent.get("mutation_id").asText().isBlank());
        JsonNode op = sent.get("op");
        assertEquals("put_row", op.get("kind").asText());
        assertEquals("user-1", new String(
                java.util.Base64.getDecoder().decode(op.get("key_base64").asText())));
        var named = new java.util.HashMap<String, JsonNode>();
        op.get("named").forEach(e -> named.put(e.get("name").asText(), e.get("values").get(0)));
        assertEquals("text", named.get("passport").get("kind").asText());
        assertEquals("P111", named.get("passport").get("value").asText());
        assertEquals("i64", named.get("salary").get("kind").asText());
        assertEquals(92000L, named.get("salary").get("value").asLong());
        assertEquals("i64", named.get("big").get("kind").asText());
        assertEquals("bool", named.get("active").get("kind").asText());
        assertTrue(named.get("active").get("value").asBoolean());
        assertEquals("bytes", named.get("badge").get("kind").asText());
        assertEquals("AQI=", named.get("badge").get("value_base64").asText());

        store.delete("user-1");
        JsonNode deleteOp = lastBody("/v1/state-stores/mutate").get("op");
        assertEquals("delete_row", deleteOp.get("kind").asText());
    }

    @Test
    void floatsAreRefusedAsNamedValues() {
        var store = client.stateStore("accounts.employees", Codecs.utf8());
        assertThrows(IllegalArgumentException.class,
                () -> store.put("k", "v", Map.of("score", 1.5)));
    }

    @Test
    void getReturnsTheRowWithFreshnessEvidence() throws Exception {
        var store = client.stateStore("accounts.employees", Codecs.utf8());
        StoreRow<String, String> row = store.get("user-1", true, 1000L);
        assertNotNull(row);
        assertEquals("payload-1", row.payload());
        assertEquals("41", row.appliedCursor());
        assertEquals(5, row.freshness().committedSeq());
        assertTrue(row.freshness().behind());
        JsonNode sent = lastBody("/v1/state-stores/lookup");
        assertTrue(sent.get("strict").asBoolean());
        assertEquals(1000, sent.get("wait_ms").asLong());
    }

    @Test
    void convenienceGetReadsFastAndGetStrictBoundsTheWait() throws Exception {
        var store = client.stateStore("accounts.employees", Codecs.utf8());
        store.get("user-1");                                    // the fast path
        JsonNode fast = lastBody("/v1/state-stores/lookup");
        assertFalse(fast.get("strict").asBoolean());
        assertTrue(fast.get("wait_ms") == null || fast.get("wait_ms").isNull());

        store.getStrict("user-1", java.time.Duration.ofSeconds(2));
        JsonNode strict = lastBody("/v1/state-stores/lookup");
        assertTrue(strict.get("strict").asBoolean());
        assertEquals(2000, strict.get("wait_ms").asLong());
    }

    @Test
    void rowRefreshReReadsThroughTheBoundStore() throws Exception {
        var store = client.stateStore("accounts.employees", Codecs.utf8());
        var row = store.get("user-1");
        assertNotNull(row);

        var again = row.refresh();                       // fast re-read, same key
        assertEquals("payload-1", again.payload());
        JsonNode fast = lastBody("/v1/state-stores/lookup");
        assertFalse(fast.get("strict").asBoolean());
        assertEquals("user-1", new String(java.util.Base64.getDecoder()
                .decode(fast.get("key_base64").asText())));

        var fresh = row.refreshStrict(java.time.Duration.ofSeconds(3));
        assertNotNull(fresh);
        JsonNode strict = lastBody("/v1/state-stores/lookup");
        assertTrue(strict.get("strict").asBoolean());
        assertEquals(3000, strict.get("wait_ms").asLong());
    }

    @Test
    void rowsFromGetByAndPagesRefreshToo() throws Exception {
        var store = client.stateStore("accounts.employees", Codecs.utf8());
        var row = store.getBy("department", "eng").rows().get(0);
        assertEquals("user-1", row.key());
        var again = row.refresh();
        assertNotNull(again);
        assertEquals("user-1", new String(java.util.Base64.getDecoder()
                .decode(lastBody("/v1/state-stores/lookup").get("key_base64").asText())));
    }

    @Test
    void detachedRowsRefuseToRefresh() {
        var row = new StoreRow<String, String>("k", "v");
        var thrown = assertThrows(IllegalStateException.class, row::refresh);
        assertTrue(thrown.getMessage().contains("detached"));
    }

    @Test
    void jsonBoundStoresDecodeOrFailWithTheTypeNamed() throws Exception {
        record Payload(String text) {}
        var store = client.stateStore("accounts.employees", Codecs.json(Payload.class));
        // The stub's getBy rows carry "p1"/"p2" — not Payload JSON —
        // so decoding surfaces the bound type, not a byte[] leak.
        var thrown = assertThrows(IllegalArgumentException.class,
                () -> store.getBy("department", "eng"));
        assertTrue(thrown.getMessage().contains("Payload"));
    }

    @Test
    void getByPostsTheNamedKeyLookupAndDecodesRows() throws Exception {
        var store = client.stateStore("accounts.employees", Codecs.utf8());
        var result = store.getBy("department", "eng", 50);
        assertEquals(2, result.rows().size());
        assertEquals("user-1", result.rows().get(0).key());
        JsonNode sent = lastBody("/v1/state-stores/get-by");
        assertEquals("department", sent.get("named_key").asText());
        assertEquals("text", sent.get("values").get(0).get("kind").asText());
        assertEquals("eng", sent.get("values").get(0).get("value").asText());
        assertEquals(50, sent.get("limit").asInt());
    }

    @Test
    void iteratePagesUntilNoNextKey() throws Exception {
        var pages = new AtomicInteger();
        var iteratePaths = new CopyOnWriteArrayList<JsonNode>();
        server.removeContext("/");
        server.createContext("/", exchange -> {
            JsonNode sent = Transport.json().readTree(
                    exchange.getRequestBody().readAllBytes());
            iteratePaths.add(sent);
            String response = pages.getAndIncrement() == 0
                    ? """
                      {"rows": [%s], "next_key_base64": "%s", "pinned_seq": 9, "served_by": "n1"}"""
                            .formatted(rowJson("a", "pa"),
                                    java.util.Base64.getEncoder().encodeToString("a".getBytes()))
                    : """
                      {"rows": [%s], "pinned_seq": 9, "served_by": "n1"}"""
                            .formatted(rowJson("b", "pb"));
            byte[] out = response.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("content-type", "application/json");
            exchange.sendResponseHeaders(200, out.length);
            exchange.getResponseBody().write(out);
            exchange.close();
        });

        var store = client.stateStore("accounts.employees", Codecs.utf8());
        var keys = new CopyOnWriteArrayList<String>();
        store.iterate(1, row -> keys.add(row.key()));
        assertEquals(List.of("a", "b"), keys);
        assertEquals(2, iteratePaths.size());
        assertEquals("a", new String(java.util.Base64.getDecoder()
                .decode(iteratePaths.get(1).get("after_key_base64").asText())));
    }

    @Test
    void clearPostsAJournaledMutationAndDropIssuesTheCommand() throws Exception {
        var store = client.stateStore("accounts.employees", Codecs.utf8());
        var cleared = store.clear();
        assertEquals(4, cleared.sequence());
        JsonNode sent = lastBody("/v1/state-stores/clear");
        assertEquals("accounts.employees", sent.get("store").asText());
        assertFalse(sent.get("mutation_id").asText().isBlank());

        store.drop();
        JsonNode command = lastBody("/v1/admin/commands").get("commands").get(0);
        assertEquals("drop", command.get("command").asText());
        assertEquals("state_store", command.get("kind").asText());
        assertEquals("accounts.employees", command.get("name").asText());
    }
}
