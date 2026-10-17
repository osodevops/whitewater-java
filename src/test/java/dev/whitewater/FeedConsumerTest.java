package dev.whitewater;

import com.sun.net.httpserver.HttpServer;
import dev.whitewater.model.FeedMessage;
import dev.whitewater.model.ReaderStart;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Scripted stub covering the supervised consumer loop: per-record
 * delivery, ack-after-handler, session-death restart, startup
 * fail-fast, and graceful close — plus the store lookup ergonomics.
 */
class FeedConsumerTest {

    private HttpServer server;
    private WhitewaterClient client;
    private final List<String> paths = new CopyOnWriteArrayList<>();
    private final List<String> bodies = new CopyOnWriteArrayList<>();
    private final AtomicInteger fetchCalls = new AtomicInteger();
    private final AtomicInteger openCalls = new AtomicInteger();
    private final AtomicInteger lookupCalls = new AtomicInteger();
    private volatile boolean killFirstSession;
    private volatile boolean pageAcked;
    private volatile boolean refuseCreates;

    private static String b64(String text) {
        return Base64.getEncoder().encodeToString(text.getBytes(StandardCharsets.UTF_8));
    }

    private static final String WCL_OK = """
            {"request_id": "019a0000-0000-7000-8000-000000000006",
             "revision": 9, "authority": "control_plane", "warning": null,
             "results": [{"statement": "s", "message": "ok", "data": {}}]}""";

    private static final String READER_SESSION = """
            {"reader_id": "019a0000-0000-7000-8000-000000000010",
             "feed_id": "019a0000-0000-7000-8000-000000000011",
             "session_epoch": 7, "capacity": 64,
             "delivered_cursor": null, "acknowledged_cursor": null}""";

    private static String recordJson(String cursor, String key, String payload) {
        return """
                {"cursor": "%s",
                 "message_id": "019a0000-0000-7000-8000-000000000002",
                 "producer_id": "019a0000-0000-7000-8000-000000000003",
                 "sequence": 7,
                 "event_time_ns": "1697040000123456789",
                 "ingest_time_ns": "1697040000223456789",
                 "key_base64": "%s",
                 "payload_base64": "%s",
                 "metadata_base64": {}}
                """.formatted(cursor, b64(key), b64(payload));
    }

    private int status;
    private String route(String path, String body) {
        status = 200;
        if (path.equals("/v1/admin/wcl")) {
            if (refuseCreates) {
                status = 403;
                return """
                        {"error": "grant denied", "code": "auth.denied"}""";
            }
            return WCL_OK;
        }
        return switch (path) {
            case "/v1/readers/open", "/v1/readers/close" -> {
                if (path.endsWith("/open")) {
                    openCalls.incrementAndGet();
                }
                yield READER_SESSION;
            }
            case "/v1/readers/ack" -> {
                if (body.contains("\"d1\"")) {
                    pageAcked = true;
                }
                yield READER_SESSION;
            }
            case "/v1/readers/fetch" -> {
                int call = fetchCalls.incrementAndGet();
                if (killFirstSession && call == 2) {
                    status = 409;
                    yield """
                            {"error": "session fenced", "code": "stale_epoch"}""";
                }
                if (!pageAcked) {
                    yield """
                            {"records": [%s, %s],
                             "delivered_cursor": "d1",
                             "acknowledged_cursor": null,
                             "recommended_capacity": 64, "retry_after_ms": 0}"""
                            .formatted(
                                    recordJson("r1", "user-1", "payload-1"),
                                    recordJson("r2", "user-2", "payload-2"));
                }
                yield """
                        {"records": [],
                         "delivered_cursor": "d2", "acknowledged_cursor": "d2",
                         "recommended_capacity": 64, "retry_after_ms": 0}""";
            }
            case "/v1/state-stores/lookup" -> {
                int call = lookupCalls.incrementAndGet();
                yield switch (call) {
                    case 1 -> """
                            {"found": true,
                             "payload_base64": "%s",
                             "applied_cursor": "sc1"}""".formatted(b64("42"));
                    case 2 -> """
                            {"found": false}""";
                    default -> """
                            {"found": true,
                             "payload_base64": "%s",
                             "applied_cursor": "sc1"}""".formatted(b64("7"));
                };
            }
            case "/v1/state-stores/get-by" -> """
                    {"rows": [{"key_base64": "%s", "payload_base64": "%s"},
                              {"key_base64": "%s", "payload_base64": "%s"}],
                      "applied_cursor": "sc1"}"""
                    .formatted(b64("k1"), b64("first"), b64("k2"), b64("second"));
            default -> "{}";
        };
    }

    @BeforeEach
    void setUp() throws IOException {
        paths.clear();
        bodies.clear();
        fetchCalls.set(0);
        openCalls.set(0);
        lookupCalls.set(0);
        killFirstSession = false;
        refuseCreates = false;
        pageAcked = false;
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            String requestBody = new String(
                    exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            paths.add(path);
            bodies.add(requestBody);
            byte[] out = route(path, requestBody).getBytes(StandardCharsets.UTF_8);
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

    private Feed<String, String> feed() {
        return client.feed("users.updates", String.class);
    }

    private void awaitCount(AtomicInteger counter, int target) throws InterruptedException {
        for (int i = 0; i < 200 && counter.get() < target; i++) {
            Thread.sleep(10);
        }
    }

    // ---- the supervised loop -------------------------------------------------

    @Test
    void deliversRecordsIndividuallyAndAcksThePage() throws Exception {
        var delivered = new ConcurrentLinkedQueue<FeedMessage<String, String>>();
        var consumer = feed().consumer("enricher")
                .onRecord(delivered::add);
        Thread thread = consumer.runAsync();
        awaitCount(fetchCalls, 2);          // first page + one empty poll
        consumer.close();
        thread.join(5_000);

        assertEquals(2, delivered.size());
        var first = delivered.poll();
        assertEquals("user-1", first.key());
        assertEquals("payload-1", first.value());
        assertEquals("r1", first.cursor().toString());
        var second = delivered.poll();
        assertEquals("user-2", second.key());
        assertTrue(bodies.stream().anyMatch(b -> b.contains("\"cursor\":\"d1\"")
                || b.contains("\"cursor\": \"d1\"")),
                "page delivered cursor must be acked");
        assertFalse(thread.isAlive());
    }

    @Test
    void restartsAfterSessionDeathAndResumesFromDurableProgress() throws Exception {
        killFirstSession = true;
        var delivered = new ConcurrentLinkedQueue<FeedMessage<String, String>>();
        var errors = new ConcurrentLinkedQueue<Exception>();
        var consumer = feed().consumer("enricher")
                .backoff(Duration.ofMillis(10), Duration.ofMillis(50))
                .onRecord(delivered::add)
                .onError(errors::add);
        consumer.runAsync();
        awaitCount(openCalls, 2);           // died once, re-opened
        consumer.close();

        assertTrue(openCalls.get() >= 2, "session must re-open after death");
        assertFalse(errors.isEmpty(), "supervised failures reach onError");
        assertTrue(delivered.size() >= 2, "delivery resumes after restart");
    }

    @Test
    void runThrowsOnStartupRefusal() {
        refuseCreates = true;
        var consumer = feed().consumer("enricher").onRecord(m -> { });
        assertThrows(WhitewaterException.Refused.class, consumer::run);
        assertEquals(0, openCalls.get(), "declaration refusal fails before any open");
    }

    @Test
    void handlerExceptionLeavesThePageUnackedAndRedelivers() throws Exception {
        var attempts = new AtomicInteger();
        var delivered = new ConcurrentLinkedQueue<String>();
        var latch = new CountDownLatch(2);
        var consumer = feed().consumer("enricher")
                .backoff(Duration.ofMillis(10), Duration.ofMillis(50))
                .onRecord(m -> {
                    if (attempts.incrementAndGet() == 1) {
                        throw new IllegalStateException("poison attempt");
                    }
                    delivered.add(m.key());
                    latch.countDown();
                });
        consumer.runAsync();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        consumer.close();

        assertTrue(attempts.get() > 2, "the failed page must redeliver");
        assertTrue(delivered.contains("user-1"));
    }

    @Test
    void closeFromAnotherThreadStopsRun() throws Exception {
        var consumer = feed().consumer("enricher").onRecord(m -> { });
        var done = new CountDownLatch(1);
        Thread runner = new Thread(() -> {
            consumer.run();
            done.countDown();
        });
        runner.start();
        awaitCount(openCalls, 1);
        consumer.close();
        assertTrue(done.await(5, TimeUnit.SECONDS), "run() must return after close");
        assertFalse(consumer.isRunning());
    }

    // ---- store lookup ergonomics ------------------------------------------------

    @Test
    void findReturnsTheDecodedValueOrEmpty() {
        var store = client.stateStore("users.profiles", Long.class);
        assertEquals(42L, store.find("alice").orElseThrow());
        assertTrue(store.find("nobody").isEmpty());
    }

    @Test
    void getOrDefaultReturnsTheFallbackOnMiss() {
        var store = client.stateStore("users.profiles", Long.class);
        assertEquals(42L, store.getOrDefault("alice", 0L));
        assertEquals(0L, store.getOrDefault("nobody", 0L));
    }

    @Test
    void findByReturnsTheFirstNamedKeyMatch() {
        var store = client.stateStore("users.profiles", String.class);
        assertEquals("first", store.findBy("user_id", "u-7").orElseThrow());
        var body = bodies.get(0);
        assertTrue(body.contains("\"named_key\":\"user_id\""));
    }

    @Test
    void startAtOnlyMattersOnFirstDeclaration() {
        // The start position is a CREATE-time fact, not a per-run seek:
        // supervision restarts resume from the Reader's durable progress.
        var consumer = feed().consumer("enricher")
                .startAt(ReaderStart.beginning());
        assertEquals("enricher", consumer.reader());
    }
}
