package dev.whitewater;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import dev.whitewater.internal.Transport;
import dev.whitewater.model.Cursor;
import dev.whitewater.model.ReaderStart;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The typed resource methods on {@link WhitewaterClient} are WCL builders:
 * each must serialize to exactly one statement on {@code POST /v1/admin/wcl},
 * and invalid names must fail locally before any HTTP call.
 */
class WclBuildersTest {

    private HttpServer server;
    private WhitewaterClient client;
    private volatile String capturedPath;
    private volatile String capturedBody;

    @BeforeEach
    void setUp() throws IOException {
        capturedPath = null;
        capturedBody = null;
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            capturedPath = exchange.getRequestURI().toString();
            capturedBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            byte[] out = ("{\"request_id\": \"00000000-0000-0000-0000-000000000000\","
                    + " \"results\": []}").getBytes(StandardCharsets.UTF_8);
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

    private String script() {
        try {
            JsonNode body = Transport.json().readTree(capturedBody);
            assertEquals("/v1/admin/wcl", capturedPath);
            return body.get("script").asText();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void creates() {
        client.createDomain("orders");
        assertEquals("CREATE DOMAIN orders", script());

        client.createFeed("orders.events");
        assertEquals("CREATE FEED orders.events", script());

        client.createWriter("checkout", "orders.events");
        assertEquals("CREATE WRITER checkout TO orders.events", script());

        client.createRole("ops");
        assertEquals("CREATE ROLE ops", script());
    }

    @Test
    void readersAndSubscriptions() {
        client.createReader("audit", "orders.events");
        assertEquals("CREATE READER audit FROM orders.events START AT BEGINNING", script());

        client.createReader("live", "orders.events", ReaderStart.now());
        assertEquals("CREATE READER live FROM orders.events START AT NOW", script());

        client.createReader("rewind", "orders.events",
                ReaderStart.timestampNs(1700000000000000000L));
        assertEquals("CREATE READER rewind FROM orders.events START AT TIMESTAMP 1700000000000000000",
                script());

        client.createReader("resumed", "orders.events",
                ReaderStart.cursor(Cursor.of("abc'def")));
        assertEquals("CREATE READER resumed FROM orders.events START AT CURSOR 'abc''def'", script());

        client.createSubscription("team", "orders.events", ReaderStart.now());
        assertEquals("CREATE SUBSCRIPTION team FROM orders.events START AT NOW", script());
    }

    @Test
    void sessionsAndSeek() {
        client.openWriterSession("checkout");
        assertEquals("OPEN WRITER SESSION checkout", script());

        client.seekReader("audit", ReaderStart.now());
        assertEquals("SEEK READER audit TO NOW", script());
    }

    @Test
    void describeRenameDropShow() {
        client.describeFeed("orders.events");
        assertEquals("DESCRIBE FEED orders.events", script());

        client.describeSubscription("team");
        assertEquals("DESCRIBE SUBSCRIPTION team", script());

        client.renameDomain("orders", "billing");
        assertEquals("RENAME DOMAIN orders TO billing", script());

        client.renameFeed("orders.events", "orders.accepted");
        assertEquals("RENAME FEED orders.events TO orders.accepted", script());

        client.dropWriter("checkout");
        assertEquals("DROP WRITER checkout", script());

        client.listFeeds();
        assertEquals("SHOW FEEDS", script());

        client.listDomains();
        assertEquals("SHOW DOMAINS", script());
    }

    @Test
    void invalidNamesFailLocally() {
        assertThrows(IllegalArgumentException.class, () -> client.createFeed("UPPER.case"));
        assertThrows(IllegalArgumentException.class, () -> client.createFeed(".leading"));
        assertThrows(IllegalArgumentException.class, () -> client.createDomain("two words"));
        assertThrows(IllegalArgumentException.class, () -> client.createWriter("bad;name", "orders.events"));
        assertThrows(IllegalArgumentException.class, () -> client.createReader("a'b", "orders.events"));
        assertThrows(IllegalArgumentException.class, () -> client.renameDomain("orders", "INJECTION; DROP"));
        assertNull(capturedPath, "no HTTP call should escape for invalid names");
    }
}
