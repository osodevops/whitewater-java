package dev.whitewater;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import dev.whitewater.internal.Transport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class SubscriptionMemberClientTest {

    private HttpServer server;
    private WhitewaterClient client;
    private final List<String> bodies = new ArrayList<>();
    private final List<String> paths = new ArrayList<>();
    private volatile String cannedResponse = "{}";

    @BeforeEach
    void setUp() throws IOException {
        bodies.clear();
        paths.clear();
        cannedResponse = "{}";
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            paths.add(exchange.getRequestURI().getPath());
            bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] out = cannedResponse.getBytes(StandardCharsets.UTF_8);
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

    private JsonNode lastBody() throws IOException {
        return Transport.json().readTree(bodies.get(bodies.size() - 1));
    }

    private SubscriptionMemberClient.Joined member() {
        return new SubscriptionMemberClient.Joined(
                UUID.fromString("11111111-1111-1111-1111-111111111111"), 7, 0, 0);
    }

    private SubscriptionMemberClient.WorkLease lease() {
        return new SubscriptionMemberClient.WorkLease(
                UUID.fromString("22222222-2222-2222-2222-222222222222"),
                UUID.fromString("11111111-1111-1111-1111-111111111111"), 7, 41, 0);
    }

    @Test
    void transferOmitsHolderMemberEpoch() throws IOException {
        cannedResponse = "{\"lease\":{\"work_id\":\"22222222-2222-2222-2222-222222222222\","
                + "\"member_id\":\"33333333-3333-3333-3333-333333333333\",\"member_epoch\":3,"
                + "\"lease_epoch\":42,\"expires_at_tick\":90}}";
        var transferred = client.subscriptionMember("app.sub").transfer(member(), lease(),
                UUID.fromString("33333333-3333-3333-3333-333333333333"), 3, 60);
        assertEquals("/v1/subscriptions/members/transfer", paths.get(0));
        JsonNode body = lastBody();
        assertFalse(body.has("member_epoch"),
                "transfer request must not carry the holder member_epoch: " + body);
        assertEquals("11111111-1111-1111-1111-111111111111", body.get("member_id").asText());
        assertEquals(41L, body.get("lease_epoch").asLong());
        assertEquals("33333333-3333-3333-3333-333333333333", body.get("to_member_id").asText());
        assertEquals(3L, body.get("to_member_epoch").asLong());
        assertEquals(60L, body.get("lease_ticks").asLong());
        assertEquals(42L, transferred.leaseEpoch());
        assertEquals(UUID.fromString("33333333-3333-3333-3333-333333333333"), transferred.memberId());
    }

    @Test
    void claimSendsMemberEpoch() throws IOException {
        cannedResponse = "{\"lease\":{\"work_id\":\"22222222-2222-2222-2222-222222222222\","
                + "\"member_id\":\"11111111-1111-1111-1111-111111111111\",\"member_epoch\":7,"
                + "\"lease_epoch\":40,\"expires_at_tick\":80}}";
        var lease = client.subscriptionMember("app.sub").claim(member(),
                UUID.fromString("22222222-2222-2222-2222-222222222222"), 60);
        assertEquals("/v1/subscriptions/members/claim", paths.get(0));
        JsonNode body = lastBody();
        assertEquals(7L, body.get("member_epoch").asLong());
        assertFalse(body.has("lease_epoch"));
        assertEquals(40L, lease.leaseEpoch());
    }

    @Test
    void fetchDecodesDeliveryEvidence() throws IOException {
        cannedResponse = "{\"records\":[],\"cursor\":\"cur-9\",\"positions\":{\"app.feed\":\"p1\"},"
                + "\"ownership_epoch\":4,\"delivery_attempt\":6,\"next_delivery_ns\":123456,"
                + "\"exhausted\":{\"app.feed\":{\"work_id\":\"w\",\"attempts\":3}}}";
        var page = client.subscriptionMember("app.sub").fetch(member(), lease(), 25);
        assertEquals(6L, page.deliveryAttempt());
        assertEquals(123456L, page.nextDeliveryNs());
        assertNotNull(page.exhausted());
        assertEquals("p1", page.positions().get("app.feed"));
        assertEquals("cur-9", page.cursor().value());
        JsonNode body = lastBody();
        assertEquals(7L, body.get("member_epoch").asLong());
        assertEquals(25, body.get("limit").asInt());
    }

    @Test
    void refusedLeaseRaisesConflict() {
        cannedResponse = "{\"lease\":null}";
        var conflict = assertThrows(WhitewaterException.Conflict.class,
                () -> client.subscriptionMember("app.sub").claim(member(),
                        UUID.fromString("22222222-2222-2222-2222-222222222222"), 60));
        assertEquals("lease.refused", conflict.code());
    }

    @Test
    void stateHitsMemberStateRoute() {
        cannedResponse = "{\"ownership_epoch\":2,\"members\":[],\"work\":[]}";
        client.subscriptionMember("app.sub").state();
        assertEquals("/v1/subscriptions/members/state", paths.get(0));
    }
}
