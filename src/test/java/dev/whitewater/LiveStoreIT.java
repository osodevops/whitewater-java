package dev.whitewater;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Live store smoke against a running Riverbed — declares a named-key
 * StateStore on the Nodes, then exercises put/get/getBy/iterate/delete/
 * clear end to end.
 *
 * <p>Runs only when {@code WHITEWATER_ENDPOINTS} is set, e.g.
 * {@code http://localhost:7371,http://localhost:7372,http://localhost:7373};
 * {@code WHITEWATER_API_KEY} defaults to the documented development key.
 */
@EnabledIfEnvironmentVariable(named = "WHITEWATER_ENDPOINTS", matches = ".+")
class LiveStoreIT {

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void declaresAndExercisesAServerSideStateStore() throws Exception {
        var endpoints = Arrays.stream(System.getenv("WHITEWATER_ENDPOINTS").split("[;,]"))
                .map(String::trim)
                .filter(e -> !e.isEmpty())
                .map(e -> e.startsWith("http") ? e : "http://" + e)
                .map(URI::create)
                .toList();
        var client = WhitewaterClient.builder()
                .endpoints(endpoints)
                .apiKey(System.getenv().getOrDefault(
                        "WHITEWATER_API_KEY",
                        "whitewater-local-development-admin-key"))
                .build();

        String domain = "smoke.java" + Long.toString(
                System.nanoTime() & 0x7fffffff, 36);
        assertTrue(client.healthy());
        client.createDomain(domain);

        var spec = StateStore.named(domain + ".employees")
                .addKey("passport", true)
                .addKey("department", false);
        var store = client.stateStores().define(spec, null);
        assertEquals(domain + ".employees", store.name());

        store.put(utf8("emp-1"), utf8("alice"),
                Map.of("passport", "P111", "department", "eng"), null);
        store.put(utf8("emp-2"), utf8("bob"),
                Map.of("passport", "P222", "department", "eng"), null);
        store.put(utf8("emp-3"), utf8("cara"),
                Map.of("passport", "P333", "department", "ops"), null);

        var row = store.get(utf8("emp-1"), true, 5000L);
        assertNotNull(row);
        assertEquals("alice", new String(row.payload(), StandardCharsets.UTF_8));

        var eng = store.getBy("department", "eng", null, true, 5000L);
        assertEquals(2, eng.rows().size());
        assertTrue(eng.freshness().appliedSeq() > 0);

        var keys = new TreeSet<String>();
        store.iterate(1, r -> keys.add(new String(r.key(), StandardCharsets.UTF_8)));
        assertEquals(new TreeSet<>(List.of("emp-1", "emp-2", "emp-3")), keys);

        store.delete(utf8("emp-1"));
        assertNull(store.get(utf8("emp-1"), true, 5000L));
        assertEquals(1, store.getBy("department", "eng", null, true, 5000L).rows().size());

        store.clear(null);
        assertTrue(store.page(null, null).rows().isEmpty());
    }
}
