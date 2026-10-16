package dev.whitewater;

import dev.whitewater.model.StateStoreSpec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Live store smoke against a running Riverbed — declares a named-key
 * StateStore on the Nodes, then exercises put/get/getBy/iterate/delete/
 * clear end to end through the typed facade: String keys and String
 * payloads, no byte plumbing in application code.
 *
 * <p>Runs only when {@code WHITEWATER_ENDPOINTS} is set, e.g.
 * {@code http://localhost:7371,http://localhost:7372,http://localhost:7373};
 * {@code WHITEWATER_API_KEY} defaults to the documented development key.
 */
@EnabledIfEnvironmentVariable(named = "WHITEWATER_ENDPOINTS", matches = ".+")
class LiveStoreIT {

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

        var spec = StateStoreSpec.named(domain + ".employees")
                .addKey("passport", true)
                .addKey("department", false);
        StateStore<String, String> store =
                client.stateStores().define(spec, null, String.class, String.class);
        assertEquals(domain + ".employees", store.name());

        store.put("emp-1", "alice", Map.of("passport", "P111", "department", "eng"));
        store.put("emp-2", "bob", Map.of("passport", "P222", "department", "eng"));
        store.put("emp-3", "cara", Map.of("passport", "P333", "department", "ops"));

        var row = store.getStrict("emp-1", Duration.ofSeconds(5));
        assertNotNull(row);
        assertEquals("alice", row.payload());

        var eng = store.getByStrict("department", "eng", Duration.ofSeconds(5));
        assertEquals(2, eng.rows().size());
        assertTrue(eng.freshness().appliedSeq() > 0);

        var keys = new TreeSet<String>();
        store.iterate(1, r -> keys.add(r.key()));
        assertEquals(new TreeSet<>(List.of("emp-1", "emp-2", "emp-3")), keys);

        store.delete("emp-1");
        assertNull(store.getStrict("emp-1", Duration.ofSeconds(5)));
        assertEquals(1, store.getByStrict("department", "eng", Duration.ofSeconds(5)).rows().size());

        store.clear();
        assertTrue(store.page().rows().isEmpty());
    }
}
