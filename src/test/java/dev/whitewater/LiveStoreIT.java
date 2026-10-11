package dev.whitewater;

import dev.whitewater.model.FeedAppend;
import dev.whitewater.model.ReaderStart;
import dev.whitewater.model.RecordStore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Live store smoke against a running Riverbed — the full path:
 * create Domain + Feed, append keyed records with metadata, then
 * {@code loadStore} a projected history read and named-key lookups.
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
    void createWriteLoadAndQueryAStore() throws Exception {
        var client = WhitewaterClient.builder()
                .endpoints(Arrays.stream(
                                System.getenv("WHITEWATER_ENDPOINTS").split(","))
                        .map(String::trim)
                        .map(URI::create)
                        .toList())
                .apiKey(System.getenv().getOrDefault(
                        "WHITEWATER_API_KEY",
                        "whitewater-local-development-admin-key"))
                .build();

        String domain = "smoke.java" + Long.toString(
                System.nanoTime() & 0x7fffffff, 36);
        String feed = domain + ".events";

        assertTrue(client.healthy());
        client.createDomain(domain);
        client.createFeed(feed);

        // Three keyed records, two departments in metadata.
        var session = UUID.randomUUID();
        String[][] rows = {
                {"emp-1", "alice", "eng"},
                {"emp-2", "bob", "eng"},
                {"emp-3", "cara", "ops"},
        };
        for (int i = 0; i < rows.length; i++) {
            client.feeds().append(feed, new FeedAppend(
                    UUID.randomUUID(), session, 1L, i + 1L,
                    utf8(rows[i][0]), utf8(rows[i][1]),
                    null, Map.of("department", utf8(rows[i][2]))));
        }

        // Projected bulk read into a named-key store.
        var store = RecordStore.named("employee-store").addKey("department");
        long loaded = client.loadStore(store).fromFeed(
                feed, ReaderStart.beginning(),
                (s, record) -> s.upsert(record)
                        .withKey(record.key())
                        .withNamedKey("department", record.metadata().get("department")));

        assertEquals(3, loaded);
        assertEquals(3, store.size());
        assertEquals("alice", new String(
                store.get("emp-1").orElseThrow().payload(), StandardCharsets.UTF_8));
        assertEquals(2, store.get("department", "eng").size(),
                "named-key lookups return every matching row");
        assertEquals(1, store.get("department", "ops").size());

        // Store bookkeeping: copy carries postings; delete strips them.
        var copy = RecordStore.named("employee-copy");
        store.copy(copy);
        assertEquals(2, copy.get("department", "eng").size());
        assertTrue(store.delete("emp-1"));
        assertEquals(1, store.get("department", "eng").size());

        var visited = new AtomicInteger();
        store.forEachParallel(2, entry -> {
            assertNotNull(entry.primaryKey());
            assertNotNull(entry.record());
            visited.incrementAndGet();
        });
        assertEquals(2, visited.get());
    }
}
