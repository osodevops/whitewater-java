package dev.whitewater.testing;

import dev.whitewater.Feed;
import dev.whitewater.FeedReader;
import dev.whitewater.FeedWriter;
import dev.whitewater.WhitewaterClient;
import dev.whitewater.WhitewaterException;
import dev.whitewater.Writer;
import dev.whitewater.model.FeedEvent;
import dev.whitewater.model.FeedMessage;
import dev.whitewater.model.ReaderStart;
import dev.whitewater.model.StateStoreSpec;
import dev.whitewater.model.TemporaryFetchOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The {@link FakeRiverbed} double exercised end-to-end through the public
 * client API: delivery/ack semantics, request-id dedup, session fencing,
 * and the StateStore key/value model.
 */
class FakeRiverbedTest {

    private FakeRiverbed riverbed;
    private WhitewaterClient client;

    @BeforeEach
    void setUp() {
        riverbed = FakeRiverbed.start();
        client = riverbed.client();
    }

    @AfterEach
    void tearDown() {
        client.close();
        riverbed.close();
    }

    private Feed<String, String> feed() {
        return client.feed("orders.events", String.class);
    }

    @Test
    void servesHealthAndCompatibility() {
        assertTrue(client.healthy());
        var health = client.checkCompatible();
        assertEquals(1, health.protocol().major());
        assertEquals("ok", health.status());
    }

    @Test
    void ackedProgressDoesNotRedeliver() {
        FeedWriter<String, String> writer = feed().writer("orders.ingest");
        writer.append("order-1", "payload-1");

        try (FeedReader<String, String> reader = feed().reader("orders.audit", 64)) {
            var page = reader.fetch();
            assertEquals(1, page.records().size());
            assertEquals("order-1", page.records().get(0).key());
            assertEquals("payload-1", page.records().get(0).value());
            reader.ack();
        }
        try (FeedReader<String, String> reader = feed().reader("orders.audit", 64)) {
            assertTrue(reader.fetch().records().isEmpty(),
                    "acknowledged progress must not redeliver");
        }
    }

    @Test
    void unackedCloseRedelivers() {
        FeedWriter<String, String> writer = feed().writer("orders.ingest");
        writer.append("order-1", "payload-1");

        try (FeedReader<String, String> reader = feed().reader("orders.audit", 64)) {
            assertEquals(1, reader.fetch().records().size());
            // close() without ack — the delivered page must replay
        }
        try (FeedReader<String, String> reader = feed().reader("orders.audit", 64)) {
            var page = reader.fetch();
            assertEquals(1, page.records().size(), "unacked work must redeliver");
            assertEquals("order-1", page.records().get(0).key());
        }
    }

    @Test
    void requestIdDedupReturnsTheOriginalAppend() {
        FeedWriter<String, String> writer = feed().writer("orders.ingest");
        UUID requestId = UUID.randomUUID();

        var first = writer.append(new FeedEvent<>(requestId, "order-1", "payload-1"));
        var retry = writer.append(new FeedEvent<>(requestId, "order-1", "payload-1"));

        assertFalse(first.deduplicated());
        assertTrue(retry.deduplicated());
        assertEquals(first.messageId(), retry.messageId());
        assertEquals(first.cursor(), retry.cursor());
        assertEquals(1, riverbed.feedSize("orders.events"), "the retry must not write twice");
    }

    @Test
    void staleWriterEpochIsRefusedAndReopenHeals() {
        Writer first = client.writer("orders.ingest");
        Writer second = client.writer("orders.ingest");   // fences the first session

        assertThrows(WhitewaterException.StaleSession.class,
                () -> first.append("order-1", "payload-1"));
        second.append("order-1", "payload-1");

        first.reopen();                                    // binds a fresh epoch
        first.append("order-2", "payload-2");
        assertThrows(WhitewaterException.StaleSession.class,
                () -> second.append("order-3", "payload-3"));
        assertEquals(2, riverbed.feedSize("orders.ingest"));
    }

    @Test
    void staleReaderEpochIsRefused() {
        feed().writer("orders.ingest").append("order-1", "payload-1");
        var first = feed().reader("orders.audit", 8);
        var second = feed().reader("orders.audit", 8);    // fences the first session

        assertThrows(WhitewaterException.StaleSession.class, first::fetch);
        assertEquals(1, second.fetch().records().size());
        second.close();
        // close on a fenced session is fenced too — the epoch no longer owns the Reader
        assertThrows(WhitewaterException.StaleSession.class, first::close);
    }

    @Test
    void temporaryFetchAndLoadReadCommittedHistory() {
        FeedWriter<String, String> writer = feed().writer("orders.ingest");
        writer.append("order-1", "payload-1");
        writer.append("order-2", "payload-2");

        var page = feed().fetch(TemporaryFetchOptions.page(null, 10));
        assertEquals(2, page.records().size());
        assertNotNull(page.nextCursor());

        var rest = feed().fetch(page.nextCursor(), 10);
        assertTrue(rest.records().isEmpty());

        var sink = new ArrayList<FeedMessage<String, String>>();
        long loaded = feed().load(ReaderStart.beginning(), sink::addAll);
        assertEquals(2, loaded);
        assertEquals(List.of("order-1", "order-2"),
                sink.stream().map(FeedMessage::key).toList());
    }

    @Test
    void subscribeDeliversAcksAndCloses() throws Exception {
        feed().writer("orders.ingest").append("order-1", "payload-1");
        var delivered = new CountDownLatch(1);
        var seen = new java.util.concurrent.CopyOnWriteArrayList<String>();
        try (var sub = feed().subscribe("orders.audit", ReaderStart.beginning(), batch -> {
            batch.forEach(m -> seen.add(m.key()));
            delivered.countDown();
        })) {
            assertTrue(delivered.await(5, TimeUnit.SECONDS), "subscription delivered");
            // the page ack lands after onRecords — wait for it before close,
            // or close legitimately redelivers the in-flight page
            long deadline = System.currentTimeMillis() + 5_000;
            while (riverbed.readerProgress("orders.audit") == 0
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertEquals(1, riverbed.readerProgress("orders.audit"), "page was acked");
        }
        assertEquals(List.of("order-1"), seen);
        // durable progress survived the close: a fresh subscribe sees nothing new
        var again = new CountDownLatch(1);
        try (var sub = feed().subscribe("orders.audit", ReaderStart.beginning(),
                batch -> again.countDown())) {
            assertFalse(again.await(1_500, TimeUnit.MILLISECONDS),
                    "acked work must not redeliver to a new session");
        }
        assertEquals(List.of("order-1"), seen);
    }

    @Test
    void storeRoundTripsRowsAndDedupesMutations() {
        client.stateStores().define(StateStoreSpec.named("accounts.employees")
                .addKey("department"), null);
        var store = client.stateStore("accounts.employees", String.class);

        UUID mutationId = UUID.randomUUID();
        var put = store.put("user-1", "profile-1", Map.of("department", "eng"), mutationId);
        var retry = store.put("user-1", "profile-1", Map.of("department", "eng"), mutationId);
        assertFalse(put.deduplicated());
        assertTrue(retry.deduplicated());
        assertEquals(put.mutationId(), retry.mutationId());
        assertEquals(1, riverbed.storeSize("accounts.employees"));

        store.put("user-2", "profile-2", Map.of("department", "sales"));
        assertEquals("profile-1", store.find("user-1").orElseThrow());
        assertTrue(store.find("nobody").isEmpty());
        assertEquals("profile-1", store.findBy("department", "eng").orElseThrow());
        assertEquals(1, store.getBy("department", "sales").rows().size());

        var keys = new ArrayList<String>();
        store.iterate(row -> keys.add(row.key()));
        assertEquals(List.of("user-1", "user-2"), keys);

        store.delete("user-1");
        assertTrue(store.find("user-1").isEmpty());
        store.clear();
        assertEquals(0, riverbed.storeSize("accounts.employees"));
    }

    @Test
    void subscriptionMemberClaimsFetchesAndAcks() {
        client.createSubscription("orders.audit", "orders.events");
        feed().writer("orders.ingest").append("order-1", "payload-1");

        var member = client.member("orders.audit");
        var work = member.claim();
        var page = member.fetch(work, 64);
        assertEquals(1, page.records().size());
        member.ack(work, page);
        member.release(work);

        var again = member.claim();
        assertTrue(member.fetch(again, 64).records().isEmpty(),
                "the shared frontier must not redeliver acked work");
        member.release(again);
    }

    @Test
    void unknownPathsSurfaceAsRefused() {
        // A path the fake does not implement returns the protocol error
        // envelope; Transport maps (404, no code) to Refused.
        var transport = new dev.whitewater.internal.Transport(
                riverbed.endpoints(), "test-key", java.time.Duration.ofSeconds(5));
        var thrown = assertThrows(WhitewaterException.Refused.class,
                () -> transport.post("/v1/does/not/exist",
                        dev.whitewater.internal.Transport.json().createObjectNode()));
        assertEquals(404, thrown.status());
    }
}
