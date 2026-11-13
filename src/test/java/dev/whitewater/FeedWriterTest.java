package dev.whitewater;

import dev.whitewater.model.AppendResult;
import dev.whitewater.model.FeedEvent;
import dev.whitewater.testing.FakeRiverbed;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The pipelined {@link FeedWriter#write} path against the real wire
 * contract: per-record futures complete on commit, records coalesce
 * into {@code append-batch} calls by linger/size, and {@code close()}
 * flushes what is queued.
 */
class FeedWriterTest {

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

    private Domain orders() {
        return client.domain("orders");
    }

    private Feed<String, String> feed() {
        return orders().feed("events", String.class);
    }

    private FeedWriter<String, String> writer() {
        return feed().writer("orders.ingest");
    }

    @Test
    void writeCompletesWithTheCommittedAppendResult() throws Exception {
        assertEquals(0, riverbed.writerEpoch("orders.ingest"),
                "an idle handle holds no epoch");
        try (var writer = writer()) {
            var result = writer.write("order-1", "payload-1")
                    .get(10, TimeUnit.SECONDS);
            assertNotNull(result.messageId());
            assertNotNull(result.cursor());
            assertFalse(result.deduplicated());
        }
        assertEquals(1, riverbed.feedSize("orders.events"));
        assertTrue(riverbed.writerEpoch("orders.ingest") > 0,
                "the session bound on first write");
    }

    @Test
    void writesCoalesceIntoFewCommits() throws Exception {
        int before = riverbed.requests();
        try (var writer = writer()) {
            var futures = new ArrayList<CompletableFuture<AppendResult>>();
            for (int i = 0; i < 10; i++) {
                futures.add(writer.write("k-" + i, "v-" + i));
            }
            for (var f : futures) {
                assertNotNull(f.get(10, TimeUnit.SECONDS));
            }
        }
        assertEquals(10, riverbed.feedSize("orders.events"));
        int served = riverbed.requests() - before;
        assertTrue(served <= 5,
                "10 writes should ride a handful of commits (declare + open + batch), saw " + served);
    }

    @Test
    void callbackFormsDeliverTheResult() throws Exception {
        try (var writer = writer()) {
            var single = new AtomicReference<AppendResult>();
            var bi = new AtomicReference<AppendResult>();
            var error = new AtomicReference<Throwable>();

            writer.write("a", "1", single::set)
                    .get(10, TimeUnit.SECONDS);
            writer.write("b", "2", (result, err) -> {
                        bi.set(result);
                        error.set(err);
                    }).get(10, TimeUnit.SECONDS);

            assertNotNull(single.get());
            assertNotNull(bi.get());
            assertNull(error.get());
            assertNotNull(single.get().messageId());
        }
    }

    @Test
    void lingerHoldsTheBatchOpen() throws Exception {
        try (var writer = orders()
                .createWriter(feed())
                .writerId("orders.ingest")
                .linger(Duration.ofMillis(250))
                .open()) {
            var future = writer.write("k", "v");
            Thread.sleep(80);
            assertFalse(future.isDone(), "the linger window must hold the write");
            assertNotNull(future.get(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void maxBatchRecordsFlushesWithoutWaitingForLinger() throws Exception {
        try (var writer = orders()
                .createWriter(feed())
                .writerId("orders.ingest")
                .linger(Duration.ofSeconds(60))
                .maxBatchRecords(3)
                .open()) {
            var futures = new ArrayList<CompletableFuture<AppendResult>>();
            for (int i = 0; i < 3; i++) {
                futures.add(writer.write("k-" + i, "v-" + i));
            }
            for (var f : futures) {
                assertNotNull(f.get(10, TimeUnit.SECONDS),
                        "hitting the size bound flushes despite the long linger");
            }
        }
    }

    @Test
    void closeFlushesQueuedWrites() throws Exception {
        CompletableFuture<AppendResult> future;
        var writer = orders()
                .createWriter(feed())
                .writerId("orders.ingest")
                .linger(Duration.ofSeconds(60))
                .open();
        future = writer.write("k", "v");
        writer.close();     // must not wait out the linger
        assertNotNull(future.get(10, TimeUnit.SECONDS));
        assertEquals(1, riverbed.feedSize("orders.events"));
    }

    @Test
    void writeAfterCloseFailsTheFuture() throws Exception {
        var writer = writer();
        writer.close();
        var future = writer.write("k", "v");
        var thrown = assertThrows(ExecutionException.class,
                () -> future.get(5, TimeUnit.SECONDS));
        assertInstanceOf(IllegalStateException.class, thrown.getCause());
    }

    @Test
    void writeWithEventCarriesIdentityAndMetadata() throws Exception {
        try (var writer = writer()) {
            var event = new FeedEvent<String, String>(null, "order-7", "payload-7")
                    .withEventTimeNs(1697040000123456789L)
                    .withTextMetadata(java.util.Map.of("tenant", "acme"));
            var result = writer.write(event).get(10, TimeUnit.SECONDS);
            assertNotNull(result.messageId());
        }
        try (var reader = feed().reader("orders.audit", 64)) {
            var page = reader.fetch();
            assertEquals(1, page.records().size());
            assertEquals("order-7", page.records().get(0).key());
        }
    }
}
