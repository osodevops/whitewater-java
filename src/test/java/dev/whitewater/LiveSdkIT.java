package dev.whitewater;

import com.fasterxml.jackson.databind.JsonNode;
import dev.whitewater.admin.Commands;
import dev.whitewater.model.IndexPredicate;
import dev.whitewater.model.IndexPredicate.IndexBound;
import dev.whitewater.model.ReaderStart;
import dev.whitewater.model.ResourceKind;
import dev.whitewater.model.TemporaryFetchOptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Live acceptance for the typed, session-bound SDK surface against a real
 * Riverbed — the Java mirror of the Rust SDK's `tests/sdk_live.rs`.
 *
 * <p>Runs only when {@code WHITEWATER_ENDPOINTS} names comma-separated
 * Node base URLs; {@code WHITEWATER_API_KEY} defaults to the local
 * development admin key. The assertions exercise the real wire through
 * {@link WhitewaterClient}'s ergonomic facades: protocol negotiation,
 * session-bound Writer idempotency and reopen, delivered-cursor Reader
 * ack semantics, member claim/fetch/ack fencing, StateStore/Index
 * freshness, and opaque-Cursor Feed reads.
 */
class LiveSdkIT {

    private static List<URI> bases;
    private static String apiKey;

    @BeforeAll
    static void endpoints() {
        String nodes = System.getenv("WHITEWATER_ENDPOINTS");
        apiKey = System.getenv().getOrDefault(
                "WHITEWATER_API_KEY", "whitewater-local-development-admin-key");
        if (nodes != null) {
            bases = new ArrayList<>();
            for (String part : nodes.split(",")) {
                String trimmed = part.trim();
                if (!trimmed.isEmpty()) {
                    bases.add(URI.create(trimmed));
                }
            }
        }
        assumeTrue(bases != null && !bases.isEmpty(),
                "WHITEWATER_ENDPOINTS unset — skipping live SDK acceptance");
    }

    private WhitewaterClient client() {
        return WhitewaterClient.builder().endpoints(bases).apiKey(apiKey).build();
    }

    private WhitewaterClient node(URI base) {
        return WhitewaterClient.builder().endpoints(base).apiKey(apiKey).build();
    }

    private static String uniq() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    }

    @Test
    void sessionBoundFacadesRoundtripAgainstALiveRiverbed() throws Exception {
        try (var client = client()) {
            var health = client.checkCompatible();
            assertEquals(64L * 1024 * 1024, health.limits().maxFrameBytes());
            assertTrue(health.limits().subscriptionMemberFetchMaxLimit() >= 1);

            String run = uniq();
            String domain = "sdk" + run;
            String feed = domain + ".events";
            String writer = "wr" + run;
            String reader = domain + ".rd";
            String subscription = domain + ".sub";
            var setup = client.wcl(
                    "CREATE DOMAIN " + domain + "; "
                            + "CREATE FEED " + feed + "; "
                            + "CREATE WRITER " + writer + " TO " + feed + "; "
                            + "CREATE READER " + reader + " FROM " + feed + " START AT BEGINNING; "
                            + "CREATE SUBSCRIPTION " + subscription + " FROM " + feed + " START AT BEGINNING;");
            assertEquals(5, setup.results().size(), "every statement reported: " + setup.results());

            // Writer: the bound handle owns the fencing epoch; the same
            // request_id must dedupe rather than commit twice.
            var bound = client.writer(writer);
            assertTrue(bound.sessionEpoch() >= 1);
            UUID requestId = UUID.randomUUID();
            var outcome = bound.append(new Writer.Record(requestId,
                    "order-42".getBytes(StandardCharsets.UTF_8),
                    "{\"total\":17}".getBytes(StandardCharsets.UTF_8),
                    1_700_000_000_000_000_001L,
                    Map.of("source", "sdk-live".getBytes(StandardCharsets.UTF_8))));
            assertFalse(outcome.deduplicated());
            assertNotNull(outcome.cursor(), "opaque Cursor returned");
            var retry = bound.append(new Writer.Record(requestId,
                    "order-42".getBytes(StandardCharsets.UTF_8),
                    "{\"total\":17}".getBytes(StandardCharsets.UTF_8),
                    1_700_000_000_000_000_001L,
                    Map.of("source", "sdk-live".getBytes(StandardCharsets.UTF_8))));
            assertTrue(retry.deduplicated(), "same request_id dedupes server-side");
            assertEquals(outcome.messageId(), retry.messageId());
            bound.reopen();
            assertTrue(bound.sessionEpoch() >= 1);

            // Reader: fetch the record, ack the page's delivered cursor —
            // never the last record's cursor.
            try (var boundReader = client.reader(reader, 16)) {
                var page = boundReader.fetch(16);
                assertEquals(1, page.records().size(), "page: " + page);
                var record = page.records().get(0);
                assertEquals("order-42", record.keyText());
                assertEquals(1_700_000_000_000_000_001L, record.eventTimeNs());
                assertEquals("sdk-live", record.metadataText("source"));
                boundReader.ack();
                assertEquals(page.deliveredCursor(), boundReader.acknowledgedCursor(),
                        "the page's delivered cursor is the durable restart point");
            }

            // Member: claim → fetch → ack; the ack consumes the lease so
            // releasing it must fence stale, not silently succeed.
            var member = client.member(subscription);
            var work = member.claim();
            assertTrue(work.leaseEpoch() >= 1);
            var workPage = member.fetch(work, 16);
            assertEquals(1, workPage.records().size(), "member page: " + workPage);
            assertEquals(work.leaseEpoch(), workPage.deliveryAttempt(),
                    "page reports its delivering claim's attempt number");
            member.ack(work, workPage);
            assertThrows(WhitewaterException.LeaseConflict.class,
                    () -> member.release(work),
                    "release after ack fences the consumed lease");

            // Opaque-Cursor read straight off the Feed.
            var records = client.feeds().records(feed, 10, null);
            assertTrue(records.stream().anyMatch(
                    r -> "order-42".equals(new String(r.key(), StandardCharsets.UTF_8))),
                    "feed read surfaces the appended record");

            // Pipe lifecycle through typed commands: declare → describe →
            // pause → resume → drop — no WCL text needed.
            String outFeed = domain + ".out";
            String relay = domain + ".relay";
            client.commands(Commands.of()
                    .createFeed(outFeed)
                    .defineForwardPipe(relay, subscription, outFeed));
            var described = client.wcl("DESCRIBE PIPE " + relay + ";")
                    .results().get(0).data();
            assertEquals(relay, described.path("name").asText());
            assertEquals("declared", described.path("stage").asText());
            assertEquals("forward", described.path("operation").path("operation").asText());
            assertFalse(described.path("output_feed_id").isNull());
            String subscriptionId = client.wcl("DESCRIBE SUBSCRIPTION " + subscription + ";")
                    .results().get(0).data().path("subscription_id").asText();
            assertEquals(subscriptionId, described.path("subscription_id").asText());
            assertTrue(client.wcl("SHOW PIPES;").toString().contains(relay),
                    "pipe appears in SHOW PIPES");
            client.commands(Commands.of().pausePipe(relay));
            assertEquals("paused", client.wcl("DESCRIBE PIPE " + relay + ";")
                    .results().get(0).data().path("stage").asText());
            client.commands(Commands.of().resumePipe(relay));
            assertEquals("declared", client.wcl("DESCRIBE PIPE " + relay + ";")
                    .results().get(0).data().path("stage").asText());
            client.commands(Commands.of().drop(ResourceKind.PIPE, relay));
            assertThrows(WhitewaterException.class,
                    () -> client.wcl("DESCRIBE PIPE " + relay + ";"),
                    "dropped pipe refuses describe");
        }
    }

    @Test
    void stateStoreIndexAndTemporaryFetchFacadesLive() throws Exception {
        try (var client = client()) {
            String run = uniq();
            String domain = "sdk" + run;
            String feed = domain + ".events";
            String writer = "wr" + run;
            String store = domain + ".kv";
            String index = domain + ".bykey";
            client.wcl(
                    "CREATE DOMAIN " + domain + "; "
                            + "CREATE FEED " + feed + "; "
                            + "CREATE WRITER " + writer + " TO " + feed + "; "
                            + "OPEN WRITER SESSION " + writer + "; "
                            + "DEFINE STATESTORE " + store + " KEYS region; "
                            + "CREATE INDEX " + index + " ON " + feed + " FIELDS KEY, METADATA 'tenant';");

            // Manual StateStore: put → strict lookup → named get_by →
            // iterate → delete → clear; identical mutation replay dedupes.
            // The typed facade takes String keys/payloads — no byte plumbing.
            StateStore<String, String> storeApi = client.stateStore(store, String.class);
            UUID mutation = UUID.randomUUID();
            var outcome = storeApi.put("acct-1", "{\"tier\":\"gold\"}",
                    Map.of("region", List.of("emea")), mutation);
            assertFalse(outcome.deduplicated());
            var replay = storeApi.put("acct-1", "{\"tier\":\"gold\"}",
                    Map.of("region", List.of("emea")), mutation);
            assertTrue(replay.deduplicated(), "same mutation_id dedupes");

            var lookup = storeApi.getStrict("acct-1");
            assertNotNull(lookup, "strict lookup returns the row");
            assertEquals("{\"tier\":\"gold\"}", lookup.payload());
            var page = storeApi.getByStrict("region", "emea", 10, java.time.Duration.ofSeconds(10));
            assertEquals(1, page.rows().size());
            var seen = new ArrayList<String>();
            storeApi.iterate(10, row -> seen.add(row.key()));
            assertEquals(1, seen.size());
            storeApi.delete("acct-1", UUID.randomUUID());
            assertNull(storeApi.getStrict("acct-1"));
            storeApi.clear(UUID.randomUUID());

            // Writer append lands in retained history; REBUILD INDEX runs
            // the shadow build — poll DESCRIBE until stage activates.
            var writerApi = client.writer(writer);
            writerApi.append(new Writer.Record(UUID.randomUUID(),
                    "order-7".getBytes(StandardCharsets.UTF_8),
                    "created".getBytes(StandardCharsets.UTF_8),
                    1_700_000_000_000_000_010L,
                    Map.of("tenant", "acme".getBytes(StandardCharsets.UTF_8))));
            client.wcl("REBUILD INDEX " + index + ";");

            boolean activated = false;
            for (int i = 0; i < 240 && !activated; i++) {
                try {
                    JsonNode stage = client.wcl("DESCRIBE INDEX " + index + ";")
                            .results().get(0).data().path("stage");
                    if (stage.hasNonNull("active")) {
                        activated = true;
                        break;
                    }
                } catch (WhitewaterException ignored) {
                    // not yet visible — keep polling
                }
                Thread.sleep(500);
            }
            assertTrue(activated, "index build activated within the bounded wait");

            // strict bounded-waits for the serving checkpoint — the posting
            // must be visible. KEY + METADATA extractors post raw bytes.
            var indexPage = client.queryIndex(index,
                    IndexPredicate.exact(
                            "order-7".getBytes(StandardCharsets.UTF_8),
                            "acme".getBytes(StandardCharsets.UTF_8)),
                    10, true, 30_000L);
            assertEquals(1, indexPage.rows().size(), "exact posting: " + indexPage);

            // Anonymous temporary fetch: bounded read off the same Feed.
            var tempPage = client.feeds().temporaryFetch(feed,
                    new TemporaryFetchOptions(null, 10, null, null, 500L, null));
            assertTrue(tempPage.records().stream().anyMatch(
                    r -> "order-7".equals(new String(r.key(), StandardCharsets.UTF_8))));

            // Range mode wire shape end-to-end (empty results are valid).
            client.queryIndex(index, IndexPredicate.range(List.of(),
                    IndexBound.inclusive(new byte[0]),
                    IndexBound.inclusive(new byte[]{(byte) 0xff})), 10, false, null);

            // Index demand attribution: every served query lands on the
            // answering Node's local row — merge across the endpoint pool.
            long queries = 0;
            long rowsReturned = 0;
            for (URI base : bases) {
                try (var node = node(base)) {
                    for (JsonNode row : node.demand("indexes").path("indexes")) {
                        if (index.equals(row.path("index").asText())) {
                            var telemetry = row.path("telemetry");
                            queries += telemetry.path("queries").asLong();
                            rowsReturned += telemetry.path("rows_returned").asLong();
                        }
                    }
                }
            }
            assertEquals(2, queries, "served index queries attributed");
            assertEquals(2, rowsReturned, "returned postings attributed");
        }
    }

    @Test
    void staleMemberEpochFencesAsTypedErrors() {
        try (var client = client()) {
            String run = uniq();
            String domain = "sdk" + run;
            String feed = domain + ".events";
            String writer = "wr" + run;
            String subscription = domain + ".sub";
            client.wcl(
                    "CREATE DOMAIN " + domain + "; "
                            + "CREATE FEED " + feed + "; "
                            + "CREATE WRITER " + writer + " TO " + feed + "; "
                            + "OPEN WRITER SESSION " + writer + "; "
                            + "CREATE SUBSCRIPTION " + subscription + " FROM " + feed + " START AT BEGINNING;");

            // A member identity that never joined (or went stale) must be
            // fenced by the wire contract, not silently admitted.
            var stale = new SubscriptionMemberClient.Joined(UUID.randomUUID(), 0, 0, 0);
            var error = assertThrows(WhitewaterException.class,
                    () -> client.subscriptionMember(subscription)
                            .claim(stale, UUID.randomUUID(), 60));
            assertTrue(error instanceof WhitewaterException.StaleSession
                            || error instanceof WhitewaterException.LeaseConflict
                            || error instanceof WhitewaterException.Conflict
                            || error instanceof WhitewaterException.Refused,
                    "unjoined member claim must fence, got " + error);
        }
    }
}
