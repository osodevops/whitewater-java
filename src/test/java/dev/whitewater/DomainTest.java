package dev.whitewater;

import dev.whitewater.model.ReaderStart;
import dev.whitewater.testing.FakeRiverbed;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static dev.whitewater.model.ReaderStart.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * The strongly-typed {@link Domain} facade: materialized catalog views
 * scoped by {@code space_id}, relative Feed resolution, and the
 * fluent {@link ReaderBuilder}/{@link WriterBuilder} surface.
 */
class DomainTest {

    private FakeRiverbed riverbed;
    private WhitewaterClient client;

    @BeforeEach
    void setUp() {
        riverbed = FakeRiverbed.start();
        client = riverbed.client();
    }

    @AfterEach
    void tearDown() {
        riverbed.close();
    }

    private void shopCatalog() {
        client.wcl("""
            CREATE DOMAIN shop;
            CREATE DOMAIN other;
            CREATE FEED shop.orders;
            CREATE FEED shop.returns;
            CREATE FEED shop.orders.eu;
            CREATE FEED other.metrics;
            CREATE WRITER shop.ingest TO shop.orders;
            CREATE READER shop.audit FROM shop.orders;
            CREATE SUBSCRIPTION shop.pipeline FROM shop.orders;
            """);
    }

    @Test
    void domainMaterializesScopedResources() {
        shopCatalog();
        var shop = client.getDomain("shop");

        assertEquals(List.of("shop.orders", "shop.orders.eu", "shop.returns"),
                shop.feedNames());
        assertEquals(List.of("shop.audit"), shop.readers());
        assertEquals(List.of("shop.ingest"), shop.writers());
        assertEquals(List.of("shop.pipeline"), shop.subscriptions());
        assertEquals(shop.domainId().toString(),
                shop.describe().path("space_id").asText());
        assertTrue(shop.indexes().isEmpty());
        assertTrue(shop.pipes().isEmpty());
        assertTrue(shop.stateStores().isEmpty());
    }

    @Test
    void feedsFiltersByRelativePrefix() {
        shopCatalog();
        var shop = client.getDomain("shop");

        assertEquals(List.of("shop.orders", "shop.orders.eu"),
                shop.getFeeds("orders").stream().map(Feed::name).toList());
        assertEquals(List.of("shop.orders.eu"),
                shop.getFeeds("orders.eu").stream().map(Feed::name).toList());
        assertEquals(List.of("shop.returns"),
                shop.feeds("ret").stream().map(Feed::name).toList());
    }

    @Test
    void feedResolvesRelativeAndQualifiedNames() {
        var shop = client.domain("shop");

        assertEquals("shop.orders", shop.feed("orders").name());
        assertEquals("shop.orders.eu", shop.feed("orders.eu").name());
        assertEquals("shop.orders", shop.getFeed("shop.orders").name());  // already qualified
        assertSame(shop.feed("orders"), shop.feed("orders"));             // cached handle
    }

    @Test
    void feedRejectsMalformedNames() {
        var shop = client.domain("shop");

        assertThrows(IllegalArgumentException.class, () -> shop.feed("BAD.NAME"));
        assertThrows(IllegalArgumentException.class, () -> shop.feed(""));
        assertThrows(NullPointerException.class, () -> shop.feed(null));
        // a name outside this Domain simply resolves under it — relative
        // resolution means shop.getFeed("other.metrics") is shop.other.metrics
        assertEquals("shop.other.metrics", shop.getFeed("other.metrics").name());
    }

    @Test
    void getDomainRefusesAnUndeclaredDomain() {
        var ex = assertThrows(WhitewaterException.Refused.class,
                () -> client.getDomain("missing"));
        assertEquals("domain.unknown", ex.code());
    }

    @Test
    void readerBuilderDeclaresAndOpensATypedReader() {
        shopCatalog();
        var shop = client.getDomain("shop");
        client.createWriter(shop, shop.getFeed("orders"))
                .writerId("shop.ingest2")
                .open()
                .append("order-1", "{\"total\":42}".getBytes());

        try (var reader = client.createReader(shop, shop.getFeed("orders"))
                .readerId("shop.fulfillment")
                .readerStartPoint(Beginning)
                .capacity(4)
                .open()) {
            var page = reader.fetch();
            assertEquals(1, page.records().size());
            assertEquals("order-1", page.records().get(0).key());
            assertTrue(reader.ack() != null);
        }
        assertTrue(client.getDomain("shop").readers().contains("shop.fulfillment"));
    }

    @Test
    void readerBuilderHonoursEndAsTailOnly() {
        shopCatalog();
        var shop = client.getDomain("shop");

        try (var reader = client.createReader(shop, shop.getFeed("orders"))
                .readerId("shop.tail")
                .readerStartPoint(End)
                .open()) {
            assertTrue(reader.fetch().records().isEmpty());
        }
    }

    @Test
    void readerBuilderRequiresAnId() {
        var shop = client.domain("shop");
        var ex = assertThrows(IllegalStateException.class,
                () -> client.createReader(shop, "orders").open());
        assertTrue(ex.getMessage().contains("readerId"));
    }

    @Test
    void createReaderRefusesAFeedOutsideTheDomain() {
        var shop = client.domain("shop");
        var other = client.feed("other.metrics");

        var ex = assertThrows(IllegalArgumentException.class,
                () -> client.createReader(shop, other));
        assertTrue(ex.getMessage().contains("outside Domain"));
    }

    @Test
    void domainHangingBuildersComposeAndOpen() {
        shopCatalog();
        var shop = client.getDomain("shop");

        // shop.createWriter / shop.createReader resolve relative names —
        // the whole service shape hangs off the Domain.
        var writer = shop.createWriter("orders").writerId("shop.w1").open();
        writer.append("order-1", "{\"total\":5}".getBytes());

        try (var reader = shop.createReader("orders")
                .readerId("shop.r1")
                .readerStartPoint(Beginning)
                .open()) {
            assertEquals("order-1", reader.fetch().records().get(0).key());
        }
        assertTrue(client.getDomain("shop").writers().contains("shop.w1"));
        assertTrue(client.getDomain("shop").readers().contains("shop.r1"));
    }

    @Test
    void domainCreateReaderRefusesAForeignFeed() {
        var shop = client.domain("shop");
        var other = client.feed("other.metrics");
        assertThrows(IllegalArgumentException.class, () -> shop.createReader(other));
        assertThrows(IllegalArgumentException.class, () -> shop.createWriter(other));
    }

    @Test
    void readerForEachDeliversRecordsAndAcksPages() {
        shopCatalog();
        var shop = client.getDomain("shop");
        var writer = shop.createWriter("orders").writerId("shop.w2").open();
        writer.append("order-1", "1".getBytes());
        writer.append("order-2", "2".getBytes());

        // capacity(1) → one record per page: page 1 delivers + acks,
        // the handler stops on page 2 before it is acked.
        var reader = shop.createReader("orders")
                .readerId("shop.pump")
                .readerStartPoint(Beginning)
                .capacity(1)
                .open();
        var seen = new java.util.ArrayList<String>();
        reader.forEachWhile(m -> {
            seen.add(m.key());
            return seen.size() < 2;
        });
        assertEquals(List.of("order-1", "order-2"), seen);
        assertNotNull(reader.acknowledgedCursor());     // page 1 committed

        // The unacked tail replays on the next open.
        try (var again = shop.createReader("orders").readerId("shop.pump").open()) {
            assertEquals("order-2", again.fetch().records().get(0).key());
        }
    }

    @Test
    void writerBuilderDeclaresAndOpensATypedWriter() {
        shopCatalog();
        var shop = client.getDomain("shop");

        var writer = client.createWriter(shop, shop.getFeed("orders"))
                .writerId("shop.orders.w")
                .open();
        writer.append("k1", "v1".getBytes());
        assertTrue(client.getDomain("shop").writers().contains("shop.orders.w"));
    }

    @Test
    void refreshPicksUpNewFeeds() {
        client.wcl("CREATE DOMAIN shop; CREATE FEED shop.a;");
        var shop = client.getDomain("shop");
        assertEquals(List.of("shop.a"), shop.feedNames());

        client.wcl("CREATE FEED shop.b;");
        assertEquals(List.of("shop.a"), shop.feedNames());   // cached
        assertEquals(List.of("shop.a", "shop.b"), shop.refresh().feedNames());
    }

    @Test
    void createFeedDeclaresInsideTheDomain() {
        client.createDomain("shop");
        var shop = client.getDomain("shop");

        shop.createFeed("orders");                     // CREATE FEED shop.orders
        shop.createFeed("shop.daily");                 // qualified form also works

        assertEquals(List.of("shop.daily", "shop.orders"),
                shop.refresh().feedNames());
        assertThrows(IllegalArgumentException.class,
                () -> shop.createFeed("BAD.NAME"));    // grammar refused locally
    }

    @Test
    void feedSchemaLayersTheFeedOverrideOverTheDomainDefault() {
        shopCatalog();
        client.setDomainSchema("shop",
                dev.whitewater.model.FeedSchema.value(
                        dev.whitewater.model.SchemaDecl.of("json", "order-v1")));
        client.setFeedSchema("shop.orders",
                dev.whitewater.model.FeedSchema.key(
                        dev.whitewater.model.SchemaDecl.of("text")));

        var shop = client.getDomain("shop");
        assertEquals("json", shop.schema().value().format());
        assertEquals("order-v1", shop.schema().value().subject());

        // Effective: key from the Feed override, value inherited.
        var effective = shop.feedSchema("orders");
        assertEquals("text", effective.key().format());
        assertEquals("json", effective.value().format());
        // Declared: only the Feed's own sides.
        var declared = shop.declaredFeedSchema("orders");
        assertEquals("text", declared.key().format());
        assertNull(declared.value());
        // Undeclared on the Feed -> inherits the Domain default.
        assertEquals("json", shop.feedSchema("returns").value().format());
        assertNull(shop.declaredFeedSchema("returns"));
    }

    @Test
    void dropSchemaRevertsToDomainInheritance() {
        shopCatalog();
        client.setDomainSchema("shop",
                dev.whitewater.model.FeedSchema.value(
                        dev.whitewater.model.SchemaDecl.of("json")));
        client.setFeedSchema("shop.orders",
                dev.whitewater.model.FeedSchema.value(
                        dev.whitewater.model.SchemaDecl.of("avro")));

        var shop = client.getDomain("shop");
        assertEquals("avro", shop.feedSchema("orders").value().format());

        client.clearFeedSchema("shop.orders");
        shop.refresh();
        assertEquals("json", shop.feedSchema("orders").value().format());
        assertNull(shop.declaredFeedSchema("orders"));
    }

    @Test
    void describeFeedCarriesDeclaredAndEffectiveSchema() {
        shopCatalog();
        client.setFeedSchema("shop.orders",
                dev.whitewater.model.FeedSchema.value(
                        dev.whitewater.model.SchemaDecl.custom("msgpack")));
        var described = client.commands(
                dev.whitewater.admin.Commands.of()
                        .describe(dev.whitewater.model.ResourceKind.FEED, "shop.orders"));
        var row = described.results().get(0).data();
        assertEquals("custom.msgpack", row.path("schema").path("value").path("format").asText());
        assertEquals("custom.msgpack",
                row.path("effective_schema").path("value").path("format").asText());
    }

    @Test
    void readerStartTimestampAcceptsAnInstant() {
        var start = ReaderStart.timestamp(Instant.ofEpochSecond(1_700_000_000L, 500));
        assertEquals(1_700_000_000_000_000_500L, start.timestampNs());
        assertThrows(ArithmeticException.class,
                () -> ReaderStart.timestamp(Instant.MAX));
    }
}
