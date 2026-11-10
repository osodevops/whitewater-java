import dev.whitewater.WhitewaterClient;

import java.net.URI;

import static dev.whitewater.model.ReaderStart.*;

/**
 * The strongly-typed Domain front door: materialized catalog views,
 * relative Feed resolution, and fluent Reader/Writer builders.
 *
 * <p>Run a local Riverbed first, then:
 * {@code java -cp target/classes:examples DomainModel}
 */
public final class DomainModel {

    public static void main(String[] args) {
        var client = WhitewaterClient.builder()
                .endpoints(URI.create("http://localhost:7071"))
                .apiKey(System.getenv().getOrDefault("WHITEWATER_API_KEY", "dev-key"))
                .build();

        client.wcl("""
            CREATE DOMAIN shop;
            CREATE FEED shop.orders;
            CREATE FEED shop.returns;
            """);

        // Materialized Domain view — one batched control read.
        var shop = client.getDomain("shop");
        System.out.println("feeds: " + shop.feedNames());
        System.out.println("filtered: " + shop.getFeeds("order").stream()
                .map(f -> f.name()).toList());

        // Fluent Writer on a codec-bound Feed — declares if missing,
        // opens a session; the SDK serializes, never getBytes. Writer
        // and Reader ids resolve inside the Domain ("ingest" → shop.ingest).
        var writer = shop.createWriter(shop.feed("orders", String.class))
                .writerId("ingest")
                .open();
        writer.append("order-1", "{\"total\":42}");

        // Fluent Reader — readerId is the durable consume identity
        // (Kafka groupId), readerStartPoint the first-time offset.
        try (var reader = shop.createReader(shop.feed("orders", String.class))
                .readerId("fulfillment")
                .readerStartPoint(Beginning)
                .maxPageSize(64)
                .open()) {
            for (var page = reader.fetch(); !page.records().isEmpty(); page = reader.fetch()) {
                page.records().forEach(m ->
                        System.out.println("delivered " + m.key() + " -> "
                                + m.value()));
                reader.ack();
            }
        }

        System.out.println("readers now in shop: " + shop.refresh().readers());
    }
}
