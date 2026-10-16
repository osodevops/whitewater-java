import dev.whitewater.Feed;
import dev.whitewater.FeedWriter;
import dev.whitewater.WhitewaterClient;
import dev.whitewater.model.FeedEvent;

import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * Producing records with typed Codecs: bind the feed once and keys/
 * payloads serialize automatically — no getBytes() anywhere.
 *
 * request_id dedupes retries: an ambiguous call (timeout, 503) retried
 * identically commits at most once. Pin a {@code requestId} on the
 * FeedEvent when you drive retries across calls.
 */
public class WriteFeed {

    /** Any Jackson-mappable type — String/byte[]/JsonNode work too. */
    record Order(String id, int total) {}

    public static void main(String[] args) throws Exception {
        var client = WhitewaterClient.builder()
                .endpoints(
                        URI.create("http://localhost:7371"),
                        URI.create("http://localhost:7372"),
                        URI.create("http://localhost:7373"))
                .apiKey("whitewater-local-development-admin-key")
                .build();

        client.createDomain("demo");
        client.createFeed("demo.orders");

        // One binding carries the feed name + codecs. Order.class maps to
        // JSON; ProtobufCodecs.of(Order.class) or Codecs.of(...) bind
        // schema formats the same way.
        Feed<String, Order> orders = client.feed("demo.orders", Order.class);

        // Session-bound handle: declares the Writer if missing, opens
        // OPEN WRITER SESSION internally, and owns the fencing epoch.
        FeedWriter<String, Order> writer = orders.writer("orders-writer");

        // simplest form: auto request identity, ingest-clock event time.
        var first = writer.append("order-1", new Order("order-1", 42));
        System.out.printf("committed %s at %s%n", first.messageId(), first.cursor());

        // explicit request identity for a retryable write you may drive
        // across calls or process restarts.
        var requestId = java.util.UUID.randomUUID();
        var second = writer.append(new FeedEvent<>(requestId, "order-2", new Order("order-2", 17))
                .withTextMetadata(Map.of("tenant", "acme")));
        System.out.printf("committed %s (deduplicated=%s)%n",
                second.messageId(), second.deduplicated());

        // batch appends share one round trip.
        var batch = writer.appendAll(List.of(
                FeedEvent.of("order-3", new Order("order-3", 5)),
                FeedEvent.of("order-4", new Order("order-4", 9))));
        System.out.printf("batch committed %d records%n", batch.results().size());

        // Escape hatch: client.writer(name) is the same session bound to
        // String/byte[] — and feed.view(String.class, byte[].class)
        // rebinds any typed handle to raw wire bytes.
    }
}
