import dev.whitewater.WhitewaterClient;
import dev.whitewater.Writer;

import java.net.URI;

/**
 * Producing records: the session-bound {@link Writer} opens
 * {@code OPEN WRITER SESSION} internally and owns the fencing epoch —
 * appending is {@code writer.append(key, payload)}.
 *
 * request_id dedupes retries: an ambiguous call (timeout, 503) retried
 * identically commits at most once. Pin a {@code requestId} on the
 * Record when you drive retries across calls.
 */
public class WriteFeed {

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
        client.createWriter("orders-writer", "demo.orders");

        // session-bound handle: the fencing epoch stays inside.
        var writer = client.writer("orders-writer");

        // simplest form: auto request identity, ingest-clock event time.
        var first = writer.append("order-1", "{\"total\":42}");
        System.out.printf("committed %s at %s%n", first.messageId(), first.cursor());

        // explicit request identity for a retryable write you may drive
        // across calls or process restarts.
        var requestId = java.util.UUID.randomUUID();
        var second = writer.append(new Writer.Record(
                requestId, "order-2", "{\"total\":17}"));
        System.out.printf("committed %s (deduplicated=%s)%n",
                second.messageId(), second.deduplicated());

        // batch appends share one round trip.
        var batch = writer.appendBatch(java.util.List.of(
                new Writer.Record("k3", "v3"),
                new Writer.Record("k4", "v4")));
        System.out.printf("batch committed %d records%n", batch.results().size());
    }
}
