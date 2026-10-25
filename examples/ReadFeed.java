import dev.whitewater.Feed;
import dev.whitewater.FeedReader;
import dev.whitewater.WhitewaterClient;
import dev.whitewater.model.FeedMessage;

import java.net.URI;

/**
 * Durable consumption: a named Reader owns an independent cursor on the
 * Riverbed, so progress survives client restarts — this is the
 * "subscribe" pattern for applications, versus the ephemeral tail in
 * {@link TailFeed}.
 *
 * <p>For a managed loop — auto-provisioning, retry policy, and a
 * {@code close()}-able handle — use {@code feed.subscribe(...)} instead
 * of this explicit form.
 *
 * The loop is explicit — Whitewater Readers pull rather than push:
 *
 *   open → [ fetch → process → ack ] → close
 *
 * ack() is the commit point: acknowledged progress is durable, so a
 * restarted client resumes after the last acked position. Process
 * records BEFORE acking them — ack-then-crash loses those records.
 */
public class ReadFeed {

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

        // The same codec binding works for reads: fetched pages arrive
        // decoded — no payloadAs() calls in the loop.
        Feed<String, Order> orders = client.feed("demo.orders", Order.class);

        // Declares the Reader if missing (durable Control Plane state),
        // opens the session, and owns the fence. Another client opening
        // the same Reader supersedes this session (stale_epoch on the
        // next fetch).
        try (FeedReader<String, Order> reader = orders.reader("demo.reader", 64)) {
            while (true) {
                // fetch() returns immediately with whatever is committed —
                // a Reader is not a long-poll. For "sit and wait" behavior,
                // sleep between empty fetches or use feed.fetch
                // with tail/wait_ms instead.
                var page = reader.fetch(50);

                for (FeedMessage<String, Order> m : page.records()) {
                    System.out.printf("%s -> %s @ %s%n",
                            m.key(), m.value(), m.ingestTimeNs());
                }

                // Commit durable progress AFTER processing the page. The
                // page's deliveredCursor covers every range the fetch drew
                // from — ack it, not the last record's cursor.
                reader.ack();

                if (page.records().isEmpty()) {
                    Thread.sleep(500);      // poll cadence for live reads
                }
            }
        }   // close() releases the session fence
    }
}
