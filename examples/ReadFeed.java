import dev.whitewater.WhitewaterClient;
import dev.whitewater.model.FeedRecord;
import dev.whitewater.model.ReaderStart;

import java.net.URI;
import java.nio.charset.StandardCharsets;

/**
 * Durable consumption: a named Reader owns an independent cursor on the
 * Riverbed, so progress survives client restarts — this is the
 * "subscribe" pattern for applications, versus the ephemeral tail in
 * {@link TailFeed}.
 *
 * <p>For a managed loop — auto-provisioning, retry policy, and a
 * {@code close()}-able handle — use {@code client.subscribe(...)} instead
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

    public static void main(String[] args) throws Exception {
        var client = WhitewaterClient.builder()
                .endpoints(
                        URI.create("http://localhost:7371"),
                        URI.create("http://localhost:7372"),
                        URI.create("http://localhost:7373"))
                .apiKey("whitewater-local-development-admin-key")
                .build();

        // One-time provisioning: the Reader declaration is durable
        // Control Plane state, not a per-session object.
        client.createDomain("demo");
        client.createFeed("demo.tail");
        client.createReader("demo-reader", "demo.tail", ReaderStart.beginning());

        var reader = client.reader("demo-reader");

        // open() fences this client as the session owner. Another client
        // opening the same Reader supersedes this session (stale_epoch on
        // the next fetch).
        var session = reader.open(64);

        try {
            while (true) {
                // fetch() returns immediately with whatever is committed —
                // a Reader is not a long-poll. For "sit and wait" behavior,
                // sleep between empty fetches or use feeds().temporaryFetch
                // with tail/wait_ms instead.
                var page = reader.fetch(session, 50);

                for (FeedRecord r : page.records()) {
                    System.out.printf("%s @ %s%n",
                            new String(r.payload(), StandardCharsets.UTF_8), r.ingestTimeNs());
                }

                // Commit durable progress AFTER processing the page. The
                // page's deliveredCursor covers every range the fetch drew
                // from — ack it, not the last record's cursor.
                if (page.deliveredCursor() != null) {
                    session = reader.ack(session, page.deliveredCursor());
                }

                if (page.records().isEmpty()) {
                    Thread.sleep(500);      // poll cadence for live reads
                }
            }
        } finally {
            reader.close(session);          // release the session fence
        }
    }
}
