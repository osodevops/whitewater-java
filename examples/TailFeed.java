import dev.whitewater.WhitewaterClient;
import dev.whitewater.model.Cursor;
import dev.whitewater.model.FeedAppend;
import dev.whitewater.model.FeedRecord;
import dev.whitewater.model.TemporaryFetchOptions;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Live demo: create a feed, sit in a tail fetch, and watch records arrive.
 * Run with the SDK jar + Jackson on the classpath:
 *
 *   java -cp "target/classes;...jackson..." TailFeed
 */
public class TailFeed {

    public static void main(String[] args) throws Exception {
        var client = WhitewaterClient.builder()
                .endpoints(
                        URI.create("http://localhost:7371"),
                        URI.create("http://localhost:7372"),
                        URI.create("http://localhost:7373"))
                .apiKey("whitewater-local-development-admin-key")
                .build();

        // 1. Provision a domain + feed via WCL (typed commands are for queries
        //    like {"command": "show", "kind": "feeds"}).
        client.wcl("CREATE DOMAIN demo; CREATE FEED demo.tail");

        // 2. Append a record 3s from now so we can watch the tail WAIT, then return.
        new Thread(() -> {
            try {
                Thread.sleep(3000);
                var result = client.feeds().append("demo.tail", new FeedAppend(
                        UUID.randomUUID(), UUID.randomUUID(), 1, 1,
                        "key-1".getBytes(StandardCharsets.UTF_8),
                        "hello whitewater".getBytes(StandardCharsets.UTF_8),   // low-level API: raw bytes
                        null, Map.of()));
                System.out.println(">>> appended, cursor=" + result.cursor().value());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        // 3. The tail loop: each fetch waits up to wait_ms server-side, so a call
        //    returns either when records arrive or when the wait expires. This is
        //    the primitive the UI uses for live view streaming.
        Cursor after = null;                       // opaque; null = feed tail position
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(10).toMillis();
        while (System.currentTimeMillis() < deadline) {
            long sent = System.currentTimeMillis();
            var page = client.feeds().temporaryFetch("demo.tail",
                    TemporaryFetchOptions.tail(after, 50, 2500));
            long waited = System.currentTimeMillis() - sent;

            for (FeedRecord r : page.records()) {
                System.out.printf("got record after %dms wait: %s  ingest_ns=%s%n",
                        waited, r.payloadText(), r.ingestTimeNs());
            }
            if (page.records().isEmpty()) {
                System.out.printf("(still waiting... fetch returned empty after %dms)%n", waited);
            }
            after = page.nextCursor();             // continuation frontier
        }
        client.close();
    }
}
