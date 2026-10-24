import dev.whitewater.WhitewaterClient;
import dev.whitewater.admin.Commands;
import dev.whitewater.model.Extractor;
import dev.whitewater.model.NamedKey;
import dev.whitewater.model.PipeOperation;
import dev.whitewater.model.WindowAggregation;
import dev.whitewater.model.WindowTime;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

/**
 * Managed Pipes: declare the processing step once and the Riverbed
 * drives it — no consumer loop, no cursor bookkeeping.
 *
 * <p>This guide declares three declarations worth knowing:
 *
 * <ol>
 *   <li>a rolling-window Pipe — every record delivered on the
 *       Subscription accumulates into fixed event-time panes of a
 *       managed StateStore view, keyed by the record Key;
 *   <li>the WATERMARK lateness model — panes close once the durable
 *       horizon {@code max(event_time) - watermark} passes the pane
 *       end, so close decisions follow event-time disorder rather than
 *       arrival order. GRACE bounds how long an emitted pane still
 *       accepts revising events (each re-emits under a bumped
 *       revision); past grace they count into the sentinel
 *       {@code late} row. EMIT TO FEED publishes each closed pane;
 *   <li>a plain forward Pipe copying the same source into an archive
 *       Feed on its own Subscription.
 * </ol>
 *
 * <p>Run against the development Riverbed:
 *
 * <pre>{@code
 * mvn -q package
 * javac -cp target/classes:$(cat target/cp.txt) -d target/examples examples/WindowPipes.java
 * java -cp "target/examples;target/classes;$(cat target/cp.txt)" WindowPipes
 * }</pre>
 *
 * <p>WHITEWATER_ENDPOINTS (csv) and WHITEWATER_API_KEY override the
 * localhost defaults, like the other examples.
 */
public class WindowPipes {

    static final long MINUTE_NS = 60_000_000_000L;
    static final long HOUR_NS = 60 * MINUTE_NS;

    static List<URI> endpoints() {
        String raw = System.getenv("WHITEWATER_ENDPOINTS");
        if (raw == null || raw.isBlank()) {
            raw = "http://localhost:7371,http://localhost:7372,http://localhost:7373";
        }
        return Arrays.stream(raw.split("[,;]"))
                .map(String::trim)
                .filter(item -> !item.isEmpty())
                .map(item -> item.contains("://") ? item : "http://" + item)
                .map(URI::create)
                .toList();
    }

    public static void main(String[] args) {
        var client = WhitewaterClient.builder()
                .endpoints(endpoints().toArray(URI[]::new))
                .apiKey(System.getenv().getOrDefault(
                        "WHITEWATER_API_KEY",
                        "whitewater-local-development-admin-key"))
                .build();

        var exec = client.commands(Commands.of()
                .createDomain("accounts")
                .createFeed("accounts.events")
                .createFeed("accounts.results")
                .createFeed("accounts.archive")
                .createSubscription("accounts.flow", "accounts.events")
                .createSubscription("accounts.archive.flow", "accounts.events")
                // A window Pipe's output store must declare non-unique
                // 'key' and 'pane' named keys — the pane view stays
                // enumerable without knowing composite row keys.
                .defineStateStore("accounts.panes", null, List.of(
                        new NamedKey("key", false),
                        new NamedKey("pane", false)))
                // WCL equivalent:
                //   CREATE PIPE accounts.watermarked
                //   FROM SUBSCRIPTION accounts.flow
                //   TO STATESTORE accounts.panes
                //   WINDOW 1h EVENT WATERMARK 5m GRACE 30s EXPIRE 2h
                //   BY KEY COUNT EMIT TO FEED accounts.results;
                .defineWindowPipe(
                        "accounts.watermarked",
                        "accounts.flow",
                        "accounts.panes",
                        PipeOperation.window(
                                HOUR_NS,                        // WINDOW 1h panes
                                Extractor.recordKey(),           // BY KEY — group on the record Key
                                WindowAggregation.count(),       // COUNT per (Key, pane)
                                WindowTime.EVENT,
                                null,                            // no MAX_LATE — one lateness model only
                                5 * MINUTE_NS,                   // WATERMARK: horizon = max(event) - 5m
                                30_000_000_000L,                 // GRACE 30s: emitted panes revise + re-emit
                                2 * HOUR_NS,                     // EXPIRE 2h: pane retention bound
                                null, null),
                        "accounts.results")                        // EMIT TO FEED
                // A plain copy Pipe on its own Subscription, for the
                // archive shape a window Pipe deliberately is not.
                .defineForwardPipe(
                        "accounts.relay", "accounts.archive.flow", "accounts.archive"));

        System.out.println("declared at catalog revision " + exec.revision());

        // Lifecycle surface: describe returns the live catalog row —
        // stage plus the opaque operation definition as declared.
        var pipe = client.pipe("accounts.watermarked");
        var info = pipe.describe();
        System.out.println(info.name() + " stage=" + info.stage()
                + " status=" + info.status());
        System.out.println("operation: " + info.operation());

        pipe.pause();      // cooperative, idempotent — in-flight effects finish
        System.out.println("paused stage=" + pipe.describe().stage());
        pipe.resume();     // restarts driving from the durable frontier
        System.out.println("resumed stage=" + pipe.describe().stage());

        client.close();
    }
}
