import dev.whitewater.Scoped;
import dev.whitewater.WhitewaterClient;

import java.net.URI;

/**
 * Cross-Domain Pipes: a Pipe is always declared inside one Domain — its
 * name resolves there — but its Subscription source and output Feed or
 * StateStore can point at resources in ANY Domain.
 *
 *   purchasing: orders feed + fulfil subscription (what sales commits)
 *   warehouse:  inbound feed (what fulfilment works from)
 *
 * A plain string argument resolves inside the Pipe's owning Domain; a
 * cross-Domain target is passed as a handle — a Feed / StateStore /
 * SubscriptionRef, which implement {@link dev.whitewater.ScopedRef} —
 * or {@link Scoped#absolute}. The reference carries its full dotted
 * name, so it can never be silently re-prefixed into the wrong Domain.
 */
public class CrossDomain {

    public static void main(String[] args) {
        var client = WhitewaterClient.builder()
                .endpoints(
                        URI.create("http://localhost:7371"),
                        URI.create("http://localhost:7372"),
                        URI.create("http://localhost:7373"))
                .apiKey("whitewater-local-development-admin-key")
                .build();

        var purchasing = client.createDomain("purchasing");
        var warehouse = client.createDomain("warehouse");

        purchasing.createFeed("orders");
        purchasing.createSubscription("fulfil", "orders");
        warehouse.createFeed("inbound");

        // Declared in purchasing — the team that owns the orders feed —
        // writing into the warehouse Domain's intake feed. The Feed
        // handle is a ScopedRef: it carries warehouse.inbound verbatim.
        purchasing.defineForwardPipe("relay",
                Scoped.relative("fulfil"), warehouse.feed("inbound"));
        // → DEFINE PIPE purchasing.relay FROM purchasing.fulfil
        //     TO FEED warehouse.inbound

        // The pull shape — warehouse owns the Pipe and draws from
        // purchasing's Subscription via its handle.
        warehouse.defineForwardPipe("pull",
                purchasing.subscription("fulfil"), Scoped.relative("inbound"));
        // → DEFINE PIPE warehouse.pull FROM purchasing.fulfil
        //     TO FEED warehouse.inbound

        // Scoped.absolute is the handle-free explicit form — the name
        // stays verbatim wherever it is used.
        purchasing.defineForwardPipe("relay2",
                Scoped.relative("fulfil"), Scoped.absolute("warehouse.inbound"));

        var pipe = purchasing.pipe("relay");
        var info = pipe.describe();
        System.out.println(info.name() + " stage=" + info.stage()
                + " status=" + info.status());
    }
}
