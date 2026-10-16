import dev.whitewater.WhitewaterClient;
import dev.whitewater.admin.Commands;
import dev.whitewater.model.IndexPredicate;
import dev.whitewater.model.Extractor;
import dev.whitewater.model.Permission;
import dev.whitewater.model.ReaderStart;
import dev.whitewater.model.ResourceKind;
import dev.whitewater.model.ShowKind;

import java.net.URI;
import java.util.List;
import java.util.Set;

/**
 * Two programmatic surfaces for resource management:
 *
 *   1. WCL text via {@code client.wcl(script)} — the full language.
 *   2. Typed commands via {@code client.commands(Commands.of()...)} —
 *      the same Command enum WCL lowers to, validated client-side.
 *
 * Prefer Commands for application code (typed, refactorable, no string
 * interpolation); use wcl() for one-off scripts and operator tooling.
 */
public class AdminCommands {

    public static void main(String[] args) {
        var client = WhitewaterClient.builder()
                .endpoints(
                        URI.create("http://localhost:7371"),
                        URI.create("http://localhost:7372"),
                        URI.create("http://localhost:7373"))
                .apiKey("whitewater-local-development-admin-key")
                .build();

        // --- WCL route ---
        client.wcl("""
                CREATE DOMAIN ops;
                CREATE FEED ops.audit;
                CREATE READER ops-audit-tail FROM ops.audit START AT BEGINNING;
                """);

        // --- typed commands route: identical catalog semantics ---
        var exec = client.commands(Commands.of()
                .createDomain("orders")
                .createFeed("orders.events")
                .createWriter("ingest", "orders.events")
                .createReader("audit", "orders.events", ReaderStart.beginning())
                .createSubscription("audit-sub", "orders.events")
                .createRole("auditor")
                .grant(Set.of(Permission.READ), "orders.events", "auditor")
                .defineIndex("orders.events.bystatus", "orders.events",
                        List.of(Extractor.metadata("status"))));

        System.out.println("executed " + exec.results().size()
                + " commands at revision " + exec.revision());

        // introspection is typed too
        var feeds = client.commands(Commands.of().show(ShowKind.FEEDS));
        feeds.results().forEach(r -> System.out.println(r.data()));

        // index reads run through commands or the query endpoint
        var page = client.queryIndex("orders.events.bystatus",
                IndexPredicate.exact("open"), 64, false, null);
        System.out.println("index page: " + page.rows().size()
                + " rows, behind=" + page.behind());

        // operator surfaces
        var state = client.subscriptionMemberState("audit-sub");
        System.out.println("subscription epoch " + state.ownershipEpoch()
                + ", members " + state.memberEpochs());
    }
}
