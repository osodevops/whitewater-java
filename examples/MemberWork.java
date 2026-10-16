import dev.whitewater.WhitewaterClient;
import dev.whitewater.model.FeedRecord;
import dev.whitewater.model.ReaderStart;

import java.net.URI;
import java.nio.charset.StandardCharsets;

/**
 * Cooperative consumption: named Subscription members claim work items
 * under epoch-fenced leases. The session-bound {@link dev.whitewater.Member}
 * joins internally and owns the member identity, epoch, and lease
 * plumbing — the worker loop reads as claim → fetch → ack → release.
 *
 * Multiple members of one Subscription share the committed frontier;
 * acknowledgement is fenced at commit, so overlapping delivery replays
 * rather than double-commits.
 */
public class MemberWork {

    public static void main(String[] args) throws Exception {
        var client = WhitewaterClient.builder()
                .endpoints(
                        URI.create("http://localhost:7371"),
                        URI.create("http://localhost:7372"),
                        URI.create("http://localhost:7373"))
                .apiKey("whitewater-local-development-admin-key")
                .build();

        client.createDomain("demo");
        client.createFeed("demo.work");
        client.createSubscription("demo-audit", "demo.work", ReaderStart.beginning());

        var member = client.member("demo-audit");

        var work = member.claim();
        try {
            var page = member.fetch(work, 64);
            for (FeedRecord r : page.records()) {
                System.out.printf("%s (attempt %d)%n",
                        new String(r.payload(), StandardCharsets.UTF_8), page.deliveryAttempt());
            }
            // Commit the shared frontier AFTER processing — anything
            // unacknowledged may replay to another claim.
            member.ack(work, page);
        } finally {
            member.release(work);
        }

        // Transfer is for explicit handoff between live members.
        var peer = client.member("demo-audit");
        var pending = member.claim();
        member.transfer(pending, peer, 60);
        System.out.println("work handed to " + peer.memberId());
    }
}
