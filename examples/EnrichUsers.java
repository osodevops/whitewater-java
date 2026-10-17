import dev.whitewater.Feed;
import dev.whitewater.FeedWriter;
import dev.whitewater.StateStore;
import dev.whitewater.WhitewaterClient;
import dev.whitewater.model.FeedEvent;
import dev.whitewater.model.FeedMessage;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.IsoFields;
import java.util.Map;
import java.util.UUID;

/**
 * A permanent enrichment consumer — the Kafka-streams shape, with the
 * plumbing inside the SDK. Reads users.update.request, enriches each
 * record from the users.profiles StateStore (primary key, then the
 * user_id named key), writes users.update.enriched.request or the DLQ,
 * and bumps day/week/month counters.
 *
 * The named Reader "enricher" owns durable server-side progress: a
 * pod restart or fenced session resumes exactly where it acked.
 */
public class EnrichUsers {

    public record UpdateRequest(String requestId, String userEmail,
                                String userId, Map<String, String> changes) {}

    public record EnrichedUpdate(String requestId, String userId,
                                 String email, String displayName,
                                 String region, Map<String, String> changes) {}

    public record UserProfile(String userId, String email,
                              String displayName, String region) {}

    public record DeadLetter(String reason, String sourceFeed,
                             String sourceMessageId, String sourceCursor,
                             UpdateRequest original) {}

    public static void main(String[] args) {
        var client = WhitewaterClient.builder()
                .endpoints(
                        URI.create("http://localhost:7371"),
                        URI.create("http://localhost:7372"),
                        URI.create("http://localhost:7373"))
                .apiKey(System.getenv().getOrDefault(
                        "WHITEWATER_API_KEY",
                        "whitewater-local-development-admin-key"))
                .build();

        Feed<String, UpdateRequest> in =
                client.feed("users.update.request", UpdateRequest.class);
        Feed<String, EnrichedUpdate> out =
                client.feed("users.update.enriched.request", EnrichedUpdate.class);
        Feed<String, DeadLetter> dlq =
                client.feed("users.update.dlq", DeadLetter.class);
        StateStore<String, UserProfile> users =
                client.stateStore("users.profiles", UserProfile.class);
        StateStore<String, Long> counters =
                client.stateStore("users.stats", Long.class);

        FeedWriter<String, EnrichedUpdate> outWriter = out.writer("enricher");
        FeedWriter<String, DeadLetter> dlqWriter = dlq.writer("enricher");

        // The whole "permanent consumer loop" — supervision, restart
        // backoff, durable resume, ack-after-handler are all inside run().
        var consumer = in.consumer("enricher")
                .onRecord(msg -> process(msg, users, counters, outWriter, dlqWriter))
                .onError(err -> System.err.println("consumer retrying: " + err));
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));
        consumer.run();
    }

    static void process(FeedMessage<String, UpdateRequest> msg,
                        StateStore<String, UserProfile> users,
                        StateStore<String, Long> counters,
                        FeedWriter<String, EnrichedUpdate> out,
                        FeedWriter<String, DeadLetter> dlq) {
        UpdateRequest req = msg.value();

        var user = users.find(req.userEmail())
                .or(() -> users.findBy("user_id", req.userId()));

        // Idempotent effect identity: a replay after a crash re-sends
        // the same requestId, which dedups to the original append.
        UUID effectId = UUID.nameUUIDFromBytes(
                msg.messageId().toString().getBytes(StandardCharsets.UTF_8));

        if (user.isEmpty()) {
            dlq.append(new FeedEvent<>(
                            req.userEmail(),
                            new DeadLetter("user not found",
                                    "users.update.request",
                                    msg.messageId().toString(),
                                    msg.cursor().toString(), req))
                    .withRequestId(effectId)
                    .withTextMetadata(Map.of("reason", "enrichment-miss")));
            return;
        }

        var profile = user.get();
        out.append(new FeedEvent<>(
                        profile.userId(),
                        new EnrichedUpdate(req.requestId(), profile.userId(),
                                profile.email(), profile.displayName(),
                                profile.region(), req.changes()))
                .withRequestId(effectId));

        var day = LocalDate.now(ZoneOffset.UTC);
        increment(counters, "updates.day." + day);
        increment(counters, "updates.week." + day.getYear()
                + "-W" + day.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR));
        increment(counters, "updates.month." + day.getYear()
                + "-" + String.format("%02d", day.getMonthValue()));
    }

    static void increment(StateStore<String, Long> counters, String bucket) {
        counters.put(bucket, counters.getOrDefault(bucket, 0L) + 1);
    }
}
