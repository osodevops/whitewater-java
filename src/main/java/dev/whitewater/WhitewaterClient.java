package dev.whitewater;

import com.fasterxml.jackson.databind.JsonNode;
import dev.whitewater.internal.Transport;
import dev.whitewater.internal.Wire;
import dev.whitewater.model.ControlExecution;
import dev.whitewater.model.ReaderStart;

import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

/**
 * Entry point for a Whitewater Riverbed: logical resource management (WCL + typed commands), Writer,
 * Reader, and Subscription member clients. Any request may reach any
 * configured Node — routing is a server implementation detail.
 *
 * <pre>{@code
 * var client = WhitewaterClient.builder()
 *         .endpoints(URI.create("http://localhost:7071"))
 *         .apiKey(System.getenv("WHITEWATER_API_KEY"))
 *         .build();
 * var writer = client.writer("orders.checkout", 1);
 * var result = writer.append(UUID.randomUUID(), "order-1".getBytes(), payload);
 * }</pre>
 */
public final class WhitewaterClient implements AutoCloseable {

    public static final String SPEC_VERSION = "1.0.0";
    public static final int PROTOCOL_MAJOR = 1;

    private final Transport transport;
    private final Executor executor;

    private WhitewaterClient(Transport transport, Executor executor) {
        this.transport = transport;
        this.executor = executor;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Execute a WCL script synchronously. Identical {@code requestId}
     * retries return the original committed execution.
     */
    public ControlExecution wcl(String script) {
        return wcl(script, UUID.randomUUID());
    }

    /** Execute a WCL script under an explicit request identity. */
    public ControlExecution wcl(String script, UUID requestId) {
        var body = Transport.json().createObjectNode();
        body.put("script", script);
        if (requestId != null) {
            body.put("request_id", requestId.toString());
        }
        return Wire.controlExecution(transport.post("/v1/admin/wcl", body));
    }

    /**
     * Execute typed control commands synchronously — the same JSON shape the
     * Control Plane accepts over {@code POST /v1/admin/commands}, e.g.
     * {@code Map.of("command", "show", "kind", "domains")}.
     */
    public ControlExecution commands(List<Map<String, Object>> commands) {
        return commands(commands, UUID.randomUUID());
    }

    /** Execute typed commands under an explicit request identity. */
    public ControlExecution commands(List<Map<String, Object>> commands, UUID requestId) {
        var body = Transport.json().createObjectNode();
        if (requestId != null) {
            body.put("request_id", requestId.toString());
        }
        body.set("commands", Transport.json().valueToTree(commands));
        return Wire.controlExecution(transport.post("/v1/admin/commands", body));
    }

    public CompletableFuture<ControlExecution> wclAsync(String script) {
        return CompletableFuture.supplyAsync(() -> wcl(script), executor);
    }

    /** Liveness of the contacted Node. */
    public boolean healthy() {
        JsonNode health = transport.get("/health");
        return "ok".equals(health.path("status").asText());
    }

    // ---- Resource management -------------------------------------------------
    // Typed convenience over {@link #wcl}: each method builds one WCL statement
    // and executes it. Names are validated before interpolation, so a bad value
    // fails locally rather than reaching the Control Plane.

    /** {@code CREATE DOMAIN <name>} */
    public ControlExecution createDomain(String name) {
        return wcl("CREATE DOMAIN " + dotted(name));
    }

    /** {@code CREATE FEED <dotted.name>} */
    public ControlExecution createFeed(String name) {
        return wcl("CREATE FEED " + dotted(name));
    }

    /** {@code CREATE WRITER <name> TO <feed>} */
    public ControlExecution createWriter(String name, String feed) {
        return wcl("CREATE WRITER " + resource(name) + " TO " + dotted(feed));
    }

    /** {@code CREATE READER <name> FROM <feed>} — starts at BEGINNING. */
    public ControlExecution createReader(String name, String feed) {
        return createReader(name, feed, ReaderStart.beginning());
    }

    /** {@code CREATE READER <name> FROM <feed> START AT <start>} */
    public ControlExecution createReader(String name, String feed, ReaderStart start) {
        return wcl("CREATE READER " + resource(name) + " FROM " + dotted(feed)
                + " START AT " + start.wcl());
    }

    /** {@code CREATE SUBSCRIPTION <name> FROM <feed>} — starts at BEGINNING. */
    public ControlExecution createSubscription(String name, String feed) {
        return createSubscription(name, feed, ReaderStart.beginning());
    }

    /** {@code CREATE SUBSCRIPTION <name> FROM <feed> START AT <start>} */
    public ControlExecution createSubscription(String name, String feed, ReaderStart start) {
        return wcl("CREATE SUBSCRIPTION " + resource(name) + " FROM " + dotted(feed)
                + " START AT " + start.wcl());
    }

    /** {@code CREATE ROLE <name>} */
    public ControlExecution createRole(String name) {
        return wcl("CREATE ROLE " + resource(name));
    }

    /** {@code OPEN WRITER SESSION <name>}; the returned epoch feeds {@link #writer}. */
    public ControlExecution openWriterSession(String writer) {
        return wcl("OPEN WRITER SESSION " + resource(writer));
    }

    /** {@code SEEK READER <name> TO <start>} — repositions a durable Reader. */
    public ControlExecution seekReader(String reader, ReaderStart start) {
        return wcl("SEEK READER " + resource(reader) + " TO " + start.wcl());
    }

    /** {@code DESCRIBE DOMAIN|FEED <name>} */
    public ControlExecution describeDomain(String name) {
        return describe("DOMAIN", name, true);
    }

    public ControlExecution describeFeed(String name) {
        return describe("FEED", name, true);
    }

    public ControlExecution describeWriter(String name) {
        return describe("WRITER", name, false);
    }

    public ControlExecution describeReader(String name) {
        return describe("READER", name, false);
    }

    public ControlExecution describeSubscription(String name) {
        return describe("SUBSCRIPTION", name, false);
    }

    public ControlExecution describeRole(String name) {
        return describe("ROLE", name, false);
    }

    /** {@code RENAME <kind> <current> TO <new>} — feed/domain names must be dotted. */
    public ControlExecution renameDomain(String current, String renamed) {
        return rename("DOMAIN", current, renamed, true);
    }

    public ControlExecution renameFeed(String current, String renamed) {
        return rename("FEED", current, renamed, true);
    }

    public ControlExecution renameWriter(String current, String renamed) {
        return rename("WRITER", current, renamed, false);
    }

    public ControlExecution renameReader(String current, String renamed) {
        return rename("READER", current, renamed, false);
    }

    public ControlExecution renameSubscription(String current, String renamed) {
        return rename("SUBSCRIPTION", current, renamed, false);
    }

    public ControlExecution renameRole(String current, String renamed) {
        return rename("ROLE", current, renamed, false);
    }

    /** {@code DROP <kind> <name>} */
    public ControlExecution dropDomain(String name) {
        return drop("DOMAIN", name, true);
    }

    public ControlExecution dropFeed(String name) {
        return drop("FEED", name, true);
    }

    public ControlExecution dropWriter(String name) {
        return drop("WRITER", name, false);
    }

    public ControlExecution dropReader(String name) {
        return drop("READER", name, false);
    }

    public ControlExecution dropSubscription(String name) {
        return drop("SUBSCRIPTION", name, false);
    }

    public ControlExecution dropRole(String name) {
        return drop("ROLE", name, false);
    }

    /** {@code SHOW <kind>} — returns the execution; rows live in {@code results[0].data}. */
    public ControlExecution listDomains() {
        return show("DOMAINS");
    }

    public ControlExecution listFeeds() {
        return show("FEEDS");
    }

    public ControlExecution listWriters() {
        return show("WRITERS");
    }

    public ControlExecution listReaders() {
        return show("READERS");
    }

    public ControlExecution listSubscriptions() {
        return show("SUBSCRIPTIONS");
    }

    public ControlExecution listRoles() {
        return show("ROLES");
    }

    public ControlExecution listGrants() {
        return show("GRANTS");
    }

    private ControlExecution show(String kind) {
        return wcl("SHOW " + kind);
    }

    private ControlExecution describe(String kind, String name, boolean dottedName) {
        return wcl("DESCRIBE " + kind + " " + name(kind, name, dottedName));
    }

    private ControlExecution rename(String kind, String current, String renamed, boolean dottedName) {
        return wcl("RENAME " + kind + " " + name(kind, current, dottedName)
                + " TO " + name(kind, renamed, dottedName));
    }

    private ControlExecution drop(String kind, String name, boolean dottedName) {
        return wcl("DROP " + kind + " " + name(kind, name, dottedName));
    }

    private String name(String kind, String value, boolean dottedName) {
        return dottedName ? dotted(value) : resource(value);
    }

    /** Lowercase dotted names: {@code ^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)*$}, ≤512 bytes. */
    private static String dotted(String name) {
        Objects.requireNonNull(name, "name is required");
        if (name.length() > 512 || name.startsWith(".") || name.endsWith(".")
                || !name.matches("[a-z][a-z0-9]*(\\.[a-z][a-z0-9]*)*")) {
            throw new IllegalArgumentException("invalid dotted name: " + name);
        }
        return name;
    }

    /** Single-segment resource names (writers, readers, subscriptions, roles). */
    private static String resource(String name) {
        Objects.requireNonNull(name, "name is required");
        if (!name.matches("[A-Za-z][A-Za-z0-9_-]{0,127}")) {
            throw new IllegalArgumentException("invalid resource name: " + name);
        }
        return name;
    }

    /** Ad-hoc Feed operations: committed reads, temporary fetches, appends. */
    public FeedClient feeds() {
        return new FeedClient(transport, executor);
    }

    /**
     * A Writer session facade. The epoch comes from
     * {@code OPEN WRITER SESSION <name>} via {@link WhitewaterClient#wcl};
     * it fences every append in this session.
     */
    public WriterSessionClient writer(String writer, long sessionEpoch) {
        return new WriterSessionClient(transport, executor, writer, sessionEpoch);
    }

    /** A named-Reader session facade. */
    public ReaderSessionClient reader(String reader) {
        return new ReaderSessionClient(transport, executor, reader);
    }

    /** A cooperating member of a named Subscription. */
    public SubscriptionMemberClient subscriptionMember(String subscription) {
        return new SubscriptionMemberClient(transport, subscription);
    }

    @Override
    public void close() {
        // java.net.http.HttpClient needs no explicit shutdown.
    }

    public static final class Builder {
        private List<URI> endpoints = List.of();
        private String apiKey;
        private Duration timeout = Duration.ofSeconds(10);
        private Executor executor = ForkJoinPool.commonPool();

        public Builder endpoints(URI... endpoints) {
            this.endpoints = Arrays.asList(endpoints);
            return this;
        }

        public Builder endpoints(List<URI> endpoints) {
            this.endpoints = endpoints;
            return this;
        }

        public Builder apiKey(String apiKey) {
            this.apiKey = apiKey;
            return this;
        }

        public Builder connectTimeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder executor(Executor executor) {
            this.executor = executor;
            return this;
        }

        public WhitewaterClient build() {
            Objects.requireNonNull(apiKey, "apiKey is required");
            return new WhitewaterClient(new Transport(endpoints, apiKey, timeout), executor);
        }
    }
}
