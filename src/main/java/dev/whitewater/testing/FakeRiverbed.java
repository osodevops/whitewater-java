package dev.whitewater.testing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dev.whitewater.WhitewaterClient;
import dev.whitewater.internal.Transport;
import dev.whitewater.internal.Wire;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * An in-JVM Riverbed double for unit tests. It speaks the real
 * whitewater-protocol v1 wire contract over a loopback HTTP port, so the
 * whole client path — serialization, endpoint round-robin, and
 * (status, code) error mapping — is exercised exactly as against live
 * Nodes:
 *
 * <pre>{@code
 * try (var riverbed = FakeRiverbed.start();
 *      var client = riverbed.client()) {
 *     var writer = client.feed("orders.events", Order.class).writer("orders.ingest");
 *     writer.append("order-1", new Order("order-1", 42));
 *     try (var reader = client.feed("orders.events", Order.class).reader("orders.audit", 64)) {
 *         var page = reader.fetch();
 *         assertEquals(1, page.records().size());
 *         reader.ack();
 *     }
 * }
 * }</pre>
 *
 * <p>The fake models the protocol's real semantics rather than scripted
 * responses:
 * <ul>
 *   <li>Appends commit ordered records with opaque cursors and fresh
 *       {@code message_id}s; an identical {@code request_id} retry
 *       returns the original committed result with
 *       {@code deduplicated: true}.</li>
 *   <li>Writer sessions fence on open: a superseded
 *       {@code session_epoch} is refused as {@code stale_epoch} (the SDK
 *       maps it to {@link dev.whitewater.WhitewaterException.StaleSession}).</li>
 *   <li>Reader delivery is at-least-once: a fetched page must be acked;
 *       closing a session with delivered-but-unacked work re-delivers it
 *       on the next open.</li>
 *   <li>StateStores keep a real key/value model: put/delete/lookup/
 *       named get-by/ordered iterate/clear, with {@code mutation_id}
 *       dedup and conflict on reuse with different content.</li>
 *   <li>WCL and typed admin commands accept declarations and lifecycle
 *       statements, so writers, readers, subscriptions, and stores are
 *       declared exactly as in production code.</li>
 * </ul>
 *
 * <p>It is deliberately single-JVM and deterministic — no quorum, no
 * persistence, no replication. Unknown paths answer a protocol-shaped
 * 404, which the client surfaces as
 * {@link dev.whitewater.WhitewaterException.Refused}.
 */
public final class FakeRiverbed implements AutoCloseable {

    private final HttpServer server;
    private final ExecutorService pool;
    private final Object lock = new Object();

    // ---- wire state ---------------------------------------------------------
    // Feed name -> committed records (insertion order).
    private final Map<String, List<Rec>> feeds = new LinkedHashMap<>();
    private final Map<String, Rec> byCursor = new HashMap<>();
    // Domain name -> space_id (CREATE DOMAIN registers; feeds scope by prefix).
    private final Map<String, String> domainIds = new LinkedHashMap<>();
    private final Map<String, ObjectNode> schemas = new HashMap<>();
    private final Map<String, String> writerFeed = new HashMap<>();
    private final Map<String, WriterSession> writers = new HashMap<>();
    private final Map<String, Reader> readers = new HashMap<>();
    private final Map<String, Store> stores = new HashMap<>();
    private final Map<String, Sub> subscriptions = new HashMap<>();
    // Ad-hoc /v1/feeds/append idempotency: feed|session_id|request_id -> result.
    private final Map<String, SavedAppend> adHocDedup = new HashMap<>();
    private final Map<String, Long> adHocSequence = new HashMap<>();
    private long revision;
    private long cursorSeq;
    private int requests;

    private static final class Rec {
        String feed;
        int index;          // position inside its feed list
        String cursor;
        String messageId;
        String producerId;
        long sequence;
        long eventTimeNs;
        long ingestTimeNs;
        JsonNode key;       // base64 value node
        JsonNode payload;
        JsonNode metadata;  // {name: base64}
    }

    private static final class WriterSession {
        long epoch;
        String sessionId;
        final Map<String, SavedAppend> dedup = new HashMap<>();   // epoch|request_id
    }

    private static final class SavedAppend {
        String messageId;
        String cursor;
    }

    private static final class Reader {
        String feed;
        final String readerId = UUID.randomUUID().toString();
        long sessionEpoch;
        int capacity = 64;
        int acked;          // durable frontier index
        int delivered;      // session frontier index
        boolean open;
    }

    private static final class Row {
        byte[] payload;
        long appliedSeq;
        Map<String, List<JsonNode>> named;   // named_key -> typed values
    }

    private static final class Store {
        final String name;
        final String storeId = UUID.randomUUID().toString();
        boolean feedSourced;
        long seq;
        final TreeMap<Key, Row> rows = new TreeMap<>();
        final Map<String, SavedMutation> dedup = new HashMap<>();

        Store(String name) {
            this.name = name;
        }
    }

    private static final class SavedMutation {
        String opJson;
        long sequence;
    }

    /** Byte-array key in unsigned lexicographic order (the wire's row order). */
    private static final class Key implements Comparable<Key> {
        final byte[] bytes;

        Key(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public int compareTo(Key other) {
            return Arrays.compareUnsigned(bytes, other.bytes);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Key k && Arrays.equals(bytes, k.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }

    private static final class Sub {
        String feed;
        String domain;
        long ownershipEpoch = 1;
        long tick;
        int frontier;       // shared fetch frontier index
        long leaseSeq;
        final Map<String, Long> memberEpochs = new HashMap<>();
        final Map<String, Lease> leases = new HashMap<>(); // work_id -> lease
    }

    private static final class Lease {
        String memberId;
        long memberEpoch;
        long leaseEpoch;
        long expiresAtTick;
    }

    private static final class Reply {
        final int status;
        final JsonNode body;

        Reply(int status, JsonNode body) {
            this.status = status;
            this.body = body;
        }
    }

    // ---- lifecycle -----------------------------------------------------------

    private FakeRiverbed(HttpServer server) {
        this.server = server;
        this.pool = Executors.newCachedThreadPool();
        server.setExecutor(pool);
        server.createContext("/", this::handle);
        server.start();
    }

    /**
     * Bind a loopback port and start serving the protocol. The returned
     * fake is {@link AutoCloseable} — try-with-resources stops it.
     */
    public static FakeRiverbed start() {
        try {
            return new FakeRiverbed(HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0));
        } catch (IOException e) {
            throw new UncheckedIOException("FakeRiverbed could not bind a loopback port", e);
        }
    }

    /** The endpoint list {@link WhitewaterClient.Builder#endpoints} takes. */
    public List<URI> endpoints() {
        return List.of(endpoint());
    }

    /** This fake's single loopback endpoint. */
    public URI endpoint() {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
    }

    /** A connected {@link WhitewaterClient} (test api key). */
    public WhitewaterClient client() {
        return client("test-key");
    }

    /** A connected {@link WhitewaterClient} under an explicit key. */
    public WhitewaterClient client(String apiKey) {
        return WhitewaterClient.builder().endpoints(endpoints()).apiKey(apiKey).build();
    }

    /** Committed records on {@code feed}. */
    public int feedSize(String feed) {
        synchronized (lock) {
            return feeds.getOrDefault(feed, List.of()).size();
        }
    }

    /** Rows in the named StateStore. */
    public int storeSize(String store) {
        synchronized (lock) {
            Store s = stores.get(store);
            return s == null ? 0 : s.rows.size();
        }
    }

    /** The named Writer's current fencing epoch (0 when never opened). */
    public long writerEpoch(String writer) {
        synchronized (lock) {
            WriterSession s = writers.get(writer);
            return s == null ? 0 : s.epoch;
        }
    }

    /** The named Reader's durable acknowledged frontier — records committed. */
    public int readerProgress(String reader) {
        synchronized (lock) {
            Reader r = readers.get(reader);
            return r == null ? 0 : r.acked;
        }
    }

    /** Total requests the fake has served (for wiring assertions). */
    public int requests() {
        synchronized (lock) {
            return requests;
        }
    }

    @Override
    public void close() {
        server.stop(0);
        pool.shutdown();
    }

    // ---- HTTP plumbing --------------------------------------------------------

    private void handle(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        Map<String, String> query = query(exchange.getRequestURI().getRawQuery());
        JsonNode body;
        try {
            byte[] bytes = exchange.getRequestBody().readAllBytes();
            body = bytes.length == 0 ? null : Transport.json().readTree(bytes);
        } catch (IOException e) {
            body = null;
        }
        Reply reply;
        synchronized (lock) {
            requests++;
            try {
                reply = route(exchange.getRequestMethod(), path, query, body);
            } catch (RuntimeException e) {
                reply = error(500, null, "fake failure: " + e.getMessage());
            }
        }
        byte[] out = Transport.json().writeValueAsBytes(reply.body);
        exchange.getResponseHeaders().set("content-type", "application/json");
        exchange.sendResponseHeaders(reply.status, out.length);
        exchange.getResponseBody().write(out);
        exchange.close();
    }

    private Reply route(String method, String path, Map<String, String> query, JsonNode body) {
        switch (method + " " + path) {
            case "GET /health" -> {
                return ok(health());
            }
            case "POST /v1/admin/wcl" -> {
                return ok(wcl(body));
            }
            case "POST /v1/admin/commands" -> {
                return ok(commands(body));
            }
            case "POST /v1/writers/append" -> {
                return append(body);
            }
            case "POST /v1/writers/append-batch" -> {
                return appendBatch(body);
            }
            case "POST /v1/feeds/append" -> {
                return feedAppend(body);
            }
            case "GET /v1/feeds/records" -> {
                return ok(feedRecords(query));
            }
            case "POST /v1/readers/open" -> {
                return readerOpen(body);
            }
            case "POST /v1/readers/fetch" -> {
                return readerFetch(body);
            }
            case "POST /v1/readers/ack" -> {
                return readerAck(body);
            }
            case "POST /v1/readers/close" -> {
                return readerClose(body);
            }
            case "POST /v1/readers/temporary/fetch" -> {
                return ok(temporaryFetch(body));
            }
            case "POST /v1/state-stores/mutate" -> {
                return storeMutate(body);
            }
            case "POST /v1/state-stores/lookup" -> {
                return ok(storeLookup(body));
            }
            case "POST /v1/state-stores/get-by" -> {
                return ok(storeGetBy(body));
            }
            case "POST /v1/state-stores/iterate" -> {
                return ok(storeIterate(body));
            }
            case "POST /v1/state-stores/clear" -> {
                return storeClear(body);
            }
            case "POST /v1/state-stores/sync", "POST /v1/state-stores/rebuild" -> {
                return ok(storeSync(body));
            }
            case "POST /v1/indexes/query" -> {
                return ok(indexQuery(body));
            }
            case "POST /v1/subscriptions/seek" -> {
                return ok(subscriptionSeek(body));
            }
            case "GET /v1/subscriptions/members/state" -> {
                return ok(memberState(query));
            }
            case "POST /v1/subscriptions/members/join" -> {
                return memberJoin(body);
            }
            case "POST /v1/subscriptions/members/claim" -> {
                return memberClaim(body, false);
            }
            case "POST /v1/subscriptions/members/renew" -> {
                return memberClaim(body, true);
            }
            case "POST /v1/subscriptions/members/release" -> {
                return memberRelease(body);
            }
            case "POST /v1/subscriptions/members/transfer" -> {
                return memberTransfer(body);
            }
            case "POST /v1/subscriptions/members/fetch" -> {
                return memberFetch(body);
            }
            case "POST /v1/subscriptions/members/ack" -> {
                return memberAck(body);
            }
            case "GET /v1/node/metrics" -> {
                return ok(obj());
            }
            case "GET /v1/cluster/members" -> {
                ObjectNode node = obj();
                node.putArray("members").addObject()
                        .put("id", "fake-0")
                        .put("endpoint", endpoint().toString());
                return ok(node);
            }
            case "GET /v1/admin/subscriptions/progress" -> {
                return ok(obj());
            }
            default -> {
                if (method.equals("GET") && path.startsWith("/v1/admin/demand/")) {
                    String surface = path.substring("/v1/admin/demand/".length());
                    ObjectNode node = obj();
                    node.putArray(surface.equals("hot-keys") ? "hot_keys" : surface);
                    return ok(node);
                }
                return error(404, null, "unknown path " + path);
            }
        }
    }

    private Reply ok(JsonNode body) {
        return new Reply(200, body);
    }

    private static Reply error(int status, String code, String message) {
        ObjectNode body = obj().put("error", message);
        if (code != null) {
            body.put("code", code);
        }
        return new Reply(status, body);
    }

    private static ObjectNode obj() {
        return Transport.json().createObjectNode();
    }

    private static Map<String, String> query(String raw) {
        Map<String, String> out = new HashMap<>();
        if (raw == null || raw.isEmpty()) {
            return out;
        }
        for (String pair : raw.split("&")) {
            int eq = pair.indexOf('=');
            String k = eq < 0 ? pair : pair.substring(0, eq);
            String v = eq < 0 ? "" : pair.substring(eq + 1);
            out.put(k, URLDecoder.decode(v, StandardCharsets.UTF_8));
        }
        return out;
    }

    private static String uuid() {
        return UUID.randomUUID().toString();
    }

    private String nextCursor() {
        return String.format("wc-%016x-%04x", ++cursorSeq * 0x9E3779B97F4A7C15L,
                (int) (cursorSeq * 2654435761L) & 0xffff);
    }

    // ---- health ---------------------------------------------------------------

    private JsonNode health() {
        ObjectNode node = obj()
                .put("status", "ok")
                .put("node_id", "fake-0")
                .put("members", 3);
        node.putObject("protocol").put("major", 1).put("minor", 0);
        node.putObject("limits")
                .put("max_frame_bytes", 16L * 1024 * 1024)
                .put("max_committed_read_bytes", 16L * 1024 * 1024)
                .put("max_logical_read_frames", 64)
                .put("max_logical_read_ranges", 16)
                .put("subscription_member_fetch_max_limit", 256)
                .put("subscription_member_max_lease_ticks", 600);
        return node;
    }

    // ---- declarations (WCL + typed commands) ----------------------------------

    private JsonNode wcl(JsonNode body) {
        String script = body == null ? "" : body.path("script").asText("");
        String requestId = requestId(body);
        ArrayNode results = obj().putArray("results");
        for (String raw : script.split("[;\\n]")) {
            String stmt = raw.trim();
            if (stmt.isEmpty()) {
                continue;
            }
            ObjectNode result = obj()
                    .put("statement", stmt)
                    .put("message", "ok");
            result.set("data", wclStatement(stmt));
            results.add(result);
        }
        if (results.isEmpty()) {
            results.addObject().put("statement", script).put("message", "ok")
                    .set("data", obj());
        }
        return execution(requestId, results);
    }

    private JsonNode wclStatement(String stmt) {
        String[] tok = stmt.split("\\s+");
        ObjectNode data = obj();
        if (tok.length >= 5 && eq(tok, 0, "CREATE") && eq(tok, 1, "WRITER") && eq(tok, 3, "TO")) {
            // CREATE WRITER <name> TO <feed>
            writerFeed.put(tok[2], tok[4]);
            feeds.computeIfAbsent(tok[4], f -> new ArrayList<>());
        } else if (tok.length >= 5 && eq(tok, 0, "CREATE") && eq(tok, 1, "READER") && eq(tok, 3, "FROM")) {
            // CREATE READER <name> FROM <feed> [START AT <start>]
            Reader r = readers.computeIfAbsent(tok[2], n -> new Reader());
            r.feed = tok[4];
            feeds.computeIfAbsent(tok[4], f -> new ArrayList<>());
            if (r.acked == 0 && !r.open) {
                r.acked = r.delivered = startIndex(stmt, r.feed);
            }
        } else if (tok.length >= 5 && eq(tok, 0, "CREATE") && eq(tok, 1, "SUBSCRIPTION") && eq(tok, 3, "FROM")) {
            // CREATE SUBSCRIPTION <name> FROM <feed> [START AT <start>]
            Sub s = subscriptions.computeIfAbsent(tok[2], n -> new Sub());
            s.feed = tok[4];
            feeds.computeIfAbsent(tok[4], f -> new ArrayList<>());
        } else if (tok.length >= 3 && eq(tok, 0, "CREATE") && eq(tok, 1, "FEED")) {
            feeds.computeIfAbsent(tok[2], f -> new ArrayList<>());
        } else if (tok.length >= 3 && eq(tok, 0, "CREATE") && eq(tok, 1, "DOMAIN")) {
            domainIds.computeIfAbsent(tok[2], FakeRiverbed::spaceId);
        } else if (tok.length >= 3 && eq(tok, 0, "DESCRIBE")) {
            return describeRow(tok[1], tok[2]);
        } else if (tok.length >= 4 && eq(tok, 0, "OPEN") && eq(tok, 1, "WRITER")
                && eq(tok, 2, "SESSION")) {
            // OPEN WRITER SESSION <name>
            data.put("session_epoch", openWriterSession(tok[3]));
        } else if (tok.length >= 5 && eq(tok, 0, "REVOKE") && eq(tok, 1, "WRITER")
                && eq(tok, 2, "SESSION")) {
            // REVOKE WRITER SESSION <name> <epoch> — fences the current epoch
            WriterSession s = writers.get(tok[3]);
            if (s != null) {
                s.epoch++;
                s.sessionId = uuid();
            }
        } else if (tok.length >= 5 && eq(tok, 0, "SEEK") && eq(tok, 1, "READER")
                && eq(tok, 3, "TO")) {
            // SEEK READER <name> TO <start>
            Reader r = readers.get(tok[2]);
            if (r != null) {
                r.acked = r.delivered = startIndex(stmt.substring(stmt.indexOf(" TO ") + 4), r.feed);
            }
        } else if (tok.length >= 6 && eq(tok, 0, "SET") && eq(tok, 1, "SCHEMA")
                && eq(tok, 2, "ON")) {
            // SET SCHEMA ON DOMAIN|FEED <name> (KEY|VALUE <fmt> [SUBJECT 's'] [DEFINITION 'd'])+
            var scopeKey = (eq(tok, 3, "FEED") ? "feed:" : "domain:") + tok[4];
            ObjectNode schema = schemas.containsKey(scopeKey)
                    ? (ObjectNode) schemas.get(scopeKey).deepCopy() : obj();
            for (int i = 5; i < tok.length; i++) {
                if (!eq(tok, i, "KEY") && !eq(tok, i, "VALUE") || i + 1 >= tok.length) {
                    break;
                }
                var side = tok[i].toLowerCase();
                ObjectNode decl = obj().put("format", tok[i + 1].toLowerCase());
                i += 2;
                while (i < tok.length && (eq(tok, i, "SUBJECT") || eq(tok, i, "DEFINITION"))) {
                    var field = tok[i].toLowerCase();
                    var value = ++i < tok.length ? tok[i] : "";
                    decl.put(field, value.replaceAll("^'|'$", "").replace("''", "'"));
                    i++;
                }
                schema.set(side, decl);
                i--;
            }
            schemas.put(scopeKey, schema);
        } else if (tok.length >= 5 && eq(tok, 0, "DROP") && eq(tok, 1, "SCHEMA")
                && eq(tok, 2, "ON")) {
            // DROP SCHEMA ON DOMAIN|FEED <name>
            schemas.remove((eq(tok, 3, "FEED") ? "feed:" : "domain:") + tok[4]);
        } else if (tok.length >= 3 && eq(tok, 0, "DROP")) {
            switch (tok[1]) {
                case "WRITER" -> {
                    writers.remove(tok[2]);
                    writerFeed.remove(tok[2]);
                }
                case "READER" -> readers.remove(tok[2]);
                case "SUBSCRIPTION" -> subscriptions.remove(tok[2]);
                case "FEED" -> feeds.remove(tok[2]);
                default -> { }
            }
        } else if (tok.length >= 2 && eq(tok, 0, "SHOW")) {
            return showRows(tok[1].toLowerCase());
        }
        return data;
    }

    /** DESCRIBE <kind> <name> — the catalog row, or a missing-resource marker. */
    private JsonNode describeRow(String kind, String name) {
        ObjectNode row = obj();
        switch (kind.toUpperCase()) {
            case "DOMAIN", "SPACE" -> {
                var id = domainIds.get(name);
                if (id == null) {
                    row.put("error", "Domain " + name + " not found");
                    return row;
                }
                row.put("space_id", id).put("name", name)
                        .put("status", "active").put("created_at_ns", 0);
                row.putNull("history_policy");
                row.set("schema", schemaOrNull("domain:" + name));
            }
            case "FEED" -> {
                if (!feeds.containsKey(name)) {
                    row.put("error", "Feed " + name + " not found");
                    return row;
                }
                row.put("feed_id", feedId(name)).put("name", name);
                var space = spaceIdOf(name);
                if (space == null) {
                    row.putNull("space_id");
                } else {
                    row.put("space_id", space);
                }
                row.put("storage_name", name).put("status", "active").put("created_at_ns", 0);
                row.set("schema", schemaOrNull("feed:" + name));
                row.set("effective_schema", effectiveSchema(name));
            }
            default -> row.put("error", "unsupported DESCRIBE " + kind);
        }
        return row;
    }

    /** SHOW <kind> — catalog rows in the real server's array shape. */
    private JsonNode showRows(String kind) {
        ArrayNode rows = obj().arrayNode();
        switch (kind) {
            case "domains", "spaces" -> {
                for (var entry : domainIds.entrySet()) {
                    rows.add(obj().put("space_id", entry.getValue())
                            .put("name", entry.getKey())
                            .put("status", "active").put("created_at_ns", 0));
                }
            }
            case "feeds" -> {
                for (var feed : feeds.keySet()) {
                    var row = obj().put("feed_id", feedId(feed)).put("name", feed)
                            .put("storage_name", feed)
                            .put("status", "active").put("created_at_ns", 0);
                    var space = spaceIdOf(feed);
                    if (space == null) {
                        row.putNull("space_id");
                    } else {
                        row.put("space_id", space);
                    }
                    row.set("schema", schemaOrNull("feed:" + feed));
                    row.set("effective_schema", effectiveSchema(feed));
                    rows.add(row);
                }
            }
            case "readers" -> {
                for (var entry : readers.entrySet()) {
                    var row = obj().put("reader_id", entry.getValue().readerId)
                            .put("name", entry.getKey())
                            .put("status", "active").put("created_at_ns", 0);
                    if (entry.getValue().feed == null) {
                        row.putNull("feed_id");
                    } else {
                        row.put("feed_id", feedId(entry.getValue().feed));
                    }
                    rows.add(row);
                }
            }
            case "writers" -> {
                for (var entry : writerFeed.entrySet()) {
                    rows.add(obj()
                            .put("writer_id", UUID.nameUUIDFromBytes(
                                    ("writer:" + entry.getKey()).getBytes(StandardCharsets.UTF_8))
                                    .toString())
                            .put("name", entry.getKey())
                            .put("feed_id", feedId(entry.getValue()))
                            .put("status", "active").put("created_at_ns", 0));
                }
            }
            case "subscriptions" -> {
                for (var entry : subscriptions.entrySet()) {
                    var row = obj()
                            .put("subscription_id", UUID.nameUUIDFromBytes(
                                    ("subscription:" + entry.getKey()).getBytes(StandardCharsets.UTF_8))
                                    .toString())
                            .put("name", entry.getKey())
                            .put("status", "active").put("created_at_ns", 0);
                    var space = entry.getValue().domain != null
                            ? domainIds.get(entry.getValue().domain)
                            : entry.getValue().feed == null ? null : spaceIdOf(entry.getValue().feed);
                    if (space == null) {
                        row.putNull("space_id");
                    } else {
                        row.put("space_id", space);
                    }
                    rows.add(row);
                }
            }
            case "schemas" -> {
                for (var entry : schemas.entrySet()) {
                    var colon = entry.getKey().indexOf(':');
                    rows.add(obj()
                            .put("scope_kind", entry.getKey().substring(0, colon))
                            .put("name", entry.getKey().substring(colon + 1))
                            .set("schema", entry.getValue()));
                }
            }
            case "statestores" -> {
                for (var entry : stores.entrySet()) {
                    var row = obj().put("store_id", entry.getValue().storeId)
                            .put("name", entry.getKey())
                            .put("status", "active").put("created_at_ns", 0);
                    var space = spaceIdOf(entry.getKey());
                    if (space == null) {
                        row.putNull("space_id");
                    } else {
                        row.put("space_id", space);
                    }
                    rows.add(row);
                }
            }
            default -> { }
        }
        return rows;
    }

    private JsonNode commands(JsonNode body) {
        String requestId = requestId(body);
        ArrayNode results = obj().putArray("results");
        JsonNode list = body == null ? null : body.path("commands");
        if (list != null && list.isArray()) {
            for (JsonNode command : list) {
                String name = command.path("command").asText("");
                ObjectNode result = obj()
                        .put("statement", name)
                        .put("message", "ok");
                result.set("data", typedCommand(name, command));
                results.add(result);
            }
        }
        if (results.isEmpty()) {
            results.addObject().put("statement", "").put("message", "ok").set("data", obj());
        }
        return execution(requestId, results);
    }

    private JsonNode typedCommand(String name, JsonNode command) {
        ObjectNode data = obj();
        switch (name) {
            case "open_writer_session" -> data.put("session_epoch",
                    openWriterSession(command.path("writer").asText()));
            case "revoke_writer_session" -> {
                WriterSession s = writers.get(command.path("writer").asText());
                if (s != null && s.epoch == command.path("session_epoch").asLong()) {
                    s.epoch++;
                    s.sessionId = uuid();
                }
            }
            case "create_domain" -> domainIds.computeIfAbsent(
                    command.path("name").asText(), FakeRiverbed::spaceId);
            case "create_feed" -> feeds.computeIfAbsent(
                    command.path("name").asText(), f -> new ArrayList<>());
            case "describe" -> {
                return describeRow(
                        command.path("kind").asText().toUpperCase(),
                        command.path("name").asText());
            }
            case "show" -> {
                return showRows(command.path("kind").asText().toLowerCase());
            }
            case "create_writer" -> {
                writerFeed.put(command.path("name").asText(), command.path("feed").asText());
                feeds.computeIfAbsent(command.path("feed").asText(), f -> new ArrayList<>());
            }
            case "create_reader" -> {
                Reader r = readers.computeIfAbsent(command.path("name").asText(), n -> new Reader());
                if (r.feed == null) {
                    r.feed = command.path("feed").asText();
                    feeds.computeIfAbsent(r.feed, f -> new ArrayList<>());
                    r.acked = r.delivered = startIndex(command.path("start"), r.feed);
                }
            }
            case "set_schema" -> {
                var scope = command.path("scope");
                var scopeKey = scope.path("kind").asText() + ":" + scope.path("name").asText();
                schemas.put(scopeKey, (ObjectNode) command.path("schema").deepCopy());
            }
            case "clear_schema" -> {
                var scope = command.path("scope");
                schemas.remove(scope.path("kind").asText() + ":" + scope.path("name").asText());
            }
            case "create_subscription" -> {
                Sub s = subscriptions.computeIfAbsent(command.path("name").asText(), n -> new Sub());
                if (s.feed == null && command.hasNonNull("feed")) {
                    s.feed = command.path("feed").asText();
                    feeds.computeIfAbsent(s.feed, f -> new ArrayList<>());
                    s.frontier = startIndex(command.path("start"), s.feed);
                }
                if (s.domain == null && command.hasNonNull("domain")) {
                    s.domain = command.path("domain").asText();
                }
            }
            case "seek_reader" -> {
                Reader r = readers.get(command.path("reader").asText());
                if (r != null) {
                    r.acked = r.delivered = startIndex(command.path("start"), r.feed);
                }
            }
            case "seek_subscription" -> {
                Sub s = subscriptions.get(command.path("name").asText());
                if (s != null) {
                    s.frontier = startIndex(command.path("start"), s.feed);
                }
            }
            case "define_state_store" -> {
                Store s = stores.computeIfAbsent(command.path("name").asText(), Store::new);
                s.feedSourced = "feed".equals(command.path("source").path("kind").asText());
            }
            case "drop" -> {
                String kind = command.path("kind").asText();
                String target = command.path("name").asText();
                switch (kind) {
                    case "state_store" -> stores.remove(target);
                    case "reader" -> readers.remove(target);
                    case "writer" -> {
                        writers.remove(target);
                        writerFeed.remove(target);
                    }
                    case "subscription" -> subscriptions.remove(target);
                    case "feed" -> feeds.remove(target);
                    default -> { }
                }
            }
            case "rename" -> {
                String kind = command.path("kind").asText();
                String current = command.path("current").asText();
                String renamed = command.path("new").asText();
                if ("feed".equals(kind) && feeds.containsKey(current)) {
                    feeds.put(renamed, feeds.remove(current));
                    writerFeed.replaceAll((w, f) -> f.equals(current) ? renamed : f);
                    readers.values().forEach(r -> {
                        if (current.equals(r.feed)) {
                            r.feed = renamed;
                        }
                    });
                }
            }
            default -> { }
        }
        return data;
    }

    private JsonNode execution(String requestId, ArrayNode results) {
        ObjectNode node = obj()
                .put("request_id", requestId)
                .put("revision", ++revision)
                .put("authority", "control_plane");
        node.putNull("warning");
        node.set("results", results);
        return node;
    }

    private static String requestId(JsonNode body) {
        String id = body == null ? null : body.path("request_id").asText(null);
        return id == null || id.isBlank() ? uuid() : id;
    }

    private static boolean eq(String[] tok, int i, String word) {
        return tok[i].equalsIgnoreCase(word);
    }

    private long openWriterSession(String writer) {
        WriterSession s = writers.computeIfAbsent(writer, w -> new WriterSession());
        s.epoch++;
        s.sessionId = uuid();
        return s.epoch;
    }

    /** Resolve a ReaderStart (WCL text or typed-command node) to a feed index. */
    private int startIndex(String clause, String feed) {
        List<Rec> recs = feeds.getOrDefault(feed, List.of());
        String upper = clause.toUpperCase();
        if (upper.contains("NOW")) {
            return recs.size();
        }
        int q = clause.indexOf('\'');
        if (upper.contains("CURSOR") && q >= 0) {
            int end = clause.indexOf('\'', q + 1);
            Rec r = byCursor.get(clause.substring(q + 1, end));
            return r != null && feed.equals(r.feed) ? r.index + 1 : recs.size();
        }
        if (upper.contains("TIMESTAMP")) {
            long ns = 0;
            for (String tok : clause.split("\\s+")) {
                if (tok.matches("-?\\d+")) {
                    ns = Long.parseLong(tok);
                }
            }
            return indexAtEventTime(recs, ns);
        }
        return 0;
    }

    private int startIndex(JsonNode start, String feed) {
        List<Rec> recs = feeds.getOrDefault(feed, List.of());
        if (start == null || !start.isObject()) {
            return 0;
        }
        switch (start.path("kind").asText("beginning")) {
            case "now" -> {
                return recs.size();
            }
            case "cursor" -> {
                Rec r = byCursor.get(start.path("cursor").asText());
                return r != null && feed.equals(r.feed) ? r.index + 1 : recs.size();
            }
            case "timestamp" -> {
                return indexAtEventTime(recs, start.path("cursor").asLong());
            }
            default -> {
                return 0;
            }
        }
    }

    private static int indexAtEventTime(List<Rec> recs, long ns) {
        for (int i = 0; i < recs.size(); i++) {
            if (recs.get(i).eventTimeNs >= ns) {
                return i;
            }
        }
        return recs.size();
    }

    // ---- writers ---------------------------------------------------------------

    private Reply append(JsonNode body) {
        String writer = body.path("writer").asText();
        long epoch = body.path("session_epoch").asLong();
        WriterSession s = writers.get(writer);
        if (s != null && epoch != s.epoch) {
            return error(409, "stale_epoch",
                    "writer " + writer + " session epoch " + epoch
                            + " superseded by epoch " + s.epoch);
        }
        if (s == null) {   // caller-managed session plumbing
            s = new WriterSession();
            s.epoch = epoch;
            s.sessionId = uuid();
            writers.put(writer, s);
        }
        String key = epoch + "|" + body.path("request_id").asText();
        SavedAppend prev = s.dedup.get(key);
        if (prev != null) {
            return ok(appendResult(prev, true));
        }
        String feed = writerFeed.getOrDefault(writer, writer);
        Rec rec = commit(feed, s.sessionId, body);
        SavedAppend saved = new SavedAppend();
        saved.messageId = rec.messageId;
        saved.cursor = rec.cursor;
        s.dedup.put(key, saved);
        return ok(appendResult(saved, false));
    }

    private Reply appendBatch(JsonNode body) {
        JsonNode records = body == null ? null : body.path("records");
        ArrayNode results = obj().putArray("results");
        if (records != null && records.isArray()) {
            for (JsonNode record : records) {
                Reply r = append(record);
                if (r.status / 100 != 2) {
                    return r;
                }
                results.add(r.body);
            }
        }
        ObjectNode node = obj();
        node.set("results", results);
        node.set("feedback", feedback());
        return ok(node);
    }

    private Reply feedAppend(JsonNode body) {
        String feed = body.path("feed").asText();
        String sessionId = body.path("writer_session_id").asText();
        long sequence = body.path("sequence").asLong();
        String dedupKey = feed + "|" + sessionId + "|" + body.path("request_id").asText();
        SavedAppend prev = adHocDedup.get(dedupKey);
        if (prev != null) {
            return ok(appendResult(prev, true));
        }
        long last = adHocSequence.getOrDefault(sessionId, -1L);
        if (sequence <= last) {
            return error(409, "sequence",
                    "writer sequence " + sequence + " stale (last " + last + ")");
        }
        Rec rec = commit(feed, sessionId, body);
        rec.sequence = sequence;
        adHocSequence.put(sessionId, sequence);
        SavedAppend saved = new SavedAppend();
        saved.messageId = rec.messageId;
        saved.cursor = rec.cursor;
        adHocDedup.put(dedupKey, saved);
        return ok(appendResult(saved, false));
    }

    private ObjectNode appendResult(SavedAppend saved, boolean deduplicated) {
        ObjectNode node = obj()
                .put("message_id", saved.messageId)
                .put("cursor", saved.cursor)
                .put("deduplicated", deduplicated)
                .put("durability", "committed");
        node.set("feedback", feedback());
        return node;
    }

    private static ObjectNode feedback() {
        return obj()
                .put("recommended_batch_count", 8)
                .put("recommended_batch_bytes", 262144)
                .put("pressure", 0.0)
                .put("retry_after_ms", 0)
                .put("max_frame_bytes", 16L * 1024 * 1024)
                .put("hot_key", false);
    }

    private Rec commit(String feed, String producerId, JsonNode body) {
        List<Rec> list = feeds.computeIfAbsent(feed, f -> new ArrayList<>());
        Rec rec = new Rec();
        rec.feed = feed;
        rec.index = list.size();
        rec.cursor = nextCursor();
        rec.messageId = uuid();
        rec.producerId = producerId;
        rec.sequence = list.size() + 1;
        rec.ingestTimeNs = System.currentTimeMillis() * 1_000_000L;
        String eventNs = body.path("event_time_ns").asText(null);
        rec.eventTimeNs = eventNs == null ? rec.ingestTimeNs : Long.parseLong(eventNs);
        rec.key = body.path("key_base64");
        rec.payload = body.path("payload_base64");
        rec.metadata = body.path("metadata_base64").isObject()
                ? body.path("metadata_base64") : obj();
        list.add(rec);
        byCursor.put(rec.cursor, rec);
        return rec;
    }

    // ---- feed reads ------------------------------------------------------------

    private JsonNode feedRecords(Map<String, String> query) {
        List<Rec> recs = feeds.getOrDefault(query.get("feed"), List.of());
        int from = afterIndex(query.get("after"), query.get("feed"));
        int limit = query.containsKey("limit") ? Integer.parseInt(query.get("limit")) : recs.size();
        ArrayNode out = obj().putArray("records");
        for (int i = from; i < Math.min(from + limit, recs.size()); i++) {
            out.add(record(recs.get(i)));
        }
        // feed.records returns a bare array, not an envelope.
        return out;
    }

    private JsonNode temporaryFetch(JsonNode body) {
        String feed = body.path("feed").asText();
        List<Rec> recs = feeds.getOrDefault(feed, List.of());
        int from;
        if (body.path("new_only").asBoolean(false)) {
            from = recs.size();
        } else if (body.hasNonNull("after_event_time_ns")) {
            from = indexAtEventTime(recs, Long.parseLong(body.path("after_event_time_ns").asText()));
        } else {
            from = afterIndex(body.path("after").asText(null), feed);
        }
        int limit = body.path("limit").isNumber() ? body.path("limit").asInt() : 256;
        ArrayNode out = obj().putArray("records");
        String next = null;
        for (int i = from; i < Math.min(from + limit, recs.size()); i++) {
            out.add(record(recs.get(i)));
            next = recs.get(i).cursor;
        }
        ObjectNode node = obj();
        node.set("records", out);
        if (next != null) {
            node.put("next_cursor", next);
        } else {
            node.putNull("next_cursor");
        }
        return node;
    }

    private int afterIndex(String after, String feed) {
        if (after == null || after.isBlank()) {
            return 0;
        }
        Rec r = byCursor.get(after);
        return r != null && feed.equals(r.feed) ? r.index + 1 : 0;
    }

    private JsonNode record(Rec rec) {
        ObjectNode node = obj()
                .put("cursor", rec.cursor)
                .put("message_id", rec.messageId)
                .put("producer_id", rec.producerId)
                .put("sequence", rec.sequence)
                .put("event_time_ns", Long.toString(rec.eventTimeNs))
                .put("ingest_time_ns", Long.toString(rec.ingestTimeNs));
        node.set("key_base64", rec.key);
        node.set("payload_base64", rec.payload);
        node.set("metadata_base64", rec.metadata);
        return node;
    }

    // ---- readers ----------------------------------------------------------------

    private Reply readerOpen(JsonNode body) {
        String name = body.path("reader").asText();
        Reader r = readers.computeIfAbsent(name, n -> new Reader());
        if (r.feed == null) {
            r.feed = name;   // undeclared readers bind a same-named feed
            feeds.computeIfAbsent(r.feed, f -> new ArrayList<>());
        }
        r.sessionEpoch++;
        r.delivered = r.acked;
        r.open = true;
        r.capacity = body.path("capacity").asInt(r.capacity);
        return ok(readerSession(r));
    }

    private Reply readerFetch(JsonNode body) {
        Reader r = readers.get(body.path("reader").asText());
        Reply stale = readerFence(r, body);
        if (stale != null) {
            return stale;
        }
        List<Rec> recs = feeds.getOrDefault(r.feed, List.of());
        int limit = body.path("limit").isNumber()
                ? Math.min(body.path("limit").asInt(), r.capacity) : r.capacity;
        ArrayNode records = obj().putArray("records");
        String delivered = null;
        for (int i = r.delivered; i < Math.min(r.delivered + limit, recs.size()); i++) {
            records.add(record(recs.get(i)));
            delivered = recs.get(i).cursor;
        }
        r.delivered = Math.min(r.delivered + limit, recs.size());
        ObjectNode node = obj();
        node.set("records", records);
        if (delivered != null) {
            node.put("delivered_cursor", delivered);
        } else {
            node.put("delivered_cursor", cursorAt(r.feed, r.delivered - 1));
        }
        node.put("acknowledged_cursor", cursorAt(r.feed, r.acked - 1));
        node.put("recommended_capacity", r.capacity);
        node.put("retry_after_ms", 0);
        return ok(node);
    }

    private Reply readerAck(JsonNode body) {
        Reader r = readers.get(body.path("reader").asText());
        Reply stale = readerFence(r, body);
        if (stale != null) {
            return stale;
        }
        Rec rec = byCursor.get(body.path("cursor").asText(null));
        if (rec != null && r.feed.equals(rec.feed)) {
            r.acked = rec.index + 1;
            r.delivered = Math.max(r.delivered, r.acked);
        }
        return ok(readerSession(r));
    }

    private Reply readerClose(JsonNode body) {
        Reader r = readers.get(body.path("reader").asText());
        Reply stale = readerFence(r, body);
        if (stale != null) {
            return stale;
        }
        r.open = false;
        r.delivered = r.acked;    // unacked delivered work replays on the next open
        return ok(readerSession(r));
    }

    private Reply readerFence(Reader r, JsonNode body) {
        if (r == null) {
            return error(404, null, "unknown reader");
        }
        long epoch = body.path("session_epoch").asLong();
        if (!r.open || epoch != r.sessionEpoch) {
            return error(409, "stale_epoch",
                    "reader " + body.path("reader").asText() + " session epoch " + epoch
                            + " superseded by epoch " + r.sessionEpoch);
        }
        return null;
    }

    private JsonNode readerSession(Reader r) {
        ObjectNode node = obj()
                .put("reader_id", r.readerId)
                .put("feed_id", feedId(r.feed))
                .put("session_epoch", r.sessionEpoch)
                .put("capacity", r.capacity);
        node.put("delivered_cursor", cursorAt(r.feed, r.delivered - 1));
        node.put("acknowledged_cursor", cursorAt(r.feed, r.acked - 1));
        return node;
    }

    private String cursorAt(String feed, int index) {
        List<Rec> recs = feeds.getOrDefault(feed, List.of());
        return index >= 0 && index < recs.size() ? recs.get(index).cursor : null;
    }

    private static String feedId(String feed) {
        return UUID.nameUUIDFromBytes(("feed:" + feed).getBytes(StandardCharsets.UTF_8)).toString();
    }

    private static String spaceId(String domain) {
        return UUID.nameUUIDFromBytes(("domain:" + domain).getBytes(StandardCharsets.UTF_8)).toString();
    }

    /** Longest-prefix Domain owning {@code name} — mirrors the server's owning_space. */
    private String owningDomain(String name) {
        String best = null;
        for (var domain : domainIds.keySet()) {
            if (name.startsWith(domain + ".") && (best == null || domain.length() > best.length())) {
                best = domain;
            }
        }
        return best;
    }

    /** The declared schema row for a scope key, or null. */
    private JsonNode schemaOrNull(String scopeKey) {
        return schemas.getOrDefault(scopeKey, null);
    }

    /** The feed's declared schema layered over its Domain's — null when undeclared. */
    private JsonNode effectiveSchema(String feedName) {
        var own = schemas.get("feed:" + feedName);
        var domain = owningDomain(feedName);
        var inherited = domain == null ? null : schemas.get("domain:" + domain);
        if (own == null) {
            return inherited;
        }
        if (inherited == null) {
            return own;
        }
        var merged = (ObjectNode) inherited.deepCopy();
        merged.setAll(own);
        return merged;
    }

    /** space_id of the Domain owning {@code resourceName}, or null when undeclared. */
    private String spaceIdOf(String resourceName) {
        var domain = owningDomain(resourceName);
        return domain == null ? null : domainIds.get(domain);
    }

    // ---- state stores ------------------------------------------------------------

    private Reply storeMutate(JsonNode body) {
        Store s = stores.computeIfAbsent(body.path("store").asText(), Store::new);
        if (s.feedSourced) {
            return error(400, null, "feed-sourced stores take replayed mutations, not client ops");
        }
        String mutationId = body.path("mutation_id").asText();
        String opJson = body.path("op").toString();
        SavedMutation prev = s.dedup.get(mutationId);
        if (prev != null) {
            if (prev.opJson.equals(opJson)) {
                return ok(mutationResult(s, mutationId, prev.sequence, true));
            }
            return error(409, null, "mutation_id reused with different content");
        }
        JsonNode op = body.path("op");
        switch (op.path("kind").asText()) {
            case "put_row" -> {
                Row row = new Row();
                row.payload = b64(op.path("payload_base64").asText(null));
                row.appliedSeq = ++s.seq;
                row.named = new LinkedHashMap<>();
                JsonNode named = op.path("named");
                if (named.isArray()) {
                    for (JsonNode entry : named) {
                        List<JsonNode> values = new ArrayList<>();
                        JsonNode vs = entry.path("values");
                        if (vs.isArray()) {
                            vs.forEach(v -> values.add(v.deepCopy()));
                        }
                        row.named.put(entry.path("name").asText(), values);
                    }
                }
                s.rows.put(new Key(b64(op.path("key_base64").asText())), row);
            }
            case "delete_row" -> {
                s.rows.remove(new Key(b64(op.path("key_base64").asText())));
                s.seq++;
            }
            default -> {
                return error(400, null, "unknown mutation op " + op.path("kind").asText());
            }
        }
        SavedMutation saved = new SavedMutation();
        saved.opJson = opJson;
        saved.sequence = s.seq;
        s.dedup.put(mutationId, saved);
        return ok(mutationResult(s, mutationId, s.seq, false));
    }

    private JsonNode mutationResult(Store s, String mutationId, long sequence, boolean deduplicated) {
        return obj()
                .put("store_id", s.storeId)
                .put("mutation_id", mutationId)
                .put("sequence", sequence)
                .put("deduplicated", deduplicated);
    }

    private JsonNode storeLookup(JsonNode body) {
        Store s = stores.computeIfAbsent(body.path("store").asText(), Store::new);
        Row row = s.rows.get(new Key(b64(body.path("key_base64").asText())));
        ObjectNode node = obj()
                .put("store_id", s.storeId)
                .put("found", row != null);
        if (row != null) {
            node.put("payload_base64", Wire.b64(row.payload));
            node.put("applied_cursor", Long.toString(row.appliedSeq));
        } else {
            node.putNull("payload_base64");
            node.putNull("applied_cursor");
        }
        return freshness(node, s);
    }

    private JsonNode storeGetBy(JsonNode body) {
        Store s = stores.computeIfAbsent(body.path("store").asText(), Store::new);
        String namedKey = body.path("named_key").asText();
        List<JsonNode> wanted = new ArrayList<>();
        JsonNode values = body.path("values");
        if (values.isArray()) {
            values.forEach(wanted::add);
        }
        int limit = body.path("limit").isNumber() ? body.path("limit").asInt() : 64;
        ArrayNode rows = obj().putArray("rows");
        for (Map.Entry<Key, Row> e : s.rows.entrySet()) {
            List<JsonNode> postings = e.getValue().named.getOrDefault(namedKey, List.of());
            boolean hit = postings.stream().anyMatch(p ->
                    wanted.stream().anyMatch(w -> sameValue(p, w)));
            if (!hit) {
                continue;
            }
            ObjectNode row = obj();
            row.put("key_base64", Wire.b64(e.getKey().bytes));
            row.put("payload_base64", Wire.b64(e.getValue().payload));
            row.put("applied_cursor", Long.toString(e.getValue().appliedSeq));
            rows.add(row);
            if (rows.size() >= limit) {
                break;
            }
        }
        ObjectNode node = obj().put("store_id", s.storeId);
        node.set("rows", rows);
        return freshness(node, s);
    }

    private static boolean sameValue(JsonNode a, JsonNode b) {
        if (!a.path("kind").asText().equals(b.path("kind").asText())) {
            return false;
        }
        return a.path("value").equals(b.path("value"))
                && a.path("value_base64").asText("").equals(b.path("value_base64").asText(""));
    }

    private JsonNode storeIterate(JsonNode body) {
        Store s = stores.computeIfAbsent(body.path("store").asText(), Store::new);
        byte[] after = b64(body.path("after_key_base64").asText(null));
        int limit = body.path("limit").isNumber() ? body.path("limit").asInt() : 256;
        ArrayNode rows = obj().putArray("rows");
        String next = null;
        Key lastKey = null;
        int count = 0;
        for (Map.Entry<Key, Row> e : s.rows.entrySet()) {
            if (after != null && Arrays.compareUnsigned(e.getKey().bytes, after) <= 0) {
                continue;
            }
            if (count >= limit) {
                next = lastKey == null ? null : Wire.b64(lastKey.bytes);
                break;
            }
            ObjectNode row = obj();
            row.put("key_base64", Wire.b64(e.getKey().bytes));
            row.put("payload_base64", Wire.b64(e.getValue().payload));
            row.put("applied_cursor", Long.toString(e.getValue().appliedSeq));
            rows.add(row);
            lastKey = e.getKey();
            count++;
        }
        ObjectNode node = obj().put("store_id", s.storeId);
        node.set("rows", rows);
        if (next != null) {
            node.put("next_key_base64", next);
        } else {
            node.putNull("next_key_base64");
        }
        node.put("pinned_seq", s.seq);
        node.put("served_by", "fake-0");
        return node;
    }

    private Reply storeClear(JsonNode body) {
        Store s = stores.computeIfAbsent(body.path("store").asText(), Store::new);
        if (s.feedSourced) {
            return error(400, null, "feed-sourced stores take replayed mutations, not client ops");
        }
        String mutationId = body.path("mutation_id").asText();
        SavedMutation prev = s.dedup.get(mutationId);
        if (prev != null) {
            if ("clear".equals(prev.opJson)) {
                return ok(mutationResult(s, mutationId, prev.sequence, true));
            }
            return error(409, null, "mutation_id reused with different content");
        }
        s.rows.clear();
        s.seq++;
        SavedMutation saved = new SavedMutation();
        saved.opJson = "clear";
        saved.sequence = s.seq;
        s.dedup.put(mutationId, saved);
        return ok(mutationResult(s, mutationId, s.seq, false));
    }

    private JsonNode storeSync(JsonNode body) {
        Store s = stores.computeIfAbsent(body.path("store").asText(), Store::new);
        ObjectNode node = obj()
                .put("store_id", s.storeId)
                .put("applied", 0)
                .put("complete", true);
        node.putNull("frontier");
        return node;
    }

    private ObjectNode freshness(ObjectNode node, Store s) {
        node.put("applied_seq", s.seq)
                .put("committed_seq", s.seq)
                .put("behind", false)
                .put("served_by", "fake-0");
        return node;
    }

    private static byte[] b64(String value) {
        return value == null || value.isEmpty() ? null : Wire.b64(value);
    }

    // ---- indexes -----------------------------------------------------------------

    private JsonNode indexQuery(JsonNode body) {
        ObjectNode node = obj()
                .put("index", body == null ? "" : body.path("index").asText(""))
                .put("generation", 1)
                .put("behind", false)
                .put("truncated", false)
                .put("served_by", "fake-0");
        node.putNull("index_id");
        node.putNull("applied_cursor");
        node.putNull("target_cursor");
        node.putArray("rows");
        return node;
    }

    // ---- subscription members -------------------------------------------------------

    private Reply memberJoin(JsonNode body) {
        Sub s = subscriptions.computeIfAbsent(body.path("subscription").asText(), n -> {
            Sub sub = new Sub();
            sub.feed = n;
            feeds.computeIfAbsent(n, f -> new ArrayList<>());
            return sub;
        });
        String memberId = body.path("member_id").asText();
        long epoch = s.memberEpochs.getOrDefault(memberId, 0L) + 1;
        s.memberEpochs.put(memberId, epoch);
        ObjectNode node = memberEnvelope(s);
        node.put("member_epoch", epoch);
        return ok(node);
    }

    private Reply memberClaim(JsonNode body, boolean renew) {
        Sub s = subscriptions.get(body.path("subscription").asText());
        Reply fenced = memberFence(s, body);
        if (fenced != null) {
            return fenced;
        }
        String workId = body.path("work_id").asText();
        String memberId = body.path("member_id").asText();
        Lease held = s.leases.get(workId);
        if (renew) {
            if (held == null || !held.memberId.equals(memberId)
                    || held.leaseEpoch != body.path("lease_epoch").asLong()) {
                return error(409, "lease.stale_lease", "lease " + workId + " no longer held");
            }
        } else if (held != null) {
            return error(409, "lease.busy", "work " + workId + " is claimed");
        }
        Lease lease = held == null ? new Lease() : held;
        lease.memberId = memberId;
        lease.memberEpoch = body.path("member_epoch").asLong();
        lease.leaseEpoch = ++s.leaseSeq;
        lease.expiresAtTick = s.tick + body.path("lease_ticks").asLong(30);
        s.leases.put(workId, lease);
        ObjectNode node = memberEnvelope(s);
        node.put("member_epoch", lease.memberEpoch);
        node.set("lease", leaseJson(workId, lease));
        return ok(node);
    }

    private Reply memberRelease(JsonNode body) {
        Sub s = subscriptions.get(body.path("subscription").asText());
        Reply fenced = memberFence(s, body);
        if (fenced != null) {
            return fenced;
        }
        s.leases.remove(body.path("work_id").asText());
        ObjectNode node = memberEnvelope(s);
        node.put("member_epoch", body.path("member_epoch").asLong());
        return ok(node);
    }

    private Reply memberTransfer(JsonNode body) {
        Sub s = subscriptions.get(body.path("subscription").asText());
        if (s == null) {
            return error(404, null, "unknown subscription");
        }
        String workId = body.path("work_id").asText();
        Lease held = s.leases.get(workId);
        if (held == null || !held.memberId.equals(body.path("member_id").asText())
                || held.leaseEpoch != body.path("lease_epoch").asLong()) {
            return error(409, "lease.stale_lease", "lease " + workId + " no longer held");
        }
        String toId = body.path("to_member_id").asText();
        long toEpoch = body.path("to_member_epoch").asLong();
        if (!Objects.equals(s.memberEpochs.get(toId), toEpoch)) {
            return error(409, "lease.stale_lease", "target member epoch " + toEpoch + " superseded");
        }
        held.memberId = toId;
        held.memberEpoch = toEpoch;
        held.leaseEpoch = ++s.leaseSeq;
        held.expiresAtTick = s.tick + body.path("lease_ticks").asLong(30);
        ObjectNode node = memberEnvelope(s);
        node.put("member_epoch", toEpoch);
        node.set("lease", leaseJson(workId, held));
        return ok(node);
    }

    private Reply memberFetch(JsonNode body) {
        Sub s = subscriptions.get(body.path("subscription").asText());
        Reply fenced = leaseFence(s, body);
        if (fenced != null) {
            return fenced;
        }
        List<Rec> recs = feeds.getOrDefault(s.feed, List.of());
        int limit = body.path("limit").isNumber()
                ? Math.min(body.path("limit").asInt(), 256) : 256;
        ArrayNode records = obj().putArray("records");
        String cursor = null;
        for (int i = s.frontier; i < Math.min(s.frontier + limit, recs.size()); i++) {
            records.add(record(recs.get(i)));
            cursor = recs.get(i).cursor;
        }
        ObjectNode node = obj();
        node.set("records", records);
        if (cursor != null) {
            node.put("cursor", cursor);
        } else {
            node.putNull("cursor");
        }
        node.putObject("positions");
        node.put("ownership_epoch", s.ownershipEpoch);
        node.put("delivery_attempt", body.path("lease_epoch").asLong());
        return ok(node);
    }

    private Reply memberAck(JsonNode body) {
        Sub s = subscriptions.get(body.path("subscription").asText());
        Reply fenced = leaseFence(s, body);
        if (fenced != null) {
            return fenced;
        }
        Rec rec = byCursor.get(body.path("cursor").asText(null));
        if (rec != null && s.feed.equals(rec.feed)) {
            s.frontier = Math.max(s.frontier, rec.index + 1);
        }
        ObjectNode node = memberEnvelope(s);
        node.put("member_epoch", body.path("member_epoch").asLong());
        return ok(node);
    }

    private JsonNode memberState(Map<String, String> query) {
        Sub s = subscriptions.computeIfAbsent(query.get("subscription"), n -> new Sub());
        ObjectNode node = obj()
                .put("subscription", query.getOrDefault("subscription", ""))
                .put("ownership_epoch", s.ownershipEpoch)
                .put("tick", s.tick);
        ObjectNode epochs = node.putObject("member_epochs");
        s.memberEpochs.forEach(epochs::put);
        ArrayNode leases = node.putArray("leases");
        s.leases.forEach((workId, lease) -> leases.add(leaseJson(workId, lease)));
        return node;
    }

    private JsonNode subscriptionSeek(JsonNode body) {
        Sub s = subscriptions.get(body.path("subscription").asText());
        if (s != null) {
            s.frontier = startIndex(seekStart(body), s.feed);
        }
        ObjectNode node = obj()
                .put("request_id", requestId(body))
                .put("ownership_epoch", s == null ? 1 : s.ownershipEpoch);
        node.put("cursor", s == null ? null : cursorAt(s.feed, s.frontier - 1));
        node.putObject("positions");
        return node;
    }

    private JsonNode seekStart(JsonNode body) {
        ObjectNode start = obj();
        String position = body.path("position").asText("beginning");
        start.put("kind", position);
        if ("cursor".equals(position) && body.hasNonNull("cursor")) {
            start.put("cursor", body.path("cursor").asText());
        }
        if ("timestamp".equals(position) && body.hasNonNull("timestamp_ns")) {
            start.put("cursor", Long.parseLong(body.path("timestamp_ns").asText()));
        }
        return start;
    }

    private Reply memberFence(Sub s, JsonNode body) {
        if (s == null) {
            return error(404, null, "unknown subscription");
        }
        long epoch = body.path("member_epoch").asLong();
        Long current = s.memberEpochs.get(body.path("member_id").asText());
        if (current == null || epoch != current) {
            return error(409, "stale_epoch",
                    "member epoch " + epoch + " superseded");
        }
        return null;
    }

    private Reply leaseFence(Sub s, JsonNode body) {
        Reply fenced = memberFence(s, body);
        if (fenced != null) {
            return fenced;
        }
        Lease held = s.leases.get(body.path("work_id").asText());
        if (held == null || !held.memberId.equals(body.path("member_id").asText())
                || held.leaseEpoch != body.path("lease_epoch").asLong()) {
            return error(409, "lease.stale_lease",
                    "lease " + body.path("work_id").asText() + " no longer held");
        }
        return null;
    }

    private static ObjectNode memberEnvelope(Sub s) {
        return obj()
                .put("ownership_epoch", s.ownershipEpoch)
                .put("tick", s.tick);
    }

    private static ObjectNode leaseJson(String workId, Lease lease) {
        return obj()
                .put("work_id", workId)
                .put("member_id", lease.memberId)
                .put("member_epoch", lease.memberEpoch)
                .put("lease_epoch", lease.leaseEpoch)
                .put("expires_at_tick", lease.expiresAtTick);
    }
}
