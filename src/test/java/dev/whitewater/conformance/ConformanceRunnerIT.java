package dev.whitewater.conformance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import dev.whitewater.WhitewaterClient;
import dev.whitewater.WhitewaterException;
import dev.whitewater.internal.Transport;
import dev.whitewater.internal.Wire;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

/**
 * Executes the shared whitewater-protocol conformance corpus
 * ({@code spec/conformance/*.json}) against a real Riverbed. Each scenario
 * runs as a JUnit {@link DynamicTest}; a scenario the SDK cannot express
 * reports {@code unsupported} explicitly rather than skipping.
 *
 * Enable with:
 *   WHITEWATER_ENDPOINTS=http://localhost:7071,http://localhost:7072,http://localhost:7073
 *   WHITEWATER_API_KEY=whitewater-local-development-admin-key
 *   WHITEWATER_CHAOS_CMD=python scripts/chaos_driver.py   (optional fault hook)
 *   mvn verify
 */
class ConformanceRunnerIT {

    private static WhitewaterClient client;
    private static Path conformanceDir;
    private static String endpointsEnv;
    private static String defaultApiKey;
    private static String chaosCommand;
    private WhitewaterClient activeClient;
    private Transport activeTransport;

    @BeforeAll
    static void setUp() {
        String endpoints = System.getenv("WHITEWATER_ENDPOINTS");
        String apiKey = System.getenv().getOrDefault(
                "WHITEWATER_API_KEY", "whitewater-local-development-admin-key");
        endpointsEnv = endpoints;
        defaultApiKey = apiKey;
        chaosCommand = System.getenv("WHITEWATER_CHAOS_CMD");
        conformanceDir = Paths.get(System.getenv().getOrDefault(
                "WHITEWATER_SPEC_DIR", "spec/conformance"));
        if (endpoints == null || !Files.isDirectory(conformanceDir)) {
            org.junit.jupiter.api.Assumptions.assumeTrue(false,
                    "WHITEWATER_ENDPOINTS and spec/conformance required for conformance");
        }
        client = WhitewaterClient.builder()
                .apiKey(apiKey)
                .endpoints(Arrays.stream(endpoints.split(",")).map(URI::create).toList())
                .build();
        assertTrue(client.healthy(), "Riverbed must be healthy for conformance");
    }

    @TestFactory
    Stream<DynamicTest> scenarios() throws IOException {
        if (client == null) {
            return Stream.empty();
        }
        try (Stream<Path> files = Files.list(conformanceDir)) {
            return files.filter(path -> path.toString().endsWith(".json"))
                    .sorted()
                    .map(path -> dynamicTest(path.getFileName().toString(), () -> runScenario(path)))
                    .toList()
                    .stream();
        }
    }

    private final Map<String, JsonNode> bound = new HashMap<>();
    private final Map<String, String> namedUuids = new HashMap<>();
    private String uniq;
    private JsonNode lastResult;
    private int lastStatus;
    private WhitewaterException lastError;

    private void runScenario(Path fixture) throws Exception {
        bound.clear();
        namedUuids.clear();
        uniq = "u" + Long.toString(System.currentTimeMillis(), 36)
                + UUID.randomUUID().toString().replace("-", "").substring(0, 6);
        lastResult = null;
        lastError = null;
        lastStatus = 0;
        JsonNode scenario = Transport.json().readTree(Files.readAllBytes(fixture));
        String scenarioKey = scenario.hasNonNull("api_key")
                ? scenario.get("api_key").asText() : defaultApiKey;
        activeClient = buildClient(scenarioKey);
        activeTransport = new Transport(endpointUris(), scenarioKey, Duration.ofSeconds(30));
        JsonNode steps = scenario.path("steps");
        for (JsonNode step : steps) {
            String op = step.path("op").asText();
            JsonNode args = substitute(step.path("args"));
            lastError = null;
            lastResult = null;
            lastStatus = 0;
            int times = 1;
            long intervalMs = 0;
            if (step.hasNonNull("poll")) {
                JsonNode poll = step.get("poll");
                times = Math.min(poll.path("times").asInt(1), 30);
                intervalMs = Math.min(poll.path("interval_ms").asLong(0), 10_000);
            }
            JsonNode expect = step.path("expect");
            for (int attempt = 0; attempt < times; attempt++) {
                try {
                    lastResult = execute(op, args);
                    lastStatus = 200;
                    lastError = null;
                } catch (WhitewaterException e) {
                    lastError = e;
                    lastStatus = e.status();
                    lastResult = null;
                }
                if (expect.isObject()) {
                    try {
                        evaluateExpectations(expect, lastResult);
                        break;  // satisfied — stop polling
                    } catch (AssertionError pending) {
                        if (attempt + 1 >= times) {
                            throw pending;
                        }
                    }
                }
                if (attempt + 1 < times && intervalMs > 0) {
                    Thread.sleep(intervalMs);
                }
            }
            if (lastError != null && !step.hasNonNull("expect")) {
                fail("step " + op + " failed: " + lastError.getMessage());
            }
            if (step.hasNonNull("bind")) {
                bound.put(step.get("bind").asText(), Objects.requireNonNullElse(
                        lastResult, Transport.json().nullNode()));
            }
        }
    }

    private JsonNode execute(String op, JsonNode args) throws Exception {
        return switch (op) {
            case "wcl" -> executeWcl(args);
            case "admin.commands" -> executeCommands(args);
            case "writer.append" -> executeWriterAppend(args);
            case "writer.append_batch" -> executeWriterAppendBatch(args);
            case "reader.open" -> executeReaderOpen(args);
            case "reader.fetch" -> executeReaderFetch(args);
            case "reader.ack" -> executeReaderAck(args);
            case "reader.close" -> executeReaderClose(args);
            case "reader.temporary_fetch" -> executeTemporaryFetch(args);
            case "member.join" -> executeMemberJoin(args);
            case "member.claim" -> executeMemberLease("/v1/subscriptions/members/claim", args, false);
            case "member.renew" -> executeMemberLease("/v1/subscriptions/members/renew", args, true);
            case "member.release" -> executeMemberLease("/v1/subscriptions/members/release", args, true);
            case "member.transfer" -> executeMemberTransfer(args);
            case "member.fetch" -> executeMemberFetch(args);
            case "member.ack" -> executeMemberAck(args);
            case "member.state" -> executeMemberState(args);
            case "subscription.seek" -> executeSubscriptionSeek(args);
            case "feed.append" -> executeFeedAppend(args);
            case "feed.records" -> executeFeedRecords(args);
            case "statestore.mutate" -> executeStoreMutate(args);
            case "statestore.lookup" -> executeStoreLookup(args);
            case "statestore.get_by" -> executeStoreGetBy(args);
            case "statestore.iterate" -> executeStoreIterate(args);
            case "statestore.clear" -> executeStoreClear(args);
            case "statestore.sync" -> executeStoreSync(args);
            case "statestore.rebuild" -> executeStoreRebuild(args);
            case "index.query" -> executeIndexQuery(args);
            case "node.metrics" -> activeTransport.get("/v1/node/metrics");
            case "cluster.members" -> activeTransport.get("/v1/cluster/members");
            case "admin.demand" -> executeDemand(args);
            case "subscription.progress" -> executeSubscriptionProgress(args);
            case "health" -> activeTransport.get("/health");
            default -> {
                if (op.startsWith("chaos.")) {
                    yield executeChaos(op, args);
                }
                throw new UnsupportedOperationException(
                        "unsupported operation in scenario: " + op);
            }
        };
    }

    private JsonNode executeWcl(JsonNode args) {
        String requestId = args.path("request_id").isTextual()
                ? args.get("request_id").asText() : UUID.randomUUID().toString();
        return toJson(activeClient.wcl(args.get("script").asText(), UUID.fromString(requestId)));
    }

    private JsonNode executeCommands(JsonNode args) {
        var commands = new ArrayList<Map<String, Object>>();
        for (JsonNode command : args.get("commands")) {
            commands.add(Transport.json().convertValue(command, Map.class));
        }
        return toJson(activeClient.commands(commands));
    }

    private JsonNode executeWriterAppend(JsonNode args) {
        var writer = activeClient.writerSession(
                args.get("writer").asText(), args.path("session_epoch").asLong())
                .useCompression(args.path("use_compression").asBoolean());
        var result = writer.append(new dev.whitewater.WriterSessionClient.Append(
                UUID.fromString(args.get("request_id").asText()),
                decodeBytes(args.get("key")),
                decodeBytes(args.get("payload")),
                args.hasNonNull("event_time_ns") ? args.get("event_time_ns").asLong() : null,
                decodeMetadata(args.path("metadata"))));
        var node = (ObjectNode) toJson(result);
        node.put("request_id", args.get("request_id").asText());
        return node;
    }

    private JsonNode executeWriterAppendBatch(JsonNode args) {
        JsonNode records = args.get("records");
        var appends = new ArrayList<dev.whitewater.WriterSessionClient.Append>();
        for (JsonNode record : records) {
            appends.add(new dev.whitewater.WriterSessionClient.Append(
                    UUID.fromString(record.get("request_id").asText()),
                    decodeBytes(record.get("key")),
                    decodeBytes(record.get("payload")),
                    record.hasNonNull("event_time_ns") ? record.get("event_time_ns").asLong() : null,
                    decodeMetadata(record.path("metadata"))));
        }
        var writer = activeClient.writerSession(
                records.get(0).get("writer").asText(),
                records.get(0).path("session_epoch").asLong())
                .useCompression(args.path("use_compression").asBoolean());
        var batch = writer.appendBatch(appends);
        var node = Transport.json().createObjectNode();
        node.set("results", toJson(batch.results()));
        node.set("feedback", toJson(batch.feedback()));
        return node;
    }

    private JsonNode executeReaderOpen(JsonNode args) {
        return toJson(activeClient.readerSession(args.get("reader").asText())
                .open(args.path("capacity").asInt(64),
                        UUID.fromString(args.get("request_id").asText())));
    }

    private JsonNode executeReaderFetch(JsonNode args) {
        var session = new dev.whitewater.model.ReaderSession(
                null, null, args.path("session_epoch").asLong(), 0, null, null);
        return toJson(activeClient.readerSession(args.get("reader").asText())
                .fetch(session, args.hasNonNull("limit") ? args.get("limit").asInt() : null,
                        UUID.fromString(args.get("request_id").asText())));
    }

    private JsonNode executeReaderAck(JsonNode args) {
        var session = new dev.whitewater.model.ReaderSession(
                null, null, args.path("session_epoch").asLong(), 0, null, null);
        return toJson(activeClient.readerSession(args.get("reader").asText())
                .ack(session, new dev.whitewater.model.Cursor(args.get("cursor").asText()),
                        UUID.fromString(args.get("request_id").asText())));
    }

    private JsonNode executeReaderClose(JsonNode args) {
        var session = new dev.whitewater.model.ReaderSession(
                null, null, args.path("session_epoch").asLong(), 0, null, null);
        return toJson(activeClient.readerSession(args.get("reader").asText()).close(session));
    }

    private JsonNode executeMemberJoin(JsonNode args) {
        return toJson(activeClient.subscriptionMember(args.get("subscription").asText())
                .join(UUID.fromString(args.get("member_id").asText())));
    }

    private dev.whitewater.SubscriptionMemberClient.Joined joinedFrom(JsonNode args) {
        return new dev.whitewater.SubscriptionMemberClient.Joined(
                UUID.fromString(args.get("member_id").asText()), memberEpoch(args), 0, 0);
    }

    private dev.whitewater.SubscriptionMemberClient.WorkLease leaseFrom(JsonNode args) {
        return new dev.whitewater.SubscriptionMemberClient.WorkLease(
                UUID.fromString(args.get("work_id").asText()),
                UUID.fromString(args.get("member_id").asText()),
                memberEpoch(args),
                args.get("lease_epoch").asLong(), 0);
    }

    private long memberEpoch(JsonNode args) {
        return args.hasNonNull("member_epoch") ? args.get("member_epoch").asLong() : 0L;
    }

    private JsonNode executeMemberLease(String path, JsonNode args, boolean needsLeaseEpoch) {
        var member = activeClient.subscriptionMember(args.get("subscription").asText());
        if ("release".equals(path.substring(path.lastIndexOf('/') + 1))) {
            member.release(joinedFrom(args), leaseFrom(args));
            return Transport.json().createObjectNode();
        }
        var lease = needsLeaseEpoch
                ? member.renew(joinedFrom(args), leaseFrom(args),
                        args.hasNonNull("lease_ticks") ? args.get("lease_ticks").asInt() : null)
                : member.claim(joinedFrom(args), UUID.fromString(args.get("work_id").asText()),
                        args.hasNonNull("lease_ticks") ? args.get("lease_ticks").asInt() : null);
        var node = Transport.json().createObjectNode();
        node.set("lease", toJson(lease));
        return node;
    }

    private JsonNode executeMemberTransfer(JsonNode args) {
        var member = activeClient.subscriptionMember(args.get("subscription").asText());
        var lease = member.transfer(joinedFrom(args), leaseFrom(args),
                UUID.fromString(args.get("to_member_id").asText()),
                args.get("to_member_epoch").asLong(),
                args.hasNonNull("lease_ticks") ? args.get("lease_ticks").asInt() : null);
        var node = Transport.json().createObjectNode();
        node.set("lease", toJson(lease));
        return node;
    }

    private JsonNode executeMemberFetch(JsonNode args) {
        var page = activeClient.subscriptionMember(args.get("subscription").asText())
                .fetch(joinedFrom(args), leaseFrom(args),
                        args.hasNonNull("limit") ? args.get("limit").asInt() : null);
        return toJson(page);
    }

    private JsonNode executeMemberAck(JsonNode args) {
        var page = new dev.whitewater.SubscriptionMemberClient.MemberPage(
                List.of(),
                args.hasNonNull("cursor")
                        ? new dev.whitewater.model.Cursor(args.get("cursor").asText()) : null,
                readPositions(args.path("positions")), 0, 0, null, null);
        activeClient.subscriptionMember(args.get("subscription").asText())
                .ack(joinedFrom(args), leaseFrom(args), page);
        return Transport.json().createObjectNode();
    }

    private JsonNode executeMemberState(JsonNode args) {
        return toJson(activeClient.subscriptionMemberState(args.get("subscription").asText()));
    }

    private JsonNode executeSubscriptionSeek(JsonNode args) {
        return toJson(activeClient.seekSubscription(
                args.get("subscription").asText(),
                args.get("position").asText(),
                args.hasNonNull("timestamp_ns") ? args.get("timestamp_ns").asLong() : null,
                args.hasNonNull("cursor")
                        ? new dev.whitewater.model.Cursor(args.get("cursor").asText()) : null,
                UUID.fromString(args.get("request_id").asText())));
    }

    private JsonNode executeFeedAppend(JsonNode args) {
        var record = new dev.whitewater.model.FeedAppend(
                UUID.fromString(args.get("request_id").asText()),
                UUID.fromString(args.get("writer_session_id").asText()),
                args.get("writer_epoch").asLong(),
                args.get("sequence").asLong(),
                decodeBytes(args.get("key")),
                decodeBytes(args.get("payload")),
                args.hasNonNull("event_time_ns") ? args.get("event_time_ns").asLong() : null,
                decodeMetadata(args.path("metadata")));
        return toJson(activeClient.feeds().useCompression(args.path("use_compression").asBoolean())
                .append(args.get("feed").asText(), record));
    }

    private JsonNode executeFeedRecords(JsonNode args) {
        StringBuilder path = new StringBuilder("/v1/feeds/records?feed=")
                .append(java.net.URLEncoder.encode(args.get("feed").asText(), StandardCharsets.UTF_8));
        if (args.hasNonNull("limit")) {
            path.append("&limit=").append(args.get("limit").asInt());
        }
        if (args.hasNonNull("after")) {
            path.append("&after=").append(java.net.URLEncoder.encode(
                    args.get("after").asText(), StandardCharsets.UTF_8));
        }
        var node = Transport.json().createObjectNode();
        node.set("records", activeTransport.get(path.toString()));
        return node;
    }

    private JsonNode executeTemporaryFetch(JsonNode args) {
        var options = new dev.whitewater.model.TemporaryFetchOptions(
                args.hasNonNull("after")
                        ? new dev.whitewater.model.Cursor(args.get("after").asText()) : null,
                args.hasNonNull("limit") ? args.get("limit").asInt() : null,
                args.path("new_only").asBoolean(false) ? Boolean.TRUE : null,
                args.path("tail").asBoolean(false) ? Boolean.TRUE : null,
                args.hasNonNull("wait_ms") ? args.get("wait_ms").asLong() : null,
                args.hasNonNull("after_event_time_ns")
                        ? args.get("after_event_time_ns").asLong() : null);
        return toJson(activeClient.feeds().temporaryFetch(args.get("feed").asText(), options));
    }

    private JsonNode executeStoreMutate(JsonNode args) {
        ObjectNode body = Transport.json().createObjectNode();
        body.put("store", args.get("store").asText());
        body.put("mutation_id", args.hasNonNull("mutation_id")
                ? args.get("mutation_id").asText() : UUID.randomUUID().toString());
        ObjectNode op = body.putObject("op");
        op.put("kind", args.path("kind").asText("put_row"));
        if (args.hasNonNull("key")) {
            op.put("key_base64", Wire.b64(decodeBytes(args.get("key"))));
        }
        if (args.hasNonNull("payload")) {
            op.put("payload_base64", Wire.b64(decodeBytes(args.get("payload"))));
        }
        // `named` arrives already wire-shaped — [{name, values:[IndexValueWire]}]
        // — and passes through verbatim; the corpus asserts exact semantics.
        if (args.hasNonNull("named")) {
            op.set("named", args.get("named"));
        }
        return activeTransport.post("/v1/state-stores/mutate", body);
    }

    private JsonNode executeStoreLookup(JsonNode args) {
        ObjectNode body = Transport.json().createObjectNode();
        body.put("store", args.get("store").asText());
        body.put("key_base64", Wire.b64(decodeBytes(args.get("key"))));
        body.put("strict", args.path("strict").asBoolean(false));
        if (args.hasNonNull("wait_ms")) {
            body.put("wait_ms", args.get("wait_ms").asLong());
        }
        return activeTransport.post("/v1/state-stores/lookup", body);
    }

    private JsonNode executeStoreGetBy(JsonNode args) {
        ObjectNode body = Transport.json().createObjectNode();
        body.put("store", args.get("store").asText());
        body.put("named_key", args.get("named_key").asText());
        var values = body.putArray("values");
        if (args.hasNonNull("values")) {
            args.get("values").forEach(v -> values.add(v));
        }
        putBound(body, "lower", args.path("lower"));
        putBound(body, "upper", args.path("upper"));
        if (args.hasNonNull("limit")) {
            body.put("limit", args.get("limit").asInt());
        }
        body.put("strict", args.path("strict").asBoolean(false));
        if (args.hasNonNull("wait_ms")) {
            body.put("wait_ms", args.get("wait_ms").asLong());
        }
        return activeTransport.post("/v1/state-stores/get-by", body);
    }

    private JsonNode executeStoreIterate(JsonNode args) {
        ObjectNode body = Transport.json().createObjectNode();
        body.put("store", args.get("store").asText());
        if (args.hasNonNull("after_key")) {
            body.put("after_key_base64", Wire.b64(decodeBytes(args.get("after_key"))));
        }
        if (args.hasNonNull("limit")) {
            body.put("limit", args.get("limit").asInt());
        }
        return activeTransport.post("/v1/state-stores/iterate", body);
    }

    private JsonNode executeStoreClear(JsonNode args) {
        ObjectNode body = Transport.json().createObjectNode();
        body.put("store", args.get("store").asText());
        body.put("mutation_id", args.hasNonNull("mutation_id")
                ? args.get("mutation_id").asText() : UUID.randomUUID().toString());
        return activeTransport.post("/v1/state-stores/clear", body);
    }

    private JsonNode executeStoreSync(JsonNode args) {
        ObjectNode body = Transport.json().createObjectNode();
        body.put("store", args.get("store").asText());
        return activeTransport.post("/v1/state-stores/sync", body);
    }

    private JsonNode executeStoreRebuild(JsonNode args) {
        ObjectNode body = Transport.json().createObjectNode();
        body.put("store", args.get("store").asText());
        if (args.hasNonNull("token")) {
            body.put("token", args.get("token").asText());
        }
        return activeTransport.post("/v1/state-stores/rebuild", body);
    }

    private JsonNode executeIndexQuery(JsonNode args) {
        return activeTransport.post("/v1/indexes/query", args);
    }

    private JsonNode executeDemand(JsonNode args) {
        return activeTransport.get("/v1/admin/demand/" + args.get("surface").asText());
    }

    private JsonNode executeSubscriptionProgress(JsonNode args) {
        return activeTransport.get("/v1/admin/subscriptions/progress?name="
                + java.net.URLEncoder.encode(args.get("subscription").asText(), StandardCharsets.UTF_8));
    }

    private JsonNode executeChaos(String op, JsonNode args) throws IOException, InterruptedException {
        if (chaosCommand == null || chaosCommand.isBlank()) {
            throw new UnsupportedOperationException(
                    "unsupported operation without WHITEWATER_CHAOS_CMD: " + op);
        }
        var command = new ArrayList<String>();
        command.addAll(List.of(chaosCommand.split("\\s+")));
        command.add(op);
        args.fieldNames().forEachRemaining(name -> command.add(args.get(name).asText()));
        var process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        int code = process.waitFor();
        if (code != 0) {
            fail("chaos hook " + op + " exited " + code + ": " + output);
        }
        return Transport.json().createObjectNode();
    }

    // ---- helpers -------------------------------------------------------------

    private static WhitewaterClient buildClient(String apiKey) {
        return WhitewaterClient.builder()
                .apiKey(apiKey)
                .endpoints(endpointUris())
                .build();
    }

    private static List<URI> endpointUris() {
        return Arrays.stream(endpointsEnv.split(",")).map(URI::create).toList();
    }

    private void putBound(ObjectNode body, String field, JsonNode bound) {
        if (bound == null || !bound.isObject()) {
            return;
        }
        ObjectNode node = body.putObject(field);
        if (bound.hasNonNull("value")) {
            node.set("value", bound.get("value"));
        }
        if (bound.hasNonNull("inclusive")) {
            node.put("inclusive", bound.get("inclusive").asBoolean());
        }
    }

    private Object decodeIndexValue(JsonNode node) {
        if (node == null || !node.isObject() || !node.hasNonNull("kind")) {
            return node == null ? null : node.asText();
        }
        return switch (node.get("kind").asText()) {
            case "null" -> null;
            case "bool" -> node.path("value").asBoolean();
            case "i64" -> node.path("value").asLong();
            case "text" -> node.path("value").asText();
            case "bytes" -> Wire.b64(node.path("value_base64").asText());
            default -> throw new IllegalArgumentException("unknown value kind " + node);
        };
    }

    private Map<String, String> readPositions(JsonNode node) {
        if (node == null || !node.isObject()) {
            return Map.of();
        }
        Map<String, String> positions = new LinkedHashMap<>();
        node.fields().forEachRemaining(e -> positions.put(e.getKey(), e.getValue().asText()));
        return positions;
    }

    private byte[] decodeBytes(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.hasNonNull("utf8")) {
            return node.get("utf8").asText().getBytes(StandardCharsets.UTF_8);
        }
        if (node.hasNonNull("hex")) {
            String hex = node.get("hex").asText();
            byte[] out = new byte[hex.length() / 2];
            for (int i = 0; i < out.length; i++) {
                out[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
            }
            return out;
        }
        if (node.hasNonNull("base64")) {
            return Base64.getDecoder().decode(node.get("base64").asText());
        }
        return node.asText().getBytes(StandardCharsets.UTF_8);
    }

    private Map<String, byte[]> decodeMetadata(JsonNode node) {
        if (node == null || !node.isObject()) {
            return Map.of();
        }
        Map<String, byte[]> metadata = new LinkedHashMap<>();
        node.fields().forEachRemaining(e -> metadata.put(e.getKey(), decodeBytes(e.getValue())));
        return metadata;
    }

    // ---- substitution ----

    private JsonNode substitute(JsonNode node) {
        if (node == null) {
            return null;
        }
        if (node.isTextual()) {
            String text = node.asText();
            if (text.startsWith("{{") && text.endsWith("}}") && text.indexOf("}}") == text.length() - 2) {
                return resolveNode(text.substring(2, text.length() - 2));
            }
            return Transport.json().getNodeFactory().textNode(substituteString(text));
        }
        if (node.isObject()) {
            var copy = (ObjectNode) node.deepCopy();
            var names = new ArrayList<String>();
            copy.fieldNames().forEachRemaining(names::add);
            for (String name : names) {
                copy.set(name, substitute(copy.get(name)));
            }
            return copy;
        }
        if (node.isArray()) {
            var copy = Transport.json().createArrayNode();
            node.forEach(item -> copy.add(substitute(item)));
            return copy;
        }
        return node;
    }

    private String substituteString(String value) {
        StringBuilder out = new StringBuilder();
        int cursor = 0;
        while (true) {
            int open = value.indexOf("{{", cursor);
            if (open < 0) {
                out.append(value, cursor, value.length());
                return out.toString();
            }
            int close = value.indexOf("}}", open);
            out.append(value, cursor, open);
            String token = value.substring(open + 2, close);
            out.append(resolveToken(token));
            cursor = close + 2;
        }
    }

    private JsonNode resolveNode(String token) {
        if (token.equals("uniq")) {
            return Transport.json().getNodeFactory().textNode(uniq);
        }
        if (token.equals("uuid")) {
            return Transport.json().getNodeFactory().textNode(UUID.randomUUID().toString());
        }
        if (token.startsWith("uuid:")) {
            return Transport.json().getNodeFactory().textNode(
                    namedUuids.computeIfAbsent(token.substring(5), k -> UUID.randomUUID().toString()));
        }
        if (token.equals("now_ns") || token.startsWith("now_ns:")) {
            long offset = token.startsWith("now_ns:")
                    ? Long.parseLong(token.substring(7)) : 0L;
            long nowNs = java.time.Instant.now().getEpochSecond() * 1_000_000_000L
                    + java.time.Instant.now().getNano() + offset;
            return Transport.json().getNodeFactory().textNode(Long.toString(nowNs));
        }
        int dot = token.indexOf('.');
        if (dot > 0) {
            JsonNode bound0 = bound.get(token.substring(0, dot));
            if (bound0 != null) {
                JsonNode value = pathGet(bound0, token.substring(dot + 1));
                if (value != null && !value.isNull() && !value.isMissingNode()) {
                    return value;
                }
            }
        }
        throw new IllegalStateException("unresolved fixture token {{" + token + "}}");
    }

    private String resolveToken(String token) {
        JsonNode value = resolveNode(token);
        return value.isTextual() ? value.asText() : value.toString();
    }

    /**
     * Resolve a fixture path against a bound result. Segments split on
     * {@code .} except inside a quoted {@code ["…"]} bracket — that form
     * names a field containing dots, e.g.
     * {@code records[0].metadata_base64["enrich.matched"]}.
     */
    private JsonNode pathGet(JsonNode node, String path) {
        JsonNode current = node;
        for (String segment : pathSegments(path)) {
            if (current == null) {
                return null;
            }
            int bracket = segment.indexOf('[');
            if (bracket >= 0) {
                String field = segment.substring(0, bracket);
                if (!field.isEmpty()) {
                    current = current.path(field);
                }
                int close = segment.indexOf(']', bracket);
                String inner = segment.substring(bracket + 1, close < 0 ? segment.length() : close);
                if (inner.startsWith("\"") && inner.endsWith("\"") && inner.length() >= 2) {
                    current = current.path(inner.substring(1, inner.length() - 1));
                } else {
                    int index = Integer.parseInt(inner);
                    if (current.isArray() && index < current.size()) {
                        current = current.get(index);
                    } else {
                        return null;
                    }
                }
            } else {
                current = current.path(segment);
            }
        }
        return current;
    }

    /** Split a fixture path on {@code .} outside quoted {@code ["…"]} brackets. */
    private static List<String> pathSegments(String path) {
        var segments = new ArrayList<String>();
        var current = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (c == '"' && i > 0 && path.charAt(i - 1) == '[') {
                quoted = true;
            } else if (c == '"' && quoted) {
                quoted = false;
            }
            if (c == '.' && !quoted) {
                segments.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        segments.add(current.toString());
        return segments;
    }

    // ---- expectations ----

    private void evaluateExpectations(JsonNode expect, JsonNode result) {
        // `error_status` asserts the step's wire status — including 200 for
        // a successful request; `error_code` asserts a wire refusal code
        // (the step must have failed).
        if (expect.hasNonNull("error_status")) {
            assertEquals(expect.get("error_status").asInt(), lastStatus,
                    "expected status " + expect.get("error_status")
                            + (lastError != null ? " but step failed: " + lastError.getMessage() : ""));
        }
        if (expect.hasNonNull("error_code")) {
            assertNotNull(lastError, "expected error_code " + expect.get("error_code")
                    + " but the step succeeded");
            assertEquals(expect.get("error_code").asText(), lastError.code());
        }
        if (expect.hasNonNull("error_code") || expect.hasNonNull("error_status")) {
            if (lastError != null) {
                return;     // asserted the failure; no field expectations apply
            }
        }
        if (lastError != null) {
            fail("step failed: " + lastError.getMessage());
        }
        var names = new ArrayList<String>();
        expect.fieldNames().forEachRemaining(names::add);
        names.remove("error_status");
        names.remove("error_code");
        for (String path : names) {
            JsonNode actual = pathGet(result, path);
            JsonNode expected = expect.get(path);
            checkExpectation(path, actual, expected);
        }
    }

    private void checkExpectation(String path, JsonNode actual, JsonNode expected) {
        String text = expected.isTextual() ? expected.asText() : null;
        if (text != null && text.equals("present")) {
            assertNotNull(actual, path + " expected present");
            return;
        }
        if (text != null && text.equals("absent")) {
            assertTrue(actual == null || actual.isNull(), path + " expected absent but was " + actual);
            return;
        }
        if (text != null && text.equals("!=null")) {
            assertTrue(actual != null && !actual.isNull(), path + " expected non-null");
            return;
        }
        if (text != null && text.startsWith("len>=")) {
            int min = Integer.parseInt(text.substring(5));
            int length = actual != null && (actual.isArray() || actual.isTextual())
                    ? actual.size() : -1;
            assertTrue(length >= min, path + " expected len>= " + min + " but was " + actual);
            return;
        }
        if (text != null && text.startsWith("==")) {
            String reference = text.substring(2);
            int dot = reference.indexOf('.');
            JsonNode boundNode = bound.get(reference.substring(0, dot));
            JsonNode expectedValue = pathGet(boundNode, reference.substring(dot + 1));
            assertEquals(expectedValue, actual, path + " expected " + expectedValue);
            return;
        }
        if (expected.isObject() && expected.hasNonNull("base64_json")) {
            assertNotNull(actual, path + " expected base64 JSON but got null");
            JsonNode decoded;
            try {
                decoded = Transport.json().readTree(
                        Base64.getDecoder().decode(actual.asText()));
            } catch (IOException | IllegalArgumentException e) {
                fail(path + " expected base64-encoded JSON; decode failed: " + e.getMessage());
                return;
            }
            expected.get("base64_json").fields().forEachRemaining(entry ->
                    checkExpectation(path + "{" + entry.getKey() + "}",
                            pathGet(decoded, entry.getKey()), entry.getValue()));
            return;
        }
        JsonNode expectedValue = expected;
        if (text != null && text.contains("{{")) {
            expectedValue = Transport.json().getNodeFactory().textNode(substituteString(text));
        }
        if (expectedValue.isTextual() && actual != null && actual.isNumber()) {
            expectedValue = Transport.json().getNodeFactory()
                    .numberNode(Long.parseLong(expectedValue.asText()));
        } else if (expectedValue.isNumber() && actual != null && actual.isTextual()) {
            expectedValue = Transport.json().getNodeFactory()
                    .textNode(expectedValue.asText());
        }
        final JsonNode expectedFinal = expectedValue;
        assertEquals(expectedFinal, actual, () -> path + " expected " + expectedFinal + " but was " + actual);
    }

    // ---- serialization helpers ----

    private JsonNode toJson(Object value) {
        return Transport.json().valueToTree(value);
    }

    private JsonNode toJson(List<?> values) {
        return Transport.json().valueToTree(values);
    }
}
