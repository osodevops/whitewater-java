package dev.whitewater.conformance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.whitewater.AdminClient;
import dev.whitewater.WhitewaterClient;
import dev.whitewater.WhitewaterException;
import dev.whitewater.internal.Transport;
import dev.whitewater.internal.Wire;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
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
 *   mvn verify
 */
class ConformanceRunnerIT {

    private static WhitewaterClient client;
    private static Path conformanceDir;
    private static String endpointsEnv;
    private static String defaultApiKey;
    private WhitewaterClient activeClient;
    private Transport activeTransport;

    @BeforeAll
    static void setUp() {
        String endpoints = System.getenv("WHITEWATER_ENDPOINTS");
        String apiKey = System.getenv().getOrDefault(
                "WHITEWATER_API_KEY", "whitewater-local-development-admin-key");
        endpointsEnv = endpoints;
        defaultApiKey = apiKey;
        conformanceDir = Paths.get(System.getenv().getOrDefault(
                "WHITEWATER_SPEC_DIR", "spec/conformance"));
        Assumptions:
        {
            if (endpoints == null || !Files.isDirectory(conformanceDir)) {
                org.junit.jupiter.api.Assumptions.assumeTrue(false,
                        "WHITEWATER_ENDPOINTS and spec/conformance required for conformance");
            }
        }
        client = WhitewaterClient.builder()
                .apiKey(apiKey)
                .endpoints(Arrays.stream(endpoints.split(",")).map(URI::create).toList())
                .build();
        assertTrue(client.admin().healthy(), "Riverbed must be healthy for conformance");
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
    private WhitewaterException lastError;

    private void runScenario(Path fixture) throws Exception {
        bound.clear();
        namedUuids.clear();
        uniq = "u" + Long.toString(System.currentTimeMillis(), 36)
                + UUID.randomUUID().toString().replace("-", "").substring(0, 6);
        lastResult = null;
        lastError = null;
        JsonNode scenario = Transport.json().readTree(Files.readAllBytes(fixture));
        String scenarioKey = scenario.hasNonNull("api_key")
                ? scenario.get("api_key").asText() : defaultApiKey;
        activeClient = buildClient(scenarioKey);
        activeTransport = new Transport(endpointUris(), scenarioKey,
                java.time.Duration.ofSeconds(30));
        JsonNode steps = scenario.path("steps");
        for (JsonNode step : steps) {
            String op = step.path("op").asText();
            JsonNode args = substitute(step.path("args"));
            lastError = null;
            try {
                lastResult = execute(op, args, step);
            } catch (WhitewaterException e) {
                lastError = e;
                lastResult = null;
            }
            if (lastError != null && !step.hasNonNull("expect")) {
                // An unhandled step failure is a scenario failure —
                // surface it rather than letting downstream tokens
                // resolve against a null binding and fail confusingly.
                fail("step " + step.path("op").asText() + " failed: " + lastError.getMessage());
            }
            if (step.hasNonNull("bind")) {
                bound.put(step.get("bind").asText(), Objects.requireNonNullElse(
                        lastResult, Transport.json().nullNode()));
            }
            if (step.hasNonNull("expect")) {
                evaluateExpectations(step.get("expect"), lastResult);
            }
        }
    }

    private JsonNode execute(String op, JsonNode args, JsonNode step) throws Exception {
        return switch (op) {
            case "wcl" -> {
                AdminClient admin = activeClient.admin();
                String requestId = args.path("request_id").isTextual()
                        ? args.get("request_id").asText() : UUID.randomUUID().toString();
                var execution = admin.wcl(args.get("script").asText(), UUID.fromString(requestId));
                yield toJson(execution);
            }
            case "writer.append" -> {
                var writer = activeClient.writer(
                        args.get("writer").asText(), args.path("session_epoch").asLong());
                var metadata = decodeMetadata(args.path("metadata"));
                var result = writer.append(new dev.whitewater.WriterSessionClient.Append(
                        UUID.fromString(args.get("request_id").asText()),
                        decodeBytes(args.get("key")),
                        decodeBytes(args.get("payload")),
                        args.hasNonNull("event_time_ns") ? args.get("event_time_ns").asLong() : null,
                        metadata));
                var node = (ObjectNode) toJson(result);
                node.put("request_id", args.get("request_id").asText());
                yield node;
            }
            case "writer.append_batch" -> {
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
                var writer = activeClient.writer(
                        records.get(0).get("writer").asText(),
                        records.get(0).path("session_epoch").asLong());
                var batch = writer.appendBatch(appends);
                var node = Transport.json().createObjectNode();
                node.set("results", toJson(batch.results()));
                node.set("feedback", toJson(batch.feedback()));
                yield node;
            }
            case "reader.open" -> {
                var session = activeClient.reader(args.get("reader").asText())
                        .open(args.path("capacity").asInt(64),
                                UUID.fromString(args.get("request_id").asText()));
                yield toJson(session);
            }
            case "reader.fetch" -> {
                var session = new dev.whitewater.model.ReaderSession(
                        null, null, args.path("session_epoch").asLong(), 0, null, null);
                var page = activeClient.reader(args.get("reader").asText())
                        .fetch(session, args.hasNonNull("limit") ? args.get("limit").asInt() : null,
                                UUID.fromString(args.get("request_id").asText()));
                yield toJson(page);
            }
            case "reader.ack" -> {
                var session = new dev.whitewater.model.ReaderSession(
                        null, null, args.path("session_epoch").asLong(), 0, null, null);
                var result = activeClient.reader(args.get("reader").asText())
                        .ack(session, new dev.whitewater.model.Cursor(args.get("cursor").asText()),
                                UUID.fromString(args.get("request_id").asText()));
                yield toJson(result);
            }
            case "reader.close" -> {
                var session = new dev.whitewater.model.ReaderSession(
                        null, null, args.path("session_epoch").asLong(), 0, null, null);
                var result = activeClient.reader(args.get("reader").asText()).close(session);
                yield toJson(result);
            }
            case "member.join" -> {
                var member = activeClient.subscriptionMember(args.get("subscription").asText())
                        .join(UUID.fromString(args.get("member_id").asText()));
                yield toJson(member);
            }
            case "member.claim" -> {
                var member = joinedFrom(args);
                var lease = activeClient.subscriptionMember(args.get("subscription").asText())
                        .claim(member, UUID.fromString(args.get("work_id").asText()),
                                args.hasNonNull("lease_ticks") ? args.get("lease_ticks").asInt() : null);
                var node = Transport.json().createObjectNode();
                node.set("lease", toJson(lease));
                yield node;
            }
            case "member.renew" -> {
                var member = joinedFrom(args);
                var client0 = activeClient.subscriptionMember(args.get("subscription").asText());
                var lease = client0.renew(member,
                        new dev.whitewater.SubscriptionMemberClient.WorkLease(
                                UUID.fromString(args.get("work_id").asText()),
                                member.memberId(), member.memberEpoch(),
                                args.get("lease_epoch").asLong(), 0),
                        args.hasNonNull("lease_ticks") ? args.get("lease_ticks").asInt() : null);
                var node = Transport.json().createObjectNode();
                node.set("lease", toJson(lease));
                yield node;
            }
            case "member.release" -> {
                var member = joinedFrom(args);
                activeClient.subscriptionMember(args.get("subscription").asText()).release(member,
                        new dev.whitewater.SubscriptionMemberClient.WorkLease(
                                UUID.fromString(args.get("work_id").asText()),
                                member.memberId(), member.memberEpoch(),
                                args.get("lease_epoch").asLong(), 0));
                yield Transport.json().createObjectNode();
            }
            case "member.fetch" -> {
                var member = joinedFrom(args);
                var page = activeClient.subscriptionMember(args.get("subscription").asText()).fetch(member,
                        new dev.whitewater.SubscriptionMemberClient.WorkLease(
                                UUID.fromString(args.get("work_id").asText()),
                                member.memberId(), member.memberEpoch(),
                                args.get("lease_epoch").asLong(), 0),
                        args.hasNonNull("limit") ? args.get("limit").asInt() : null);
                yield toJson(page);
            }
            case "member.ack" -> {
                var member = joinedFrom(args);
                var page = new dev.whitewater.SubscriptionMemberClient.MemberPage(
                        List.of(),
                        args.hasNonNull("cursor") ? new dev.whitewater.model.Cursor(args.get("cursor").asText()) : null,
                        readPositions(args.path("positions")),
                        0);
                activeClient.subscriptionMember(args.get("subscription").asText()).ack(member,
                        new dev.whitewater.SubscriptionMemberClient.WorkLease(
                                UUID.fromString(args.get("work_id").asText()),
                                member.memberId(), member.memberEpoch(),
                                args.get("lease_epoch").asLong(), 0),
                        page);
                yield Transport.json().createObjectNode();
            }
            case "admin.commands" -> {
                var commands = new ArrayList<Map<String, Object>>();
                for (JsonNode command : args.get("commands")) {
                    commands.add(Transport.json().convertValue(command, Map.class));
                }
                yield toJson(activeClient.admin().commands(commands));
            }
            case "feed.append" -> {
                var record = new dev.whitewater.model.FeedAppend(
                        UUID.fromString(args.get("request_id").asText()),
                        UUID.fromString(args.get("writer_session_id").asText()),
                        args.get("writer_epoch").asLong(),
                        args.get("sequence").asLong(),
                        decodeBytes(args.get("key")),
                        decodeBytes(args.get("payload")),
                        args.hasNonNull("event_time_ns") ? args.get("event_time_ns").asLong() : null,
                        decodeMetadata(args.path("metadata")));
                yield toJson(activeClient.feeds().append(args.get("feed").asText(), record));
            }
            case "reader.temporary_fetch" -> {
                var options = new dev.whitewater.model.TemporaryFetchOptions(
                        args.hasNonNull("after")
                                ? new dev.whitewater.model.Cursor(args.get("after").asText())
                                : null,
                        args.hasNonNull("limit") ? args.get("limit").asInt() : null,
                        args.path("new_only").asBoolean(false) ? Boolean.TRUE : null,
                        args.path("tail").asBoolean(false) ? Boolean.TRUE : null,
                        args.hasNonNull("wait_ms") ? args.get("wait_ms").asLong() : null,
                        args.hasNonNull("after_event_time_ns")
                                ? args.get("after_event_time_ns").asLong() : null);
                yield toJson(activeClient.feeds().temporaryFetch(args.get("feed").asText(), options));
            }
            case "feed.records" -> {
                String path = "/v1/feeds/records?feed=" + args.get("feed").asText()
                        + (args.hasNonNull("limit") ? "&limit=" + args.get("limit").asInt() : "")
                        + (args.hasNonNull("after") ? "&after=" + args.get("after").asText() : "");
                var node = Transport.json().createObjectNode();
                // Corpus asserts wire shape (key_base64, decimal-string
                // nanoseconds), so bind the raw response.
                node.set("records", activeTransport.get(path));
                yield node;
            }
            default -> throw new UnsupportedOperationException(
                    "unsupported operation in scenario: " + op);
        };
    }

    private final Transport transport = null; // set in setUp via client internals is not accessible

    private static WhitewaterClient buildClient(String apiKey) {
        return WhitewaterClient.builder()
                .apiKey(apiKey)
                .endpoints(endpointUris())
                .build();
    }

    private static List<URI> endpointUris() {
        return Arrays.stream(endpointsEnv.split(",")).map(URI::create).toList();
    }

    private dev.whitewater.SubscriptionMemberClient.Joined joinedFrom(JsonNode args) {
        return new dev.whitewater.SubscriptionMemberClient.Joined(
                UUID.fromString(args.get("member_id").asText()),
                args.get("member_epoch").asLong(),
                0, 0);
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
            // A whole-node {{binding.path}} reference splices the bound value
            // verbatim, preserving objects/arrays (e.g. positions).
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

    private JsonNode pathGet(JsonNode node, String path) {
        JsonNode current = node;
        for (String segment : path.split("\\.")) {
            if (current == null) {
                return null;
            }
            int bracket = segment.indexOf('[');
            if (bracket >= 0) {
                String field = segment.substring(0, bracket);
                int index = Integer.parseInt(
                        segment.substring(bracket + 1, segment.indexOf(']')));
                current = current.path(field);
                if (current.isArray() && index < current.size()) {
                    current = current.get(index);
                } else {
                    return null;
                }
            } else {
                current = current.path(segment);
            }
        }
        return current;
    }

    // ---- expectations ----

    private void evaluateExpectations(JsonNode expect, JsonNode result) {
        if (expect.hasNonNull("error_code") || expect.hasNonNull("error_status")) {
            assertNotNull(lastError, "expected error " + expect + " but the step succeeded");
            if (expect.hasNonNull("error_code")) {
                assertEquals(expect.get("error_code").asText(), lastError.code());
            }
            if (expect.hasNonNull("error_status")) {
                assertEquals(expect.get("error_status").asInt(), lastError.status());
            }
            return;
        }
        if (lastError != null) {
            fail("step failed: " + lastError.getMessage());
        }
        var names = new ArrayList<String>();
        expect.fieldNames().forEachRemaining(names::add);
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
            int length = actual.isArray() || actual.isTextual() ? actual.size() : -1;
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
        JsonNode expectedValue = expected;
        if (text != null && text.contains("{{")) {
            expectedValue = Transport.json().getNodeFactory().textNode(substituteString(text));
        }
        // The wire serializes i64 nanoseconds as decimal strings, but some
        // admin DTOs emit JSON numbers; compare numerically when one side
        // is a number and the other is numeric text.
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
