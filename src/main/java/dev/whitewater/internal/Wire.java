package dev.whitewater.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.whitewater.model.*;

import java.util.*;

/**
 * Wire <-> model codecs. Nanosecond times travel as decimal strings,
 * binary as base64, cursors as opaque strings.
 */
public final class Wire {

    private Wire() {}

    public static byte[] b64(String value) {
        return value == null ? null : Base64.getDecoder().decode(value);
    }

    public static String b64(byte[] value) {
        return value == null ? null : Base64.getEncoder().encodeToString(value);
    }

    public static Map<String, byte[]> metadata(JsonNode node) {
        if (node == null || !node.isObject()) {
            return Map.of();
        }
        Map<String, byte[]> metadata = new LinkedHashMap<>();
        node.fields().forEachRemaining(entry ->
                metadata.put(entry.getKey(), b64(entry.getValue().asText())));
        return Map.copyOf(metadata);
    }

    public static FeedRecord record(JsonNode node) {
        return new FeedRecord(
                Cursor.of(node.path("cursor").asText()),
                UUID.fromString(node.path("message_id").asText()),
                UUID.fromString(node.path("producer_id").asText()),
                node.path("sequence").asLong(),
                Long.parseLong(node.path("event_time_ns").asText()),
                Long.parseLong(node.path("ingest_time_ns").asText()),
                b64(node.path("key_base64").asText()),
                b64(node.path("payload_base64").asText()),
                metadata(node.path("metadata_base64")));
    }

    public static List<FeedRecord> records(JsonNode array) {
        if (array == null || !array.isArray()) {
            return List.of();
        }
        List<FeedRecord> out = new ArrayList<>(array.size());
        array.forEach(node -> out.add(record(node)));
        return List.copyOf(out);
    }

    public static ServerFeedback feedback(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return null;
        }
        return new ServerFeedback(
                node.path("recommended_batch_count").asInt(),
                node.path("recommended_batch_bytes").asInt(),
                node.path("pressure").asDouble(),
                node.path("retry_after_ms").asLong(),
                node.path("max_frame_bytes").asLong(),
                node.path("hot_key").asBoolean(false));
    }

    public static AppendResult appendResult(JsonNode node) {
        return new AppendResult(
                UUID.fromString(node.path("message_id").asText()),
                Cursor.of(node.path("cursor").asText()),
                node.path("deduplicated").asBoolean(false),
                node.path("durability").asText(),
                feedback(node.path("feedback")));
    }

    public static ReaderSession readerSession(JsonNode node) {
        return new ReaderSession(
                UUID.fromString(node.path("reader_id").asText()),
                UUID.fromString(node.path("feed_id").asText()),
                node.path("session_epoch").asLong(),
                node.path("capacity").asInt(),
                Cursor.of(node.path("delivered_cursor").isNull() ? null : node.path("delivered_cursor").asText()),
                Cursor.of(node.path("acknowledged_cursor").isNull() ? null : node.path("acknowledged_cursor").asText()));
    }

    public static ReaderPage readerPage(JsonNode node) {
        return new ReaderPage(
                records(node.path("records")),
                Cursor.of(node.path("delivered_cursor").isNull() ? null : node.path("delivered_cursor").asText()),
                Cursor.of(node.path("acknowledged_cursor").isNull() ? null : node.path("acknowledged_cursor").asText()),
                node.path("recommended_capacity").asInt(),
                node.path("retry_after_ms").asLong());
    }

    public static ControlExecution controlExecution(JsonNode node) {
        List<ControlExecution.StatementResult> results = new ArrayList<>();
        node.path("results").forEach(result -> results.add(new ControlExecution.StatementResult(
                result.path("statement").asText(),
                result.path("message").asText(),
                result.path("data"))));
        return new ControlExecution(
                UUID.fromString(node.path("request_id").asText()),
                node.path("revision").asLong(),
                node.path("authority").asText(),
                node.path("warning").asText(),
                List.copyOf(results));
    }

    public static ObjectNode appendRequest(
            UUID requestId,
            String writer,
            long sessionEpoch,
            byte[] key,
            byte[] payload,
            Long eventTimeNs,
            Map<String, byte[]> metadata) {
        ObjectNode node = Transport.json().createObjectNode();
        node.put("request_id", requestId.toString());
        node.put("writer", writer);
        node.put("session_epoch", sessionEpoch);
        if (eventTimeNs != null) {
            node.put("event_time_ns", Long.toString(eventTimeNs));
        }
        node.put("key_base64", b64(key));
        node.put("payload_base64", b64(payload));
        ObjectNode meta = node.putObject("metadata_base64");
        if (metadata != null) {
            metadata.forEach((name, value) -> meta.put(name, b64(value)));
        }
        return node;
    }
}
