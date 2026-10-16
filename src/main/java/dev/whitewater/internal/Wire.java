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
        node.properties().forEach(entry ->
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

    public static ObjectNode feedAppendRequest(String feed, FeedAppend record) {
        ObjectNode node = Transport.json().createObjectNode();
        node.put("request_id", record.requestId().toString());
        node.put("feed", feed);
        node.put("writer_session_id", record.writerSessionId().toString());
        node.put("writer_epoch", record.writerEpoch());
        node.put("sequence", record.sequence());
        if (record.eventTimeNs() != null) {
            node.put("event_time_ns", Long.toString(record.eventTimeNs()));
        }
        node.put("key_base64", b64(record.key()));
        node.put("payload_base64", b64(record.payload()));
        ObjectNode meta = node.putObject("metadata_base64");
        if (record.metadata() != null) {
            record.metadata().forEach((name, value) -> meta.put(name, b64(value)));
        }
        return node;
    }

    /**
     * One typed StateStore/Index value for the wire: {@code null} -> null,
     * {@link Boolean} -> bool, {@link Byte}/{@link Short}/{@link Integer}/
     * {@link Long}/{@link java.math.BigInteger} -> i64, {@link String} ->
     * text, {@code byte[]} -> bytes. Floats/doubles are refused — the wire
     * carries exact i64/text/bytes.
     */
    public static JsonNode indexValue(Object value) {
        ObjectNode node = Transport.json().createObjectNode();
        if (value == null) {
            node.put("kind", "null");
        } else if (value instanceof Boolean bool) {
            node.put("kind", "bool").put("value", bool);
        } else if (value instanceof Byte || value instanceof Short
                || value instanceof Integer || value instanceof Long) {
            node.put("kind", "i64").put("value", ((Number) value).longValue());
        } else if (value instanceof java.math.BigInteger big) {
            node.put("kind", "i64").put("value", big.longValueExact());
        } else if (value instanceof String text) {
            node.put("kind", "text").put("value", text);
        } else if (value instanceof byte[] bytes) {
            node.put("kind", "bytes").put("value_base64", b64(bytes));
        } else {
            throw new IllegalArgumentException(
                    "unsupported StateStore value type: " + value.getClass().getSimpleName());
        }
        return node;
    }

    /** {@code {department: "eng", composite: List.of(v1, v2)}} -> the mutation's named entries. */
    public static com.fasterxml.jackson.databind.node.ArrayNode namedValues(Map<String, ?> named) {
        var array = Transport.json().createArrayNode();
        named.forEach((name, value) -> {
            ObjectNode entry = Transport.json().createObjectNode();
            entry.put("name", name);
            var values = entry.putArray("values");
            if (value instanceof List<?> list) {
                list.forEach(component -> values.add(indexValue(component)));
            } else {
                values.add(indexValue(value));
            }
            array.add(entry);
        });
        return array;
    }

    public static MutationResult mutationResult(JsonNode node) {
        return new MutationResult(
                UUID.fromString(node.path("store_id").asText()),
                UUID.fromString(node.path("mutation_id").asText()),
                node.path("sequence").asLong(),
                node.path("deduplicated").asBoolean(false));
    }

    public static StoreFreshness storeFreshness(JsonNode node) {
        return new StoreFreshness(
                node.path("applied_seq").asLong(),
                node.path("committed_seq").asLong(),
                node.path("behind").asBoolean(false),
                node.path("served_by").asText());
    }

    public static StoreRow<byte[], byte[]> storeRow(JsonNode node, StoreFreshness freshness) {
        JsonNode cursor = node.path("applied_cursor");
        return new StoreRow<>(
                b64(node.path("key_base64").asText()),
                b64(node.path("payload_base64").asText()),
                cursor.isNull() || cursor.isMissingNode() ? null : cursor.asText(),
                freshness);
    }

    public static StoreRows<byte[], byte[]> storeRows(JsonNode node) {
        StoreFreshness freshness = storeFreshness(node);
        List<StoreRow<byte[], byte[]>> rows = new ArrayList<>();
        JsonNode list = node.path("rows");
        if (list.isArray()) {
            list.forEach(item -> rows.add(storeRow(item, freshness)));
        }
        return new StoreRows<>(List.copyOf(rows), freshness);
    }

    public static Health health(JsonNode node) {
        JsonNode protocol = node.path("protocol");
        JsonNode limits = node.path("limits");
        return new Health(
                node.path("status").asText(),
                node.path("node_id").asText(null),
                node.path("members").asInt(0),
                new Health.Protocol(
                        protocol.path("major").asInt(0),
                        protocol.path("minor").asInt(0)),
                new Health.Limits(
                        limits.path("max_frame_bytes").asLong(0),
                        limits.path("max_committed_read_bytes").asLong(0),
                        limits.path("max_logical_read_frames").asLong(0),
                        limits.path("max_logical_read_ranges").asLong(0),
                        limits.path("subscription_member_fetch_max_limit").asLong(0),
                        limits.path("subscription_member_max_lease_ticks").asLong(0)));
    }

    public static IndexPage indexPage(JsonNode node) {
        List<IndexPage.Row> rows = new ArrayList<>();
        JsonNode list = node.path("rows");
        if (list.isArray()) {
            list.forEach(item -> rows.add(new IndexPage.Row(
                    b64(item.path("key_base64").asText()),
                    b64(item.path("payload_base64").asText()))));
        }
        return new IndexPage(
                node.path("index").asText(),
                node.path("index_id").isTextual()
                        ? UUID.fromString(node.path("index_id").asText()) : null,
                node.path("generation").asLong(),
                List.copyOf(rows),
                node.path("applied_cursor").isNull() ? null
                        : Cursor.of(node.path("applied_cursor").asText()),
                node.path("target_cursor").isNull() ? null
                        : Cursor.of(node.path("target_cursor").asText()),
                node.path("behind").asBoolean(false),
                node.path("truncated").asBoolean(false),
                node.path("served_by").asText());
    }

    public static MemberState memberState(JsonNode node) {
        Map<String, Long> memberEpochs = new LinkedHashMap<>();
        node.path("member_epochs").properties().forEach(entry ->
                memberEpochs.put(entry.getKey(), entry.getValue().asLong()));
        return new MemberState(
                node.path("subscription").asText(),
                node.path("ownership_epoch").asLong(),
                node.path("tick").asLong(),
                Map.copyOf(memberEpochs),
                node.path("leases"));
    }

    public static SeekResult seekResult(JsonNode node) {
        Map<String, String> positions = new LinkedHashMap<>();
        node.path("positions").properties().forEach(entry ->
                positions.put(entry.getKey(), entry.getValue().asText()));
        return new SeekResult(
                node.path("request_id").isTextual()
                        ? UUID.fromString(node.path("request_id").asText()) : null,
                Cursor.of(node.path("cursor").asText(null)),
                Map.copyOf(positions),
                node.path("ownership_epoch").asLong());
    }

    public static StoreSync storeSync(JsonNode node) {
        return new StoreSync(
                node.path("store_id").isTextual()
                        ? UUID.fromString(node.path("store_id").asText()) : null,
                node.path("applied").asLong(),
                node.path("frontier").isNull() ? null : node.path("frontier").asText(),
                node.path("complete").asBoolean(false));
    }

    public static StorePage<byte[], byte[]> storePage(JsonNode node) {
        List<StoreRow<byte[], byte[]>> rows = new ArrayList<>();
        JsonNode list = node.path("rows");
        if (list.isArray()) {
            list.forEach(item -> rows.add(storeRow(item, null)));
        }
        JsonNode next = node.path("next_key_base64");
        return new StorePage<>(
                List.copyOf(rows),
                next.isNull() || next.isMissingNode() || next.asText().isEmpty()
                        ? null
                        : b64(next.asText()),
                node.path("pinned_seq").asLong(),
                node.path("served_by").asText());
    }
}
