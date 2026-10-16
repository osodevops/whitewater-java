package dev.whitewater.model;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.UUID;

/**
 * The catalog row {@code DESCRIBE PIPE}/{@code SHOW PIPES} returns for a
 * declared Pipe. {@code operation} stays an opaque node — Forward,
 * Window, and Enrich variants evolve server-side without an SDK type
 * bump.
 */
public record PipeInfo(
        String name,
        /** {@code declared} while driven, {@code paused} under {@code PAUSE PIPE}. */
        String stage,
        UUID subscriptionId,
        JsonNode operation,
        UUID outputFeedId,
        UUID outputStoreId,
        String status,
        long createdAtNs) {

    /** Decode one catalog row; {@code null} fields stay absent. */
    public static PipeInfo from(JsonNode node) {
        if (node == null || node.isNull() || !node.isObject()) {
            return null;
        }
        var subscription = uuid(node, "subscription_id");
        if (subscription == null) {
            return null;
        }
        return new PipeInfo(
                node.path("name").asText(),
                node.path("stage").asText(),
                subscription,
                node.get("operation"),
                uuid(node, "output_feed_id"),
                uuid(node, "output_store_id"),
                node.path("status").asText(),
                node.path("created_at_ns").asLong());
    }

    private static UUID uuid(JsonNode node, String field) {
        var raw = node.path(field);
        if (!raw.isTextual()) {
            return null;
        }
        return UUID.fromString(raw.asText());
    }
}
