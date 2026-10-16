package dev.whitewater.model;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * One demand row for a declared resource (Feed, Reader, Subscription,
 * Index, or Pipe). {@code fields} carries the server's counters verbatim
 * ({@code appended_records}, {@code drives}, {@code queries}, …) — new
 * counters arrive without an SDK update.
 */
public record DemandRow(String name, String domain, JsonNode fields) {

    /** One counter as {@code Long} when present and numeric. */
    public Long counter(String field) {
        var node = fields == null ? null : fields.get(field);
        return node != null && node.isNumber() ? node.longValue() : null;
    }
}
