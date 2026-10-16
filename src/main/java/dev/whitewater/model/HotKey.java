package dev.whitewater.model;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Evidence that one Feed Key dominates its traffic window. A strictly
 * ordered hot Key is never parallelized — the honest operator signal is
 * to spread or shed load for the Key, not add capacity.
 */
public record HotKey(JsonNode evidence) {

    /** The Feed carrying the dominant Key, when reported. */
    public String feed() {
        return text("feed");
    }

    /** Dominant-Key share of the window in basis points. */
    public Long shareBps() {
        return number("share_bps");
    }

    /** Records attributed to the Key inside the sampled window. */
    public Long windowRecords() {
        return number("window_records");
    }

    /** Bytes attributed to the Key inside the sampled window. */
    public Long windowBytes() {
        return number("window_bytes");
    }

    private String text(String field) {
        var node = evidence.get(field);
        return node != null && node.isTextual() ? node.asText() : null;
    }

    private Long number(String field) {
        var node = evidence.get(field);
        return node != null && node.isNumber() ? node.longValue() : null;
    }
}
