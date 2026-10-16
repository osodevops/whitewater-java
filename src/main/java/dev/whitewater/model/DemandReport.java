package dev.whitewater.model;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

/**
 * A demand snapshot for one resource kind: per-resource {@link DemandRow}s
 * plus the per-Domain rollup map. Counter names are a server-owned
 * vocabulary that evolves without an SDK bump, so rows keep their fields
 * verbatim — the same contract as {@link PipeInfo#operation}.
 */
public record DemandReport(List<DemandRow> rows, JsonNode domains) {

    /** The domain rollup row for {@code domain}, when attributed. */
    public JsonNode domain(String domain) {
        return domains == null ? null : domains.get(domain);
    }
}
