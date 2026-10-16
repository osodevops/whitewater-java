package dev.whitewater.model;

import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.whitewater.internal.Transport;

/**
 * A Node's self-reported measured capacity, committed into the catalog so
 * every replica's eligibility evaluation reads identical data.
 */
public record NodeCapacity(
        long cpuCores,
        long memoryBytes,
        long diskTotalBytes,
        long diskFreeBytes,
        long networkMbps,
        String failureDomain) {

    /** The wire form. */
    public ObjectNode toWire() {
        ObjectNode node = Transport.json().createObjectNode();
        node.put("cpu_cores", cpuCores);
        node.put("memory_bytes", memoryBytes);
        node.put("disk_total_bytes", diskTotalBytes);
        node.put("disk_free_bytes", diskFreeBytes);
        node.put("network_mbps", networkMbps);
        node.put("failure_domain", failureDomain == null ? "" : failureDomain);
        return node;
    }
}
