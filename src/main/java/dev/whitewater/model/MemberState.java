package dev.whitewater.model;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Map;

/**
 * The authoritative view of a Subscription's member state: the shared
 * {@code ownershipEpoch}, the per-member fencing epochs, and the live
 * work leases.
 */
public record MemberState(
        String subscription,
        long ownershipEpoch,
        long tick,
        Map<String, Long> memberEpochs,
        JsonNode leases) {}
