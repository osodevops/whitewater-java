package dev.whitewater.model;

import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.whitewater.internal.Transport;

import java.util.Objects;

/**
 * The processing step a Pipe applies to each delivered input record.
 * Build with {@link #forward()}, {@link #enrich(...)}, or
 * {@link #window(...)}; the wire form is internally tagged on
 * {@code operation}.
 */
public record PipeOperation(
        String operation,
        Extractor key,
        Long windowNs,
        WindowAggregation aggregation,
        WindowTime time,
        Long maxLateNs,
        Long expireNs,
        Long keyLimit,
        Long byteLimit,
        EnrichFreshness freshness,
        String named,
        EnrichMiss onMiss) {

    /** Copy each delivered record to the output Feed unchanged. */
    public static PipeOperation forward() {
        return new PipeOperation("forward", null, null, null, null,
                null, null, null, null, null, null, null);
    }

    /**
     * Point-lookup enrichment against {@code store}: resolve one row by
     * the projected {@code key} and copy its payload into output Metadata
     * ({@code enrich.payload}, {@code enrich.matched}). The lookup
     * resolution rides inside the effect journal, so replayed outputs are
     * byte-identical.
     */
    public static PipeOperation enrich(
            Extractor key,
            EnrichFreshness freshness,
            String named,
            EnrichMiss onMiss) {
        return new PipeOperation("enrich",
                Objects.requireNonNull(key, "enrich key extractor is required"),
                null, null, null, null, null, null, null,
                freshness == null ? EnrichFreshness.LATEST : freshness,
                named,
                onMiss == null ? EnrichMiss.FORWARD : onMiss);
    }

    /** Enrich with defaults: latest freshness, forward-on-miss. */
    public static PipeOperation enrich(Extractor key) {
        return enrich(key, null, null, null);
    }

    /**
     * Accumulate each delivered record into fixed {@code windowNs}-wide
     * panes of a managed StateStore view. Panes are immutable once
     * written; {@code maxLateNs} bounds late-event policy (later arrivals
     * quarantine into a countable {@code late} row) and {@code expireNs}
     * bounds pane retention once the pane provably closes.
     */
    public static PipeOperation window(
            long windowNs,
            Extractor key,
            WindowAggregation aggregation,
            WindowTime time,
            Long maxLateNs,
            Long expireNs,
            Long keyLimit,
            Long byteLimit) {
        if (windowNs <= 0) {
            throw new IllegalArgumentException("windowNs must be positive");
        }
        return new PipeOperation("window",
                Objects.requireNonNull(key, "window key extractor is required"),
                windowNs,
                Objects.requireNonNull(aggregation, "aggregation is required"),
                time == null ? WindowTime.EVENT : time,
                maxLateNs, expireNs, keyLimit, byteLimit,
                null, null, null);
    }

    /** Window with event time and no lateness/limit bounds. */
    public static PipeOperation window(long windowNs, Extractor key, WindowAggregation aggregation) {
        return window(windowNs, key, aggregation, null, null, null, null, null);
    }

    /** The wire form: {@code {"operation":"window","window_ns":…}}. */
    public ObjectNode toWire() {
        ObjectNode node = Transport.json().createObjectNode();
        node.put("operation", operation);
        if (key != null) {
            node.set("key", key.toWire());
        }
        if (windowNs != null) {
            node.put("window_ns", windowNs);
        }
        if (aggregation != null) {
            node.set("aggregation", aggregation.toWire());
        }
        if (time != null) {
            node.put("time", time.wire());
        }
        if (maxLateNs != null) {
            node.put("max_late_ns", maxLateNs);
        }
        if (expireNs != null) {
            node.put("expire_ns", expireNs);
        }
        if (keyLimit != null) {
            node.put("key_limit", keyLimit);
        }
        if (byteLimit != null) {
            node.put("byte_limit", byteLimit);
        }
        if (freshness != null) {
            node.put("freshness", freshness.wire());
        }
        if (named != null) {
            node.put("named", named);
        }
        if (onMiss != null) {
            node.put("on_miss", onMiss.wire());
        }
        return node;
    }
}
