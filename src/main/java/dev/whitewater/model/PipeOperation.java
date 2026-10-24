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
        Long watermarkNs,
        Long graceNs,
        Long expireNs,
        Long keyLimit,
        Long byteLimit,
        EnrichFreshness freshness,
        String named,
        EnrichMiss onMiss) {

    /** Copy each delivered record to the output Feed unchanged. */
    public static PipeOperation forward() {
        return new PipeOperation("forward", null, null, null, null,
                null, null, null, null, null, null, null, null, null);
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
                null, null, null, null, null, null, null, null, null,
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
     * written; the late-event policy is one of two mutually exclusive
     * models — {@code maxLateNs} bounds ingest-clock lateness (later
     * arrivals quarantine into a countable {@code late} row), while
     * {@code watermarkNs} drives a durable event-time horizon
     * {@code max(event_time) - watermarkNs} that closes a pane once it
     * passes the pane end — refused under the ingest clock. Under a
     * watermark, {@code graceNs} bounds how long an emitted pane still
     * accepts revising events — each revises the pane and re-emits it
     * under a bumped revision; past the grace bound they count into the
     * {@code late} row. {@code expireNs} bounds pane retention once the
     * pane provably closes.
     */
    public static PipeOperation window(
            long windowNs,
            Extractor key,
            WindowAggregation aggregation,
            WindowTime time,
            Long maxLateNs,
            Long watermarkNs,
            Long graceNs,
            Long expireNs,
            Long keyLimit,
            Long byteLimit) {
        if (windowNs <= 0) {
            throw new IllegalArgumentException("windowNs must be positive");
        }
        if (maxLateNs != null && watermarkNs != null) {
            throw new IllegalArgumentException(
                    "maxLateNs and watermarkNs are alternative lateness models — declare exactly one");
        }
        if (watermarkNs != null && time == WindowTime.INGEST) {
            throw new IllegalArgumentException(
                    "watermarkNs is meaningless under the ingest clock — the horizon would follow arrival order, not event-time disorder");
        }
        if (graceNs != null && watermarkNs == null) {
            throw new IllegalArgumentException(
                    "graceNs requires watermarkNs — revision bounds ride the event-time horizon");
        }
        return new PipeOperation("window",
                Objects.requireNonNull(key, "window key extractor is required"),
                windowNs,
                Objects.requireNonNull(aggregation, "aggregation is required"),
                time == null ? WindowTime.EVENT : time,
                maxLateNs, watermarkNs, graceNs, expireNs, keyLimit, byteLimit,
                null, null, null);
    }

    /**
     * Window bounded by ingest-clock lateness, retention, and cost
     * limits — the watermark/grace lateness model stays unset; declare
     * an event-time horizon on the ten-argument form.
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
        return window(windowNs, key, aggregation, time, maxLateNs,
                null, null, expireNs, keyLimit, byteLimit);
    }

    /** Window with event time and no lateness/limit bounds. */
    public static PipeOperation window(long windowNs, Extractor key, WindowAggregation aggregation) {
        return window(windowNs, key, aggregation, null, null, null, null, null);
    }

    /**
     * Event-time window with a watermark close horizon and no revision
     * grace — an event landing on an emitted pane quarantines into the
     * {@code late} row.
     */
    public static PipeOperation window(
            long windowNs,
            Extractor key,
            WindowAggregation aggregation,
            long watermarkNs) {
        return window(windowNs, key, aggregation, WindowTime.EVENT,
                null, watermarkNs, null, null, null, null);
    }

    /**
     * Event-time window with a watermark close horizon plus a revision
     * grace — late events landing on an emitted pane while the horizon
     * stays within {@code graceNs} of the pane end revise and re-emit it
     * under a bumped revision.
     */
    public static PipeOperation window(
            long windowNs,
            Extractor key,
            WindowAggregation aggregation,
            long watermarkNs,
            long graceNs) {
        return window(windowNs, key, aggregation, WindowTime.EVENT,
                null, watermarkNs, graceNs, null, null, null);
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
        if (watermarkNs != null) {
            node.put("watermark_ns", watermarkNs);
        }
        if (graceNs != null) {
            node.put("grace_ns", graceNs);
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
