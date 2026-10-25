package dev.whitewater.model;

import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.whitewater.internal.Transport;

import java.util.Objects;

/**
 * The processing step a Pipe applies to each delivered input record.
 * Build with {@link #forward()}, {@link #enrich(...)}, or
 * {@link #window(...)}; the wire form is internally tagged on
 * {@code operation}.
 *
 * <p>Every duration is declared as a {@link Time} — the SDK extracts
 * nanoseconds internally for the wire.
 */
public record PipeOperation(
        String operation,
        Extractor key,
        Time window,
        WindowAggregation aggregation,
        WindowTime time,
        Time maxLate,
        Time watermark,
        Time grace,
        Time expire,
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
     * Accumulate each delivered record into fixed {@code window}-wide
     * panes of a managed StateStore view. Panes are immutable once
     * written; the late-event policy is one of two mutually exclusive
     * models — {@code maxLate} bounds ingest-clock lateness (later
     * arrivals quarantine into a countable {@code late} row), while
     * {@code watermark} drives a durable event-time horizon
     * {@code max(event_time) - watermark} that closes a pane once it
     * passes the pane end — refused under the ingest clock. Under a
     * watermark, {@code grace} bounds how long an emitted pane still
     * accepts revising events — each revises the pane and re-emits it
     * under a bumped revision; past the grace bound they count into the
     * {@code late} row. {@code expire} bounds pane retention once the
     * pane provably closes. All durations are {@link Time} values —
     * {@code Time.h(1)}, {@code Time.m(5)}, {@code Time.s(30)}; the SDK
     * extracts nanoseconds internally for the wire.
     */
    public static PipeOperation window(
            Time window,
            Extractor key,
            WindowAggregation aggregation,
            WindowTime time,
            Time maxLate,
            Time watermark,
            Time grace,
            Time expire,
            Long keyLimit,
            Long byteLimit) {
        requirePositive(window, "window");
        requirePositiveOrAbsent(maxLate, "maxLate");
        requirePositiveOrAbsent(watermark, "watermark");
        requirePositiveOrAbsent(grace, "grace");
        requirePositiveOrAbsent(expire, "expire");
        if (maxLate != null && watermark != null) {
            throw new IllegalArgumentException(
                    "maxLate and watermark are alternative lateness models — declare exactly one");
        }
        if (watermark != null && time == WindowTime.INGEST) {
            throw new IllegalArgumentException(
                    "watermark is meaningless under the ingest clock — the horizon would follow arrival order, not event-time disorder");
        }
        if (grace != null && watermark == null) {
            throw new IllegalArgumentException(
                    "grace requires watermark — revision bounds ride the event-time horizon");
        }
        return new PipeOperation("window",
                Objects.requireNonNull(key, "window key extractor is required"),
                window,
                Objects.requireNonNull(aggregation, "aggregation is required"),
                time == null ? WindowTime.EVENT : time,
                maxLate, watermark, grace, expire, keyLimit, byteLimit,
                null, null, null);
    }

    /**
     * Window bounded by ingest-clock lateness, retention, and cost
     * limits — the watermark/grace lateness model stays unset; declare
     * an event-time horizon on the ten-argument form.
     */
    public static PipeOperation window(
            Time window,
            Extractor key,
            WindowAggregation aggregation,
            WindowTime time,
            Time maxLate,
            Time expire,
            Long keyLimit,
            Long byteLimit) {
        return window(window, key, aggregation, time, maxLate,
                null, null, expire, keyLimit, byteLimit);
    }

    /** Window with event time and no lateness/limit bounds. */
    public static PipeOperation window(Time window, Extractor key, WindowAggregation aggregation) {
        return window(window, key, aggregation, null, null, null, null, null);
    }

    /**
     * Event-time window with a {@code watermark} close horizon and no
     * revision grace — an event landing on an emitted pane quarantines
     * into the {@code late} row.
     */
    public static PipeOperation window(
            Time window,
            Extractor key,
            WindowAggregation aggregation,
            Time watermark) {
        return window(window, key, aggregation, WindowTime.EVENT,
                null, watermark, null, null, null, null);
    }

    /**
     * Event-time window with a {@code watermark} close horizon plus a
     * revision {@code grace} — late events landing on an emitted pane
     * while the horizon stays within {@code grace} of the pane end
     * revise and re-emit it under a bumped revision.
     */
    public static PipeOperation window(
            Time window,
            Extractor key,
            WindowAggregation aggregation,
            Time watermark,
            Time grace) {
        return window(window, key, aggregation, WindowTime.EVENT,
                null, watermark, grace, null, null, null);
    }

    private static void requirePositive(Time value, String name) {
        if (value == null || !value.isPositive()) {
            throw new IllegalArgumentException(name + " must be a positive Time");
        }
    }

    private static void requirePositiveOrAbsent(Time value, String name) {
        if (value != null && !value.isPositive()) {
            throw new IllegalArgumentException(
                    name + " must be a positive Time when declared");
        }
    }

    /** The wire form: {@code {"operation":"window","window_ns":…}}. */
    public ObjectNode toWire() {
        ObjectNode node = Transport.json().createObjectNode();
        node.put("operation", operation);
        if (key != null) {
            node.set("key", key.toWire());
        }
        if (window != null) {
            node.put("window_ns", window.nanos());
        }
        if (aggregation != null) {
            node.set("aggregation", aggregation.toWire());
        }
        if (time != null) {
            node.put("time", time.wire());
        }
        if (maxLate != null) {
            node.put("max_late_ns", maxLate.nanos());
        }
        if (watermark != null) {
            node.put("watermark_ns", watermark.nanos());
        }
        if (grace != null) {
            node.put("grace_ns", grace.nanos());
        }
        if (expire != null) {
            node.put("expire_ns", expire.nanos());
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
