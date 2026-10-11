package dev.whitewater.model;

/**
 * Options for an anonymous temporary Reader fetch. {@code after} continues
 * after an opaque Cursor; {@code afterEventTimeNs} seeks by record event time
 * (signed epoch nanoseconds); {@code newOnly} atomically positions at the
 * current end Cursor without returning history; {@code tail} plus
 * {@code waitMs} long-polls for newly committed records.
 *
 * {@code after}, {@code newOnly}, and {@code afterEventTimeNs} are mutually
 * exclusive on the server.
 */
public record TemporaryFetchOptions(
        Cursor after,
        Integer limit,
        Boolean newOnly,
        Boolean tail,
        Long waitMs,
        Long afterEventTimeNs) {

    /** Read a bounded page after {@code after} (or from the beginning). */
    public static TemporaryFetchOptions page(Cursor after, int limit) {
        return new TemporaryFetchOptions(after, limit, null, null, null, null);
    }

    /** Long-poll for newly committed records after {@code after}. */
    public static TemporaryFetchOptions tail(Cursor after, int limit, long waitMs) {
        return new TemporaryFetchOptions(after, limit, null, true, waitMs, null);
    }

    /** Position at the current end Cursor without returning records. */
    public static TemporaryFetchOptions atEnd() {
        return new TemporaryFetchOptions(null, null, true, null, null, null);
    }

    /** Read a bounded page of records whose event time is at or after {@code nanos}. */
    public static TemporaryFetchOptions afterEventTime(long nanos, int limit) {
        return new TemporaryFetchOptions(null, limit, null, null, null, nanos);
    }

    public TemporaryFetchOptions withAfter(Cursor cursor) {
        return new TemporaryFetchOptions(cursor, limit, newOnly, tail, waitMs, afterEventTimeNs);
    }
}
