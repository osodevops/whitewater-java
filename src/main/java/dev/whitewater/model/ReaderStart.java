package dev.whitewater.model;

/**
 * Where a Reader or Subscription starts consuming a Feed — maps to the WCL
 * {@code START AT} clause. {@link #beginning()} and {@link #now()} cover the
 * common cases; {@link #cursor(Cursor)} and {@link #timestampNs(long)} resume
 * from an explicit position.
 */
public final class ReaderStart {

    public enum Kind {BEGINNING, NOW, CURSOR, TIMESTAMP}

    private final Kind kind;
    private final Cursor cursor;
    private final long timestampNs;

    private ReaderStart(Kind kind, Cursor cursor, long timestampNs) {
        this.kind = kind;
        this.cursor = cursor;
        this.timestampNs = timestampNs;
    }

    /** From the oldest retained record. */
    public static ReaderStart beginning() {
        return new ReaderStart(Kind.BEGINNING, null, 0);
    }

    /** From records appended after creation. */
    public static ReaderStart now() {
        return new ReaderStart(Kind.NOW, null, 0);
    }

    /** From an opaque Cursor previously returned by the Riverbed. */
    public static ReaderStart cursor(Cursor cursor) {
        return new ReaderStart(Kind.CURSOR, cursor, 0);
    }

    /** From a signed Unix epoch nanosecond timestamp. */
    public static ReaderStart timestampNs(long nanos) {
        return new ReaderStart(Kind.TIMESTAMP, null, nanos);
    }

    public Kind kind() {
        return kind;
    }

    /** Set when {@link #kind()} is {@code CURSOR}. */
    public Cursor cursor() {
        return cursor;
    }

    /** Set when {@link #kind()} is {@code TIMESTAMP}. */
    public long timestampNs() {
        return timestampNs;
    }

    /** The WCL {@code START AT} operand, e.g. {@code BEGINNING}. */
    public String wcl() {
        return switch (kind) {
            case BEGINNING -> "BEGINNING";
            case NOW -> "NOW";
            case CURSOR -> "CURSOR '" + escape(cursor.value()) + "'";
            case TIMESTAMP -> "TIMESTAMP " + timestampNs;
        };
    }

    /**
     * The typed-command form: {@code {"kind":"beginning"}},
     * {@code {"kind":"cursor","cursor":"…"}},
     * {@code {"kind":"timestamp","cursor":<i64ns>}}.
     */
    public com.fasterxml.jackson.databind.node.ObjectNode toWire() {
        var node = new com.fasterxml.jackson.databind.node.JsonNodeFactory(false)
                .objectNode();
        switch (kind) {
            case BEGINNING -> node.put("kind", "beginning");
            case NOW -> node.put("kind", "now");
            case CURSOR -> node.put("kind", "cursor").put("cursor", cursor.value());
            case TIMESTAMP -> node.put("kind", "timestamp")
                    .put("cursor", timestampNs);
        }
        return node;
    }

    private static String escape(String value) {
        return value.replace("'", "''");
    }
}
