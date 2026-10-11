package dev.whitewater.model;

/**
 * Where a Reader or Subscription starts consuming a Feed — maps to the WCL
 * {@code START AT} clause. {@link #beginning()} and {@link #now()} cover the
 * common cases; {@link #cursor(Cursor)} and {@link #timestampNs(long)} resume
 * from an explicit position.
 */
public final class ReaderStart {

    private final String wcl;

    private ReaderStart(String wcl) {
        this.wcl = wcl;
    }

    /** From the oldest retained record. */
    public static ReaderStart beginning() {
        return new ReaderStart("BEGINNING");
    }

    /** From records appended after creation. */
    public static ReaderStart now() {
        return new ReaderStart("NOW");
    }

    /** From an opaque Cursor previously returned by the Riverbed. */
    public static ReaderStart cursor(Cursor cursor) {
        return new ReaderStart("CURSOR '" + escape(cursor.value()) + "'");
    }

    /** From a signed Unix epoch nanosecond timestamp. */
    public static ReaderStart timestampNs(long nanos) {
        return new ReaderStart("TIMESTAMP " + nanos);
    }

    /** The WCL {@code START AT} operand, e.g. {@code BEGINNING}. */
    public String wcl() {
        return wcl;
    }

    private static String escape(String value) {
        return value.replace("'", "''");
    }
}
