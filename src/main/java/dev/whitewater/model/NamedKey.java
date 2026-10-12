package dev.whitewater.model;

/**
 * One declared secondary key on a StateStore. Non-unique keys map a value
 * to every primary row carrying it (the group/list lookup); {@code unique}
 * enforces one row per value, arbitrated first-committed-wins by the
 * store's journal order.
 *
 * <p>{@code extractor} is feed-sourced stores only: the projection
 * server-side replay applies to each committed source record. Manual
 * stores take values on each {@code put} instead.
 */
public record NamedKey(String name, boolean unique, Extractor extractor) {

    /** Bounds the named secondary keys one StateStore declares. */
    public static final int MAX_NAMED_KEYS = 10;

    public NamedKey(String name, boolean unique) {
        this(name, unique, null);
    }
}
