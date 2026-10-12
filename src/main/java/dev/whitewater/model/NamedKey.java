package dev.whitewater.model;

/**
 * One declared secondary key on a StateStore. Non-unique keys map a value
 * to every primary row carrying it (the group/list lookup); {@code unique}
 * enforces one row per value, arbitrated first-committed-wins by the
 * store's journal order.
 */
public record NamedKey(String name, boolean unique) {

    /** Bounds the named secondary keys one StateStore declares. */
    public static final int MAX_NAMED_KEYS = 10;
}
