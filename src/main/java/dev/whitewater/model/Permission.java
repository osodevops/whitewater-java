package dev.whitewater.model;

/** Grantable actions: {@code READ}, {@code WRITE}, or {@code MANAGE}. */
public enum Permission {
    READ,
    WRITE,
    MANAGE;

    /** The wire value. */
    public String wire() {
        return name().toLowerCase();
    }
}
