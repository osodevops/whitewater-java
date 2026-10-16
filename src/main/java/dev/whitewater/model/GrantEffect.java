package dev.whitewater.model;

/** Whether a grant allows or explicitly denies the action. */
public enum GrantEffect {
    ALLOW,
    DENY;

    /** The wire value. */
    public String wire() {
        return name().toLowerCase();
    }
}
