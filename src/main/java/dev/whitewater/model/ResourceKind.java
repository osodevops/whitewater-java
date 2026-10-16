package dev.whitewater.model;

/** Resource kinds for {@code describe}, {@code rename}, {@code drop}, and grants. */
public enum ResourceKind {
    DOMAIN,
    SPACE,
    FEED,
    WRITER,
    READER,
    SUBSCRIPTION,
    ROLE,
    PIPE,
    INDEX,
    STATE_STORE;

    /** The wire value. */
    public String wire() {
        return name().toLowerCase();
    }
}
