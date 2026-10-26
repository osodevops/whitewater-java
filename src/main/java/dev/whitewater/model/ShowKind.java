package dev.whitewater.model;

/** Listing kinds for {@code SHOW} / the {@code show} typed command. */
public enum ShowKind {
    DOMAINS,
    SPACES,
    FEEDS,
    WRITERS,
    READERS,
    SUBSCRIPTIONS,
    ROLES,
    GRANTS,
    API_KEYS,
    STORAGE_NODES,
    HISTORY_POLICIES,
    SCHEMAS,
    PIPES,
    INDEXES,
    STATE_STORES;

    /** The wire value. */
    public String wire() {
        return name().toLowerCase();
    }
}
