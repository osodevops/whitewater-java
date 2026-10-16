package dev.whitewater.model;

import java.util.List;

/**
 * A named-key lookup result: every primary row the value points at
 * (non-unique named keys are one-to-many), plus the serving replica's
 * freshness evidence.
 */
public record StoreRows<K, V>(List<StoreRow<K, V>> rows, StoreFreshness freshness) {
}
