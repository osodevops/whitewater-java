package dev.whitewater.model;

import java.util.List;

/**
 * A named-key lookup result: every primary row the value points at plus
 * the serving replica's freshness evidence.
 */
public record StoreRows(List<StoreRow> rows, StoreFreshness freshness) {
}
