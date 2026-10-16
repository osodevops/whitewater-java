package dev.whitewater.model;

import java.util.UUID;

/**
 * One Feed-derived StateStore replay pass: how far the store's journal
 * applied, the frontier it reached, and whether the pass finished the
 * outstanding source history.
 */
public record StoreSync(UUID storeId, long applied, String frontier, boolean complete) {}
