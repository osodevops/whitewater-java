package dev.whitewater.model;

import java.util.Base64;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A named, thread-safe in-memory store of Feed records — the target of
 * {@code client.feeds().load(...)} and a typical sink for
 * {@code client.subscribe(...)}:
 *
 * <pre>{@code
 * var store = RecordStore.named("employee-store");
 * client.feeds().load("hr.employees", ReaderStart.beginning(), store);
 * try (var sub = client.subscribe("hr-view", "hr.employees",
 *         ReaderStart.now(), store::putAll)) {
 *     // store stays warm until close()
 * }
 * }</pre>
 *
 * Rows are keyed by the record's key bytes; unkeyed records key on their
 * Cursor so every record is retained.
 */
public final class RecordStore implements Iterable<FeedRecord> {

    private final String name;
    private final ConcurrentHashMap<String, FeedRecord> rows = new ConcurrentHashMap<>();

    private RecordStore(String name) {
        this.name = name;
    }

    public static RecordStore named(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("store name is required");
        }
        return new RecordStore(name);
    }

    public String name() {
        return name;
    }

    /** Insert or replace the row for this record's key. */
    public void put(FeedRecord record) {
        rows.put(rowKey(record), record);
    }

    public void putAll(List<FeedRecord> records) {
        for (FeedRecord record : records) {
            put(record);
        }
    }

    /** The latest record for a key, when one has been loaded. */
    public Optional<FeedRecord> get(byte[] key) {
        return Optional.ofNullable(rows.get(keyId(key)));
    }

    public int size() {
        return rows.size();
    }

    public Collection<FeedRecord> values() {
        return List.copyOf(rows.values());
    }

    @Override
    public Iterator<FeedRecord> iterator() {
        return values().iterator();
    }

    private static String rowKey(FeedRecord record) {
        byte[] key = record.key();
        return key != null ? keyId(key) : "cursor:" + record.cursor().value();
    }

    private static String keyId(byte[] key) {
        return Base64.getEncoder().encodeToString(key);
    }
}
