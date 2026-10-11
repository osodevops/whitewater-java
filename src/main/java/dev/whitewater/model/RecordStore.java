package dev.whitewater.model;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * A named, thread-safe in-memory store of Feed records — the target of
 * {@code client.loadStore(store).fromFeed(...)} and a typical sink for
 * {@code client.subscribe(...)}:
 *
 * <pre>{@code
 * var store = RecordStore.named("employee-store")
 *         .addKey("passport").addKey("department");
 *
 * client.loadStore(store).fromFeed("hr.employees", ReaderStart.beginning(),
 *         (s, record) -> s.upsert(record)
 *                 .withKey(record.key())
 *                 .withNamedKey("passport", record.metadata().get("pp")));
 * }</pre>
 *
 * The primary map keys rows on the key chosen by {@link Upsert#withKey}
 * (the record's own key when loaded via {@code put}/{@code putAll}, the
 * Cursor for unkeyed records). Each declared named key maintains a
 * separate postings map — conceptually {@code employee-store.passport} —
 * of value → primary keys, so named lookups are not unique and return
 * every matching row.
 */
public final class RecordStore implements Iterable<RecordStore.Entry> {

    /** Named secondary keys per store; matches the server Index field budget. */
    public static final int MAX_NAMED_KEYS = 10;

    private final String name;
    private final Set<String> declaredKeys = new LinkedHashSet<>();
    private final ConcurrentHashMap<String, FeedRecord> rows = new ConcurrentHashMap<>();
    private final Map<String, ConcurrentHashMap<String, Set<String>>> namedIndex =
            new ConcurrentHashMap<>();

    private RecordStore(String name) {
        this.name = name;
    }

    public static RecordStore named(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("store name is required");
        }
        return new RecordStore(name);
    }

    /** Declare a named secondary key (at most {@value #MAX_NAMED_KEYS}). */
    public RecordStore addKey(String namedKey) {
        if (namedKey == null || namedKey.isBlank()) {
            throw new IllegalArgumentException("named key is required");
        }
        synchronized (declaredKeys) {
            if (!declaredKeys.contains(namedKey) && declaredKeys.size() >= MAX_NAMED_KEYS) {
                throw new IllegalStateException(
                        "a RecordStore supports at most " + MAX_NAMED_KEYS + " named keys");
            }
            declaredKeys.add(namedKey);
        }
        namedIndex.computeIfAbsent(namedKey, k -> new ConcurrentHashMap<>());
        return this;
    }

    /** The declared named keys, in declaration order. */
    public List<String> namedKeys() {
        synchronized (declaredKeys) {
            return List.copyOf(declaredKeys);
        }
    }

    public String name() {
        return name;
    }

    /** A stored row: its primary key and the record value. */
    public record Entry(byte[] primaryKey, FeedRecord record) {}

    /** Start a row write: choose the primary key and named-key values. */
    public Upsert upsert(FeedRecord record) {
        return new Upsert(record);
    }

    /** Insert or replace keyed on the record's key (Cursor when unkeyed). */
    public void put(FeedRecord record) {
        commitUpsert(record, defaultId(record), Map.of());
    }

    public void putAll(List<FeedRecord> records) {
        for (FeedRecord record : records) {
            put(record);
        }
    }

    /** The latest record for a primary key, when one has been loaded. */
    public Optional<FeedRecord> get(byte[] key) {
        return Optional.ofNullable(rows.get(keyId(key)));
    }

    public Optional<FeedRecord> get(String key) {
        return get(key.getBytes(StandardCharsets.UTF_8));
    }

    /** Every row whose named key equals {@code value} — named keys are not unique. */
    public List<FeedRecord> get(String namedKey, byte[] value) {
        var postings = namedIndex.get(namedKey);
        if (postings == null) {
            throw new IllegalArgumentException("undeclared named key: " + namedKey);
        }
        var primaryIds = postings.get(keyId(value));
        if (primaryIds == null) {
            return List.of();
        }
        var result = new ArrayList<FeedRecord>(primaryIds.size());
        for (String id : primaryIds) {
            var record = rows.get(id);
            if (record != null) {
                result.add(record);
            }
        }
        return result;
    }

    public List<FeedRecord> get(String namedKey, String value) {
        return get(namedKey, value.getBytes(StandardCharsets.UTF_8));
    }

    /** Remove the row for a primary key plus its named-key postings. */
    public boolean delete(byte[] key) {
        String id = keyId(key);
        if (rows.remove(id) == null) {
            return false;
        }
        removePostings(id);
        return true;
    }

    public boolean delete(String key) {
        return delete(key.getBytes(StandardCharsets.UTF_8));
    }

    /** Empty the primary map and every named-key index. */
    public void clear() {
        rows.clear();
        namedIndex.values().forEach(Map::clear);
    }

    /**
     * Copy every row into {@code target}, re-attaching named-key postings;
     * missing named keys are declared on the target.
     */
    public void copy(RecordStore target) {
        for (var namedKey : declaredKeys) {
            target.addKey(namedKey);
        }
        rows.forEach((id, record) -> {
            var named = namedValuesOf(id);
            target.commitUpsert(record, id, named);
        });
    }

    public int size() {
        return rows.size();
    }

    /** Apply {@code consumer} to a stable snapshot of every row. */
    @Override
    public void forEach(Consumer<? super Entry> consumer) {
        for (Entry entry : snapshot()) {
            consumer.accept(entry);
        }
    }

    /** {@code forEach} spread over {@code threads} platform threads. */
    public void forEachParallel(int threads, Consumer<? super Entry> consumer) {
        if (threads < 1) {
            throw new IllegalArgumentException("threads must be >= 1");
        }
        var entries = snapshot();
        if (entries.isEmpty() || threads == 1) {
            entries.forEach(consumer);
            return;
        }
        var pool = Executors.newFixedThreadPool(threads);
        try {
            int chunk = Math.max(1, (entries.size() + threads - 1) / threads);
            var futures = new ArrayList<java.util.concurrent.Future<?>>();
            for (int from = 0; from < entries.size(); from += chunk) {
                var slice = entries.subList(from, Math.min(from + chunk, entries.size()));
                futures.add(pool.submit(() -> slice.forEach(consumer)));
            }
            for (var future : futures) {
                future.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (java.util.concurrent.ExecutionException e) {
            throw new RuntimeException("forEachParallel worker failed", e.getCause());
        } finally {
            pool.shutdown();
        }
    }

    @Override
    public java.util.Iterator<Entry> iterator() {
        return snapshot().iterator();
    }

    /** Row-level write builder returned by {@link #upsert}. */
    public final class Upsert {
        private final FeedRecord record;
        private String primaryId;
        private final Map<String, byte[]> named = new LinkedHashMap<>();

        private Upsert(FeedRecord record) {
            this.record = record;
            this.primaryId = defaultId(record);
        }

        /** Choose the row's primary key. */
        public Upsert withKey(byte[] key) {
            String id = keyId(key);
            if (!id.equals(primaryId)) {
                rows.remove(primaryId);
                removePostings(primaryId);
                primaryId = id;
            }
            commit();
            return this;
        }

        public Upsert withKey(String key) {
            return withKey(key.getBytes(StandardCharsets.UTF_8));
        }

        /** Add a named-key value (must be declared via {@link #addKey}). */
        public Upsert withNamedKey(String namedKey, byte[] value) {
            if (!namedIndex.containsKey(namedKey)) {
                throw new IllegalArgumentException("undeclared named key: " + namedKey);
            }
            named.put(namedKey, value.clone());
            commit();
            return this;
        }

        public Upsert withNamedKey(String namedKey, String value) {
            return withNamedKey(namedKey, value.getBytes(StandardCharsets.UTF_8));
        }

        private void commit() {
            commitUpsert(record, primaryId, named);
        }
    }

    void commitUpsert(FeedRecord record, String primaryId, Map<String, byte[]> named) {
        removePostings(primaryId);
        named.forEach((key, value) -> {
            var postings = namedIndex.get(key);
            if (postings == null) {
                throw new IllegalArgumentException("undeclared named key: " + key);
            }
            postings.computeIfAbsent(keyId(value), v -> new CopyOnWriteArraySet<>())
                    .add(primaryId);
        });
        rows.put(primaryId, record);
    }

    private Map<String, byte[]> namedValuesOf(String primaryId) {
        var named = new LinkedHashMap<String, byte[]>();
        namedIndex.forEach((key, postings) -> postings.forEach((valueId, ids) -> {
            if (ids.contains(primaryId)) {
                named.put(key, Base64.getDecoder().decode(valueId));
            }
        }));
        return named;
    }

    private void removePostings(String primaryId) {
        namedIndex.values().forEach(postings -> {
            postings.values().forEach(ids -> ids.remove(primaryId));
            postings.entrySet().removeIf(entry -> entry.getValue().isEmpty());
        });
    }

    private List<Entry> snapshot() {
        var entries = new ArrayList<Entry>(rows.size());
        rows.forEach((id, record) ->
                entries.add(new Entry(Base64.getDecoder().decode(id), record)));
        return entries;
    }

    private static String defaultId(FeedRecord record) {
        byte[] key = record.key();
        return keyId(key != null ? key : ("cursor:" + record.cursor().value())
                .getBytes(StandardCharsets.UTF_8));
    }

    private static String keyId(byte[] key) {
        return Base64.getEncoder().encodeToString(key);
    }
}
