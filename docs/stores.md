# RecordStore and loadStore — coding guide

A `RecordStore` is a named, thread-safe, in-memory projection of Feed
records. You fill it once with `loadStore` (bulk history) and keep it
warm with `subscribe` (live tail) — the Whitewater equivalent of a local
materialized view.

## Store anatomy

Two structures per store:

- **Primary map** — `primary key -> FeedRecord`. One row per key; a new
  row for an existing key replaces it.
- **Named-key posting maps** — one per declared named key, conceptually
  `employee-store.passport`: `named value -> set of primary keys`.
  Named keys are **not unique** — many rows may share one named value,
  so lookups return a list.

```java
var store = RecordStore.named("employee-store")
        .addKey("passport")          // declare before use
        .addKey("department");       // at most RecordStore.MAX_NAMED_KEYS (10)
```

`addKey` returns the store for chaining. Adding an eleventh named key
throws `IllegalStateException`; blank names throw
`IllegalArgumentException`.

## Loading history: `client.loadStore(store).fromFeed(...)`

```java
long loaded = client.loadStore(store).fromFeed(
        "hr.employees",
        ReaderStart.beginning(),            // or .now(), .cursor(c), .timestampNs(ns)
        (s, record) -> s.upsert(record)
                .withKey(record.key())                          // primary key
                .withNamedKey("passport", record.metadata()
                        .getOrDefault("pp", new byte[0]))
                .withNamedKey("department", "eng".getBytes())); // String overloads too
```

Semantics, in order:

1. Pages `temporaryFetch` from `start` until the frontier is drained —
   this is a **one-shot bounded read**, not a tail.
2. For every record, the projection callback runs `store.upsert(record)`.
3. `withKey(bytes)` chooses the primary key. Defaults to `record.key()`;
   call it with anything — e.g. a composite `"dept:emp-1"` — and the row
   re-keys (old postings are stripped first). `withKey(String)` is UTF-8.
4. `withNamedKey(name, bytes)` attaches one named-key value; call it
   once per declared key you want indexed. Passing an **undeclared**
   named key throws `IllegalArgumentException` immediately — a projection
   bug fails the load, never silently writes an unindexed row.
5. Returns the count of records read (not rows written — a re-key or a
   second append under the same key replaces, not duplicates).

Skip `withNamedKey` entirely and the row lands in the primary map only —
still fully readable, just not indexable by that named key.

The lighter `feeds().load(feed, start, sink)` remains for "dump history
into a store or callback" with no projection; `sink` may be the store
itself (rows key on `record.key`, cursor fallback for unkeyed records)
or any `Consumer<List<FeedRecord>>`.

## Keeping the store warm: `subscribe`

```java
try (var sub = client.subscribe("hr-view", "hr.employees",
        ReaderStart.now(),                          // only new records
        records -> {                                // invoked per fetched page
            for (var r : records) {
                store.upsert(r)
                        .withKey(r.key())
                        .withNamedKey("department", r.metadata()
                                .getOrDefault("department", new byte[0]));
            }
        },
        error -> System.err.println("subscription ended: " + error))) {
    // The loop runs on a non-daemon background thread — the JVM stays up.
    mainWork();
}                                                   // close() stops the loop
```

Delivery is **at-least-once**: records land in the callback, then the
loop acks `deliveredCursor`. A crash between callback and ack replays —
make the projection idempotent (upsert semantics already are).

## Reading the store

```java
store.get("emp-1");                       // Optional<FeedRecord> by primary key
store.get(empBytes);                      // byte[] overload
store.get("passport", "P123");            // List<FeedRecord> — all rows matching
store.get("passport", ppBytes);           // byte[] overload
store.get("undeclared", "x");             // throws IllegalArgumentException
store.size();                             // row count
store.namedKeys();                        // declared keys, in order
store.name();                             // "employee-store"
store.forEach(e -> export(e.record()));   // stable snapshot iteration
for (var e : store) { /* e.primaryKey(), e.record() */ }
```

## Mutating outside a load

```java
store.put(record);                        // primary-key upsert, no named keys
store.putAll(page.records());             // subscribe-ready sink shape
store.upsert(record).withKey("custom");   // same builder as projections
store.delete("emp-1");                    // removes row + all its postings
store.clear();                            // empties everything
```

`delete` and `clear` strip named-key postings too — a deleted row never
lingers in a `get(namedKey, value)` result.

## Copying and parallel work

```java
var replica = RecordStore.named("employee-backup");
store.copy(replica);                      // rows + named postings; declares
                                          // missing keys on the target
store.forEachParallel(3, entry -> rebuild(entry));
```

`forEachParallel` fans a stable snapshot across a fixed thread pool —
chunked, all tasks joined before returning. `threads <= 0` throws
`IllegalArgumentException`; a worker's exception surfaces as
`RuntimeException` wrapping the cause. Use it for CPU or blocking work —
not for another `put` into the same store (writes are already
thread-safe, but you don't need the pool).

## Error handling reference

| Where | Type | Meaning / remedy |
|---|---|---|
| `addKey` #11 | `IllegalStateException` | Store key budget (10) exceeded — declare fewer keys. |
| `addKey`/`get`/`withNamedKey` undeclared or blank | `IllegalArgumentException` | Programming error — declare the key or fix the name. |
| `loadStore` mid-read | `WhitewaterException` (usually `Unavailable`) | Transport refused a page; retry the whole `fromFeed` call — re-upserts are idempotent. |
| `subscribe` transient | retried internally | `Unavailable`/`AmbiguousResult` backoff inside the loop. |
| `subscribe` terminal | `onError` + `subscription.failure()` | `StaleSession` (another client opened your Reader — pick a unique name or take over deliberately), `Refused`, `Conflict`. The loop is dead; inspect, fix, re-subscribe. |

All `WhitewaterException` subtypes carry `status()` and `code()`.

## What a RecordStore is **not**

- **Not durable** — it dies with the process. Rebuild is cheap:
  `loadStore` from `ReaderStart.beginning()` (or a snapshot Cursor).
- **Not a server StateStore** — the server-side replicated store model
  is a separate, later seam. This is the client-side projection.
- **Not unique on named keys** — `get(namedKey, value)` always returns a
  list, even when it happens to hold one row.
