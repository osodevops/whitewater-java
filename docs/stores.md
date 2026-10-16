# StateStore — coding guide

There is exactly one store: the **node-side StateStore** — durable,
RF3-replicated, Fjall-backed, and queryable from any Node. The SDK's
`StateStore<K, V>` is a typed facade over it: it holds no rows, keeps no
local projection, serializes keys and payloads through bound `Codec`s,
and every operation is an HTTP call with freshness evidence.

## Declaring

```java
var spec = StateStoreSpec.named("accounts.employees")
        .addKey("passport", true)            // unique: one row per value
        .addKey("department", false);        // non-unique: value -> many rows

client.stateStores().define(spec, null);     // manual store
```

`addKey` records declaration intent used by `define` — named keys are
declared at creation; at most `StateStoreSpec.MAX_NAMED_KEYS` (10).
Adding a key to an existing store has no effect server-side.

## Binding a typed facade

The store carries raw bytes on the wire; `Codec`s decide what your
application sees. `client.stateStore(name)` defaults to String keys and
`byte[]` payloads — pass Codecs for anything richer:

```java
// String keys, raw byte[] payloads (the default):
StateStore<String, byte[]> raw = client.stateStore("accounts.employees");

// String keys, UTF-8 text payloads:
StateStore<String, String> text = client.stateStore("accounts.employees", Codecs.utf8());

// String keys, Jackson-mapped object payloads — objects in, objects out:
StateStore<String, Employee> store =
        client.stateStore("accounts.employees", Codecs.json(Employee.class));

// Fully custom: key type too (object keys, protobuf, ...):
StateStore<UserKey, Employee> keyed =
        client.stateStore("accounts.employees", Codecs.json(UserKey.class), Codecs.json(Employee.class));
```

`Codecs` ships `utf8()`, `bytes()`, `json(Class)`/`json(TypeReference)`,
`jsonTree()`, and `of(encoder, decoder)` for anything else.
`store.view(otherCodecs)` rebinds the same store — no wire call.

## Writing

```java
store.put("emp-1", new Employee("alice", "eng", "P111"), Map.of(
        "passport", "P111",
        "department", "eng",
        "salary", 92000L,
        "active", true,
        "badge", new byte[]{1, 2},
        "note", null));
store.put("emp-1", employee, null, stableMutationUuid);  // caller-owned idempotency
store.delete("emp-1");                                    // mutationId auto-generated
store.delete("emp-1", stableMutationUuid);
```

- `put` commits one journaled `put_row` through the store's RF3 quorum
  and returns a `MutationResult(storeId, mutationId, sequence, deduplicated)`.
- Named values are **typed**: `String` -> text, `Byte`/`Short`/`Integer`/
  `Long`/`BigInteger` -> i64, `Boolean` -> bool, `byte[]` -> bytes,
  `null` -> null. A `List` value carries one typed value per posting
  component. `Float`/`Double` are refused — the wire carries exact
  i64/text/bytes.
- Named values must match the store's declared keys — the server
  refuses undeclared names. `unique` claims arbitrate
  first-committed-wins by journal order.
- `mutationId` is the idempotency identity: retry a timed-out `put` with
  the same ID and it dedupes; reuse it with different contents and it
  conflicts. The two- and three-arg `put`/`delete`/`clear` overloads
  generate one — use the explicit form only when you own retry logic.

## Reading

```java
var row = store.get("emp-1");                       // StoreRow<String, Employee> | null
Employee alice = row.payload();                      // decoded — never getBytes
String id = row.key();                               // decoded too
row.freshness();                                     // appliedSeq / committedSeq / behind / servedBy

var fresh = store.getStrict("emp-1");               // bounded-waits for the committed frontier
var bounded = store.getStrict("emp-1", Duration.ofSeconds(5));

var result = store.getBy("department", "eng");
result.rows();                                       // every match
result.freshness().servedBy();                       // which Node answered
```

`getBy` is the group/list lookup: one non-unique named value resolves
to **every** primary row carrying it. `get` is the fast path — it reads
the serving replica immediately and reports `freshness().behind()`
honestly; `getStrict`/`getByStrict` bounded-wait for the replica to
apply its committed frontier rather than answering stale. The full
wire shape (`get(key, strict, waitMs)`, `getBy(key, value, lower,
upper, limit, strict, waitMs)`) stays available when you need bounds.

## Iterating

```java
store.iterate(row -> export(row));                  // default page bound
store.iterate(256, row -> export(row));

var page = store.page(afterKey, 256);               // one bounded page
page.nextKey();                                      // decoded continuation, or null
page.pinnedSeq();                                    // journal position pinned to
```

`iterate`/`page` ride `/v1/state-stores/iterate` — the same bounded,
key-ordered row set replica catch-up serves — so `pinnedSeq` is the
iteration's freshness evidence. `nextKey` arrives decoded through the
key codec; feed it back into `page(...)`.

## Lifecycle

```java
store.clear();                            // one journaled ClearAll
store.sync();                             // resume feed-derived replay
store.rebuild();                          // re-materialize from retained history
store.drop();                             // or client.stateStores().drop(name)
```

- `clear` empties the store as one durable, idempotent mutation;
  replicas sweep rows and postings in bounded batches.
- `drop` tombstones the definition — the name cannot be recreated and
  every data path refuses. Rows already applied on replicas remain
  until a bounded reclaim pass.
- Feed-sourced stores: `client.stateStores().define(spec, "accounts.events")`
  materializes committed Feed history.

### Feed-sourced named keys

Named keys on a feed-sourced store declare a projection — how each
committed source record maps to typed lookup values. Every key carries
an `Extractor`:

```java
var spec = StateStoreSpec.named("accounts.tagged")
        .addKey("passport", true, Extractor.metadata("pp"))
        .addKey("department", false, Extractor.json("dept"));
client.stateStores().define(spec, "accounts.events");
```

`Extractor.recordKey()` reads the record's key,
`Extractor.metadata("pp")` a Metadata entry, `Extractor.json("dept")` a
dotted JSON-path over the payload. Missing fields and unparseable
payloads project a `Null` posting rather than stalling replay. Manual
stores omit the extractor — `put` carries the values per row.

### WCL and typed commands

Everything above is a convenience over the Control Plane's two doors.
WCL spells `DEFINE`, `DROP`, `DESCRIBE`, and `SHOW` for stores (data ops
stay typed):

```java
client.wcl("DEFINE STATESTORE accounts.employees KEYS department, passport UNIQUE");
client.wcl("DEFINE STATESTORE accounts.tagged FROM FEED accounts.events KEYS passport = METADATA 'pp' UNIQUE, department = JSON 'dept'");
client.wcl("SHOW STATESTORES");
client.wcl("DESCRIBE STATESTORE accounts.employees"); // stage incl. "dropped"
client.wcl("DROP STATESTORE accounts.employees");
```

`commands` posts the typed command the facade builds for you:

```java
client.commands(Commands.of().defineStateStore(
        "accounts.employees", null,
        List.of(new NamedKey("passport", true), new NamedKey("department", false))));
```

## What a StateStore facade is **not**

- **Not a local store** — there is no in-process copy to drift from
  the Nodes. Every read goes to a replica; freshness evidence tells
  you how current it was.
- **Not unique on non-unique keys** — `getBy` always returns a list.
- **Not a serializer of last resort** — Codecs are explicit per binding;
  the SDK never guesses a payload type from the bytes.

## Errors

| Where | Type | Meaning / remedy |
|---|---|---|
| `addKey` #11 / duplicate / blank | `IllegalStateException` / `IllegalArgumentException` | Declaration contract (10-key bound, distinct names). |
| `define` spec + `source` + bare named keys | `IllegalArgumentException` | Feed-sourced keys need an `Extractor` — checked before the call. |
| `define` manual spec + `Extractor` | `IllegalArgumentException` | Extractors apply to feed-sourced stores only. |
| Codec decode of a foreign payload | `IllegalArgumentException` | The stored bytes aren't your type — bind the matching `Codec` or `bytes()`. |
| `Float`/`Double` named value | `IllegalArgumentException` | The wire carries exact i64 — pass an integral type. |
| Undeclared named key / missing store | `WhitewaterException` 400 | Server-side contract — fix the declaration. |
| `mutationId` reused differently | `WhitewaterException` 409 | Retry with the original contents or a fresh ID. |
| No quorum / replica down | `WhitewaterException` 503 | Retry — the journal needs a quorum to commit. |
| `strict` read can't catch up | `WhitewaterException` 409 `index_behind` | Bounded wait expired; retry or drop `strict`. |
