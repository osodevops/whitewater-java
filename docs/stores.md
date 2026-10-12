# StateStore — coding guide

There is exactly one store: the **node-side StateStore** — durable,
RF3-replicated, Fjall-backed, and queryable from any Node. The SDK's
`StateStore` is a thin facade over it: it holds no rows, keeps no local
projection, and every operation is an HTTP call with freshness evidence.

```java
var spec = StateStore.named("accounts.employees")
        .addKey("passport", true)            // unique: one row per value
        .addKey("department", false);        // non-unique: value -> many rows

var store = client.stateStores().define(spec, null);   // manual store
// or a bound facade for an already-declared store:
var same = client.stateStore("accounts.employees");
```

`addKey` records declaration intent used by `define` — named keys are
declared at creation; at most `StateStore.MAX_NAMED_KEYS` (10). Adding a
key to an existing store has no effect server-side.

## Writing

```java
store.put("user-1".getBytes(), "profile-1".getBytes(), Map.of(
        "passport", "P111",
        "department", "eng",
        "salary", 92000L,
        "active", true,
        "badge", new byte[]{1, 2},
        "note", null), null);               // null mutationId -> generated
store.delete("user-1".getBytes());          // mutationId auto-generated
store.delete("user-1".getBytes(), stableMutationUuid);
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
  conflicts.

## Reading

```java
var row = store.get("user-1".getBytes(), false, null);      // StoreRow | null
row.payload();                                             // byte[]
row.freshness();            // appliedSeq / committedSeq / behind / servedBy

var result = store.getBy("department", "eng", null, false, null);
result.rows();                            // List<StoreRow> — every match
result.freshness().servedBy();            // which Node answered
```

`getBy` is the group/list lookup: one non-unique named value resolves
to **every** primary row carrying it. `strict` + `waitMs` bounded-waits
for the serving replica to apply its committed frontier rather than
answering stale; non-strict reads still report `freshness().behind()`
honestly.

## Iterating

```java
store.iterate(256, row -> export(row));

var page = store.page(afterKey, 256);       // one bounded page
page.nextKey();                             // continuation, or null
page.pinnedSeq();                           // journal position pinned to
```

`iterate`/`page` ride `/v1/state-stores/iterate` — the same bounded,
key-ordered row set replica catch-up serves — so `pinnedSeq` is the
iteration's freshness evidence.

## Lifecycle

```java
store.clear(null);                          // one journaled ClearAll
store.drop();                               // or client.stateStores().drop(name)
```

- `clear` empties the store as one durable, idempotent mutation;
  replicas sweep rows and postings in bounded batches.
- `drop` tombstones the definition — the name cannot be recreated and
  every data path refuses. Rows already applied on replicas remain
  until a bounded reclaim pass.
- Feed-sourced stores: `client.stateStores().define(spec, "accounts.events")`
  materializes committed Feed history; named keys require a manual
  store until declared projections exist.

## What a StateStore facade is **not**

- **Not a local store** — there is no in-process copy to drift from
  the Nodes. Every read goes to a replica; freshness evidence tells
  you how current it was.
- **Not unique on non-unique keys** — `getBy` always returns a list.

## Errors

| Where | Type | Meaning / remedy |
|---|---|---|
| `addKey` #11 / duplicate / blank | `IllegalStateException` / `IllegalArgumentException` | Declaration contract (10-key bound, distinct names). |
| `define` spec + `source` + named keys | `IllegalArgumentException` | Named keys need a manual store — checked before the call. |
| Data op on unbound spec | `IllegalStateException` | `StateStore.named` builds a spec; bind via `define` or `client.stateStore(name)`. |
| `Float`/`Double` named value | `IllegalArgumentException` | The wire carries exact i64 — pass an integral type. |
| Undeclared named key / missing store | `WhitewaterException` 400 | Server-side contract — fix the declaration. |
| `mutationId` reused differently | `WhitewaterException` 409 | Retry with the original contents or a fresh ID. |
| No quorum / replica down | `WhitewaterException` 503 | Retry — the journal needs a quorum to commit. |
| `strict` read can't catch up | `WhitewaterException` 409 `index_behind` | Bounded wait expired; retry or drop `strict`. |
