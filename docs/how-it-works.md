# How the Java SDK works

Everything a caller can do enters through one class —
`dev.whitewater.WhitewaterClient` — and every HTTP call funnels through
one internal `Transport`. There are no other network paths.

## Layers

```
WhitewaterClient  ─┬─ wcl / commands / Commands.of()    → POST /v1/admin/*
                   ├─ createDomain / domain / getDomain → Domain handles
                   ├─ describeDomain / renameDomain / …  → Domain lifecycle
                   └─ demand*/hotKeys/nodeMetrics       → cluster observability

Domain            ─┬─ createFeed / describeFeed / …     → typed WCL builders
                   ├─ feedOps()  → FeedClient           → /v1/feeds/*, /v1/readers/temporary/fetch
                   ├─ writer(name)                      → session-bound Writer
                   ├─ writerSession(name, epoch)        → /v1/writers/append[-batch]
                   ├─ reader(name, maxPageSize)         → session-bound Reader
                   ├─ readerSession(name)               → /v1/readers/open|fetch|ack|close
                   ├─ member(sub)                       → session-bound Member
                   ├─ subscription(name).memberClient() → /v1/subscriptions/members/*
                   ├─ subscription(name).seek/memberState → /v1/subscriptions/*
                   ├─ queryIndex                        → /v1/indexes/query
                   └─ stateStore / defineStateStore     → /v1/state-stores/*
        │                      │
     Transport            Wire (codec)
   (endpoints,        (request building,
    auth, errors)      response mapping)
```

The client owns Domains and cluster-wide plumbing only — every Feed,
Writer, Reader, Subscription, Member, Pipe, StateStore, and Index is
reached through a `Domain` handle, never directly off the client. There
is no `client.feed(...)` or `client.writer(...)`: a name like "orders"
only means something inside a business namespace.

### `Transport`

Holds the endpoint list and API key. `post`/`get` round-robin the
endpoints, attach `authorization: Bearer <key>`, and map every non-2xx
response to a `WhitewaterException` subtype by the wire `code` field:

- `stale_epoch` → `StaleSession`
- `lease.*`, `writer.stale_sequence` → `LeaseConflict` / `Conflict`
- `control.unavailable`, timeouts, unreachable Nodes → `Unavailable`
- 5xx with no code → `AmbiguousResult`

`AmbiguousResult` means the server may or may not have applied the
write — **retry with the same `requestId`**, never a new one.

### `Wire`

The only place request JSON is built and response JSON is decoded.
Record fields translate here: keys/payloads/metadata values are
base64 `byte[]`, `event_time_ns`/`ingest_time_ns` are signed 64-bit
nanoseconds on decimal strings, cursors stay opaque strings.

## Resource management — two doors, one room

`wcl(script)` sends a raw WCL script to `POST /v1/admin/wcl`. The typed
Domain methods are thin builders over it —
`domain.createFeed("orders.events")` serializes
`CREATE FEED <domain>.orders.events` and calls `wcl` internally.

Two name validators run before interpolation:

- `dotted()` — domains/feeds must match
  `^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)*$`, ≤512 bytes.
- `resource()` — writers/readers/subscriptions/roles must match
  `^[A-Za-z][A-Za-z0-9_-]{0,127}$`.

A bad name throws `IllegalArgumentException` locally — nothing reaches
the wire, so `domain.createFeed("x; DROP ...")` cannot inject a statement.

`ReaderStart` supplies `START AT` positions: `beginning()`, `now()`,
`cursor(c)` (single-quotes escaped `'` → `''`), `timestampNs(ns)`.

Every management call returns `ControlExecution` — `requestId`,
`revision`, `authority`, `warning`, and one `StatementResult` per WCL
statement (`statement`, `message`, `data`).

### WCL by example

Raw script — several statements commit as one request (pass a shared
`requestId` to dedupe a retried batch):

```java
client.wcl("""
    CREATE DOMAIN shop;
    CREATE FEED shop.orders;
    CREATE WRITER orders.writer TO shop.orders;
    OPEN WRITER SESSION orders.writer;
    CREATE READER audit FROM shop.orders START AT BEGINNING;
    SHOW FEEDS;
    """);
```

The same calls through the typed helpers — each serializes one WCL
statement and calls `wcl` internally. The Domain-typed surface composes
names for you: hand it the relative name and it prefixes the Domain:

```java
var shop = client.createDomain("shop");              // CREATE DOMAIN shop; + handle
// or client.getDomain("shop")                       // verifies + materializes

shop.createFeed("orders");                           // CREATE FEED shop.orders;
shop.createWriter("writer", "orders");               // shop.writer on shop.orders
shop.openWriterSession("writer");                    // epoch in .data()
shop.createReader("audit", "orders", ReaderStart.beginning());
shop.feeds();                                        // [shop.orders, …]
shop.seekReader("audit", ReaderStart.now());         // SEEK READER shop.audit TO NOW;
shop.dropFeed("orders");                             // DROP FEED shop.orders;
```

Every name handed to a Domain method resolves inside that Domain —
`shop.createReader("audit", …)` declares `shop.audit` bound to
`shop.orders`; a name already prefixed (`"shop.orders"`) passes through
verbatim, so handles compose freely.

`client.commands(...)` is the second door — the typed JSON command
path. Two forms:

```java
// Raw maps — escape hatch for commands the fluent builder has not grown:
client.commands(List.of(Map.of("command", "show", "kind", "feeds")));

// Fluent, validated — the form application code should use:
client.commands(Commands.of()
        .createDomain("accounts")
        .defineStateStore("accounts.employees", null, List.of(
                new NamedKey("passport", true),
                new NamedKey("department", false)))
        .grant(Set.of(Permission.READ, Permission.WRITE),
                "accounts.employees", "hr.app"));
```

`dev.whitewater.admin.Commands` covers the public-safe command
vocabulary — resources, grants, Indexes, StateStores, Pipes, storage
Nodes. Internal replica-authority commands (range cutover, owner
movement, commit proofs) are absent by design: the public endpoint
refuses them and they are not application surface. Everything validates
locally, so a malformed command fails before it reaches the Control
Plane.

(`domain.defineStateStore(spec, null)` is sugar over
`define_state_store`.) `DROP` is spelled in WCL for every resource
including stores: `client.wcl("DROP STATESTORE accounts.employees")` ≡
`store.drop()` ≡ `Commands.of().drop(ResourceKind.STATE_STORE, ...)`.

## Domains — strongly-typed navigation

A Domain owns every Feed it prefixes (`shop` owns `shop.orders`,
including deeper names like `shop.orders.eu`). `client.domain(name)`
returns a lazy handle; `client.getDomain(name)` verifies the Domain
exists and materializes its catalog view immediately.

The first catalog accessor issues **one batched control read** —
`describe domain` plus every `show` listing in a single
`POST /v1/admin/commands` — joins rows by `space_id` (readers/writers
join through their Feed's `feed_id`), and caches the snapshot:

```java
Domain shop = client.getDomain("shop");
shop.domainId();            // immutable id — survives renames
shop.describe();            // raw DESCRIBE DOMAIN row (status, history_policy)
shop.feeds();               // List<Feed> — every Feed in the Domain
shop.getFeeds("order");     // prefix filter on the relative name
shop.readers();             // Readers on this Domain's Feeds
shop.writers();             shop.subscriptions();
shop.pipes();               shop.indexes();   shop.stateStores();
shop.refresh();             // re-pull the snapshot
```

`domain.feed("orders")` / `domain.getFeed("orders")` resolve
`shop.orders` — pure name composition, zero I/O, codecs bindable through
the `Class`/`Codec` overloads. Deeper relative names work the same way:
`shop.feed("orders.hourly")` binds `shop.orders.hourly`. The same
composition applies to the other resources — `shop.pipe("orders.relay")`
binds the Pipe `shop.orders.relay`, `shop.stateStore("customers")` the
StateStore `shop.customers`, and `shop.resolve("orders.hourly")` →
`shop.orders.hourly` composes the wire name anywhere a full name is
asked for (`Commands.of()`, `Scoped.absolute(...)`, the store
`Class`/`Codec` overloads). A relative name is always interpreted inside
the Domain; a name that fails the dotted grammar is refused locally.

Pipe endpoints can *cross* Domains deliberately: a `ScopedRef` — a
`Feed`/`StateStore`/`SubscriptionRef` handle from another Domain, or
`Scoped.absolute("other.name")` — carries its full dotted name while
plain `String`/`Scoped.relative` endpoints resolve in the declaring
Domain. See [pipes.md](pipes.md) and
[examples/CrossDomain.java](../examples/CrossDomain.java).

### Fluent Readers and Writers

`domain.createReader(feed)` and `domain.createWriter(feed)` return
builders that declare the resource (idempotently) and open a session
with the Feed's codecs bound. `readerId`/`writerId` resolve inside the
Domain too — `"fulfillment"` declares `shop.fulfillment`, not a global
name:

```java
import static dev.whitewater.model.ReaderStart.*;   // Beginning, End

try (var reader = shop.createReader("orders")
        .readerId("fulfillment")         // shop.fulfillment — Kafka groupId
        .readerStartPoint(Beginning)     // first-time start — auto.offset.reset
        .maxPageSize(256)
        .open()) {                        // FeedReader<String, String>
    reader.forEach(m -> handle(m));      // per-record pump: deliver → ack each page
    // or stay manual: fetch() → handle → ack()
}
```

- `readerId` is required — it *is* the progress identity; reopening the
  same name resumes the acknowledged frontier.
- `readerStartPoint` applies only at first declaration: `Beginning`
  (oldest retained), `End` (new records only), `cursor(c)`, or
  `timestamp(Instant)` — the same mapping Kafka's
  `auto.offset.reset=earliest|latest` gives a fresh group.
- `reader.listen(handler)` pumps on an SDK-owned daemon thread and
  returns a `Pump` handle (`close()` stops the thread + session;
  `failure()` surfaces a handler error) — no user-side `Thread` or
  executor. `reader.forEach(handler)` is the blocking form;
  `forEachWhile` stops on a `false` return leaving the in-flight page
  unacked so its tail replays. The loop paces empty fetches on the
  server's `retry_after_ms`.
- `maxPageSize` bounds each `fetch` page (default 128).
- `.declare()` creates the catalog row without opening a session —
  useful for provisioning ahead of consumers.
- The same shape exists for producers:
  `shop.createWriter("orders").writerId("orders.ingest").open()`
  returns a `FeedWriter<K,V>`.

`domain.feed(...)` handles are codec-compatible `Feed<K,V>` facades —
typed reads come back as `FeedMessage<K,V>` with decoded keys/values.
`feed(name)` defaults to `Feed<String, String>` — UTF-8 both ways, so
`writer.append("order-1", "{\"total\":42}")` passes a String and
`m.value()` decodes one (the SDK owns the byte conversion; plain JSON
needs no schema binding). `feed(name, byte[].class)` opts into raw
binary; any other `Class`/`Codec` binds schema or custom serialization.
`domain.stateStore(name)` defaults to `StateStore<String, String>` the
same way.

## Reading

Three shapes, in increasing durability:

1. **`domain.feedOps().records(feed, limit, after)`** — one-shot
   committed history page.
2. **`domain.feedOps().temporaryFetch(feed, options)`** — anonymous ephemeral
   reader. `TemporaryFetchOptions.tail(after, limit, waitMs)` is a
   server-side long-poll: the call blocks until records arrive or
   `wait_ms` elapses, returning `records` + `nextCursor`. Continue with
   `nextCursor`, never the last record's cursor — the page may be
   filtered or empty.
3. **`domain.reader(name, maxPageSize)`** — durable independent cursor,
   session-bound: opens internally, `fetch(limit)` tracks the delivered
   Cursor so `ack()` commits the right frontier, `close()` releases the
   fence. (The explicit-epoch form `domain.readerSession(name)` +
   `open`/`fetch`/`ack`/`close` remains for callers managing session
   state themselves.) Progress survives restarts.

Two managed wrappers sit on top of those primitives:

- **`domain.feedOps().load(feed, start, sink)`** — one-shot bulk read:
  pages `temporaryFetch` from `ReaderStart` until the frontier is
  drained. `sink` is any `Consumer<List<FeedRecord>>`.
- **`domain.subscribe(reader, feed, start, onRecords[, onError])`** — a
  durable consume loop on a background thread: provisions the Reader
  when missing, then `open` → [`fetch` → `onRecords` → `ack`] → `close`.
  Transient errors retry; `StaleSession` and hard refusals terminate via
  `onError`/`failure()`. The non-daemon thread keeps the JVM alive until
  `Subscription.close()`.

For cooperating consumers, `domain.member(sub)` joins internally and
owns the member identity, fencing epoch, and lease plumbing — the worker
loop reads `claim() → fetch → ack → release`, with `transfer(work, peer)`
for explicit handoff. The raw `domain.memberClient(sub)` keeps
explicit epoch plumbing for callers managing identities themselves.
Members claim work leases; there is no global rebalance.

## Writing

- **Session-bound**: `domain.writer(name)` opens `OPEN WRITER SESSION`
  internally and owns the fencing epoch — `append(key, payload)` auto
  request identities, `reopen()` rebinds after `StaleSession`.
- **Explicit epoch**: `domain.writerSession(name, epoch)` binds a session
  opened elsewhere; `append` with a stable `requestId` per logical write.
  The server dedupes identical request IDs and fences stale sequences.
- **Ad-hoc**: `domain.feedOps().append(feed, new FeedAppend(requestId,
  writerSessionId, key, payload))` — caller supplies ephemeral writer
  identity; same dedup rule.

## Testing

Unit tests stub the wire with a JDK `HttpServer` capturing path + body,
asserting both serialization and response mapping — no live Riverbed
required. `ConformanceRunnerIT` replays the shared
`sdk/spec/conformance` fixtures against a real Riverbed when
`WHITEWATER_ENDPOINTS` and `WHITEWATER_SPEC_DIR` are set; it self-skips
otherwise.
