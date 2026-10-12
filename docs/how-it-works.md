# How the Java SDK works

Everything a caller can do enters through one class —
`dev.whitewater.WhitewaterClient` — and every HTTP call funnels through
one internal `Transport`. There are no other network paths.

## Layers

```
WhitewaterClient  ─┬─ wcl / commands / healthy          → POST /v1/admin/*
                   ├─ createFeed / describeFeed / …     → typed WCL builders
                   ├─ feeds()   → FeedClient            → /v1/feeds/*, /v1/readers/temporary/fetch
                   ├─ writer(name, epoch)               → /v1/writers/append[-batch]
                   ├─ reader(name)                      → /v1/readers/open|fetch|ack|close
                   └─ subscriptionMember(name)          → /v1/subscriptions/members/*
        │                      │
     Transport            Wire (codec)
   (endpoints,        (request building,
    auth, errors)      response mapping)
```

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
methods are thin builders over it — `client.createFeed("orders.events")`
serializes `CREATE FEED orders.events` and calls `wcl` internally.

Two name validators run before interpolation:

- `dotted()` — domains/feeds must match
  `^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)*$`, ≤512 bytes.
- `resource()` — writers/readers/subscriptions/roles must match
  `^[A-Za-z][A-Za-z0-9_-]{0,127}$`.

A bad name throws `IllegalArgumentException` locally — nothing reaches
the wire, so `createFeed("x; DROP ...")` cannot inject a statement.

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
    CREATE WRITER orders-writer TO shop.orders;
    OPEN WRITER SESSION orders-writer;
    CREATE READER audit FROM shop.orders START AT BEGINNING;
    SHOW FEEDS;
    """);
```

The same calls through the typed builders — each serializes one WCL
statement and calls `wcl` internally:

```java
client.createDomain("shop");                         // CREATE DOMAIN shop;
client.createFeed("shop.orders");                    // CREATE FEED shop.orders;
client.createWriter("orders-writer", "shop.orders");
client.openWriterSession("orders-writer");           // epoch in .data()
client.createReader("audit", "shop.orders", ReaderStart.beginning());
client.listFeeds();                                  // SHOW FEEDS;
client.seekReader("audit", ReaderStart.now());       // SEEK READER ... TO NOW;
client.dropFeed("shop.orders");                      // DROP FEED shop.orders;
```

`client.commands(...)` is the second door — the typed JSON command
path for what WCL does not spell. Declaring a StateStore with named
keys is the current example (there is no `DEFINE STATESTORE` WCL
statement yet):

```java
client.commands(List.of(Map.of(
    "command", "define_state_store",
    "name", "accounts.employees",
    "source", Map.of("kind", "manual"),
    "named_keys", List.of(
        Map.of("name", "passport", "unique", true),
        Map.of("name", "department", "unique", false)))));
```

(`client.stateStores().define(spec, null)` posts exactly this body for
you.) `DROP` is spelled in WCL for every resource including stores:
`client.wcl("DROP STATESTORE accounts.employees")` ≡ `store.drop()`.

## Reading

Three shapes, in increasing durability:

1. **`feeds().records(feed, limit, after)`** — one-shot committed history
   page.
2. **`feeds().temporaryFetch(feed, options)`** — anonymous ephemeral
   reader. `TemporaryFetchOptions.tail(after, limit, waitMs)` is a
   server-side long-poll: the call blocks until records arrive or
   `wait_ms` elapses, returning `records` + `nextCursor`. Continue with
   `nextCursor`, never the last record's cursor — the page may be
   filtered or empty.
3. **`client.reader(name)`** — durable independent cursor.
   `open(capacity)` → `fetch(session, limit)` → `ack(session,
   deliveredCursor)` → `close(session)`. Progress survives restarts.

Two managed wrappers sit on top of those primitives:

- **`feeds().load(feed, start, sink)`** — one-shot bulk read: pages
  `temporaryFetch` from `ReaderStart` until the frontier is drained.
  `sink` is any `Consumer<List<FeedRecord>>`.
- **`client.subscribe(reader, feed, start, onRecords[, onError])`** — a
  durable consume loop on a background thread: provisions the Reader
  when missing, then `open` → [`fetch` → `onRecords` → `ack`] → `close`.
  Transient errors retry; `StaleSession` and hard refusals terminate via
  `onError`/`failure()`. The non-daemon thread keeps the JVM alive until
  `Subscription.close()`.

For cooperating consumers, `client.subscriptionMember(sub)` gives the
lease vocabulary: `join` → `claim`/`renew`/`release` → `fetch` → `ack`.
Members claim work leases; there is no global rebalance.

## Writing

- **Named session**: `openWriterSession(name)` returns a `session_epoch`
  in `results[0].data`; bind `client.writer(name, epoch)` and `append`
  with a stable `requestId` per logical write. The server dedupes
  identical request IDs and fences stale sequences.
- **Ad-hoc**: `feeds().append(feed, new FeedAppend(requestId,
  writerSessionId, key, payload))` — caller supplies ephemeral writer
  identity; same dedup rule.

## Testing

Unit tests stub the wire with a JDK `HttpServer` capturing path + body,
asserting both serialization and response mapping — no live Riverbed
required. `ConformanceRunnerIT` replays the shared
`sdk/spec/conformance` fixtures against a real Riverbed when
`WHITEWATER_ENDPOINTS` and `WHITEWATER_SPEC_DIR` are set; it self-skips
otherwise.
