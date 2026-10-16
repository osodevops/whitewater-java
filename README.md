# whitewater-java

Java SDK for Whitewater Riverbeds. Implements the versioned
`whitewater-protocol` wire contract defined in the main repository under
`sdk/spec/` (`PROTOCOL.md`, `endpoints.json`, `conformance/`).

Requires Java 17+. The only runtime dependency is Jackson Databind; HTTP
uses `java.net.http` — no framework required.

## Clients

```java
WhitewaterClient client = WhitewaterClient.builder()
        .apiKey(System.getenv("WHITEWATER_API_KEY"))
        .endpoints(List.of(
                URI.create("https://node1:7070"),
                URI.create("https://node2:7070"),
                URI.create("https://node3:7070")))
        .build();
```

| Client | Covers |
|---|---|
| `client` (direct) | WCL (`POST /v1/admin/wcl`), typed commands (`POST /v1/admin/commands`), cluster health — plus typed resource methods (`createDomain`, `createFeed`, `createWriter`, `createReader`, `createSubscription`, `describeFeed`, `renameFeed`, `dropFeed`, `listFeeds`, `seekReader`, `openWriterSession`, …) that build the same WCL for you |
| `client.commands(Commands.of()…)` | The typed-command vocabulary as a validated fluent builder — Domains, Feeds, Writers, Readers, Subscriptions (feed or Domain source, delivery delay, retry budget), Roles, grants/revocations, API keys, Indexes (`define_index`/`rebuild`/`get_index`), StateStores, Pipes (forward/enrich/window, pause/resume), storage-Node registration/drain/retire/capacity |
| `ReaderStart` | Reader/Subscription start positions: `beginning()`, `now()`, `cursor(c)`, `timestampNs(ns)` |
| `client.writer(name)` | **Session-bound Writer**: opens `OPEN WRITER SESSION` internally; `append(key, payload)` auto request IDs + epoch, `reopen()` after `StaleSession` |
| `client.reader(name, capacity)` | **Session-bound Reader**: opens internally; `fetch(limit)` tracks the delivered Cursor so `ack()` commits the right frontier; `close()` releases the fence |
| `client.member(sub)` | **Session-bound member**: joins internally; `claim() → fetch → ack → release → transfer(work, peer)` with no epoch threading |
| `client.subscriptionMember(sub)` | Raw member client: explicit `memberId`/`memberEpoch`/`leaseEpoch` plumbing (`join`, `claim`, `renew`, `release`, `transfer`, `fetch`, `ack`, `state()`) |
| `client.seekSubscription(name, position, ts, cursor)` | Operator reposition of a Subscription's shared frontier — refused while a member work lease is active |
| `client.subscriptionMemberState(name)` | Authoritative member state: ownership epoch, per-member epochs, live leases |
| `client.queryIndex(name, predicate)` | `exact` / `prefix` / `range` reads against an activated Index generation, with applied-vs-target freshness evidence |
| `client.feed(name[, type/codec])` | **Typed feed facade**: binds `Codec`s once so keys/payloads serialize automatically in both directions — `feed.writer(name)` (declares + opens a session), `feed.reader(name, capacity)` (decoded `fetch`/`ack`), `feed.fetch`/`records`/`load` (decoded ad-hoc reads), `feed.subscribe` (decoded batches). Same codec vocabulary as `stateStore`: `String`/`byte[]`/`JsonNode`/Jackson classes out of the box, protobuf/Avro/custom via `dev.whitewater.codec`/`Codecs.of` |
| `client.feeds()` | Raw-byte ad-hoc Feed operations: committed record reads, anonymous temporary fetches (`tail`/`wait_ms`/`new_only`), ad-hoc appends, `load(feed, start, sink)` one-shot history reads — the escape hatch under `client.feed(...)` |
| `client.subscribe(reader, feed, start, onRecords)` | Managed durable consume: provisions the Reader, runs open → fetch → deliver → ack on a background thread; `Subscription.close()` to stop |
| `client.stateStore(name[, type/codec])` / `client.stateStores()` | Typed server-side StateStore facade — durable RF3 rows on the Nodes, serialized by class (String/JSON) or bound `Codec`s (String keys + `byte[]` payloads by default): `put`/`get`/`getBy`/`iterate`/`delete`/`clear`/`drop`/`sync()`/`rebuild()`. Optional protobuf/Avro codecs in `dev.whitewater.codec` |
| `client.health()` / `client.checkCompatible()` | Liveness plus the protocol block (`major`/`minor`) and advertised transport limits; `checkCompatible()` refuses a major-version mismatch |

```java
var spec = StateStoreSpec.named("accounts.employees")
        .addKey("passport", true).addKey("department", false);          // up to 10 named keys
var store = client.stateStores().define(spec, null);                    // durable store on the Nodes

store.put("emp-1", payload, Map.of("passport", "P123", "department", "eng"));
var row = store.get("emp-1");                          // StoreRow | null + freshness
var rows = store.getBy("passport", "P123");            // named key — a list
store.iterate(r -> export(r));
```

`client.stateStore(name)` binds String keys + `byte[]` payloads by
default; `client.stateStore(name, Codecs.json(Employee.class))` puts and
reads back your own objects — no `getBytes()` in application code.
`client.feed(name, Employee.class)` binds Feeds the same way: writers
serialize, readers/fetches/subscriptions deserialize, and the codec
vocabulary (UTF-8, raw bytes, JSON classes, protobuf, Avro, custom) is
shared across feeds and stores:

```java
Feed<String, Order> orders = client.feed("demo.orders", Order.class);
orders.writer("ingest").append("order-1", new Order("order-1", 42));
try (var reader = orders.reader("audit", 64)) {
    Order o = reader.fetch().records().get(0).value();   // decoded
}
StateStore<String, Order> latest = client.stateStore("demo.orders_latest", Order.class);
latest.put("order-1", order);                            // same codec seam
```

The store lives on the Nodes — the SDK holds no local copy. See
[docs/stores.md](docs/stores.md) for the full store guide.

Errors surface as `WhitewaterException` subtypes mapped from wire `code`
values (`stale_epoch`, `lease.busy`, `lease.stale_lease`,
`writer.stale_sequence`, `control.unavailable`, ...). Retryable transient
refusals and genuinely ambiguous results (status + timeout) are distinct
subtypes — never lumped together.

Cursors are opaque: the SDK never parses, orders, or constructs them.
`event_time_ns`/`ingest_time_ns` are signed 64-bit nanoseconds and travel
as decimal strings on the wire.

## Build

```powershell
mvn verify          # compile + unit tests (+ conformance, if configured)
```

No local Maven? Docker works:

```powershell
docker run --rm -v "${PWD}:/work" -w /work maven:3.9-eclipse-temurin-21 mvn verify
```

## Conformance

`ConformanceRunnerIT` executes the shared fixture corpus against a real
three-Node Riverbed. Point it at endpoints and the spec checkout:

```powershell
$env:WHITEWATER_ENDPOINTS = "http://localhost:7071,http://localhost:7072,http://localhost:7073"
$env:WHITEWATER_API_KEY   = "whitewater-local-development-admin-key"
$env:WHITEWATER_SPEC_DIR  = "C:\Code\Stackapps\whitewater\sdk\spec\conformance"
mvn verify
```

A scenario whose `op` the SDK cannot express fails explicitly with
`UnsupportedOperationException` rather than silently skipping — that is a
spec-required signal, not a bug.
