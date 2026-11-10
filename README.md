# whitewater-java

Java SDK for Whitewater Riverbeds. Implements the versioned
`whitewater-protocol` wire contract defined in the main repository under
`sdk/spec/` (`PROTOCOL.md`, `endpoints.json`, `conformance/`).

Requires Java 17+. The only runtime dependency is Jackson Databind; HTTP
uses `java.net.http` — no framework required.

A complete microservice walkthrough — provision, produce, pump-process, dead-letter, shutdown — lives in [`docs/demo-service.md`](docs/demo-service.md).

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
| `client` (direct) | WCL (`POST /v1/admin/wcl`), typed commands (`POST /v1/admin/commands`), cluster health, demand reports — plus Domain-level administration: `createDomain`, `domain`/`getDomain`, `describeDomain`, `renameDomain`, `dropDomain`, `listDomains`, domain schema/role lifecycle. Everything else — Feeds, Writers, Readers, Subscriptions, Members, Pipes, Stores, Indexes — is reached **only** through a `Domain` handle |
| `client.commands(Commands.of()…)` | The typed-command vocabulary as a validated fluent builder — Domains, Feeds, Writers, Readers, Subscriptions (feed or Domain source, delivery delay, retry budget), Roles, grants/revocations, API keys, Indexes (`define_index`/`rebuild`/`get_index`), StateStores, Pipes (forward/enrich/window, pause/resume), storage-Node registration/drain/retire/capacity |
| `ReaderStart` | Reader/Subscription start positions: `Beginning`, `End`, `beginning()`, `now()`, `cursor(c)`, `timestamp(instant)`, `timestampNs(ns)` — `import static dev.whitewater.model.ReaderStart.*` makes `Beginning`/`End` bare |
| `client.createDomain(name)` / `client.domain(name)` / `client.getDomain(name)` | **The front door.** `createDomain` declares + returns the handle; `domain` is a lazy handle; `getDomain` verifies + materializes the Domain's catalog view in one batched read — `feeds()`/`feedNames()`, `readers()`, `writers()`, `subscriptions()`, `pipes()`, `indexes()`, `stateStores()`, `describe()`, `historyPolicy()`, `refresh()`. `domain.feed("orders")` resolves `shop.orders` with zero I/O. There is no flat `client.feed(...)`/`client.writer(...)` — Feeds and producers exist only inside a Domain |
| `domain.writer(name)` | **Session-bound Writer**: opens `OPEN WRITER SESSION` internally; `append(key, payload)` auto request IDs + epoch, `reopen()` after `StaleSession` |
| `domain.reader(name, maxPageSize)` | **Session-bound Reader**: opens internally; `fetch(limit)` tracks the delivered Cursor so `ack()` commits the right frontier; `close()` releases the fence |
| `domain.member(sub)` | **Session-bound member**: joins internally; `claim() → fetch → ack → release → transfer(work, peer)` with no epoch threading |
| `domain.subscription(name)` | `SubscriptionRef`: `member()`/`memberClient()` raw member plumbing (`join`, `claim`, `renew`, `release`, `transfer`, `fetch`, `ack`, `state()`), `memberState()` authoritative member state, `seek(position, ts, cursor)` operator reposition — refused while a member work lease is active |
| `domain.queryIndex(name, predicate)` | `exact` / `prefix` / `range` reads against an activated Index generation, with applied-vs-target freshness evidence |
| `domain.createReader(relative)` | **Fluent durable Reader**: `.readerId("fulfillment")` (the Kafka groupId analogue, resolved inside the Domain) → `.readerStartPoint(Beginning)` → `.maxPageSize(n)` → `.open()` returning a codec-bound `FeedReader<K,V>` |
| `domain.createWriter(relative)` | **Fluent durable Writer**: `.writerId("ingest")` → `.open()` returning a codec-bound `FeedWriter<K,V>` |
| `domain.feed(name[, type/codec])` | **Typed feed facade**: binds `Codec`s once so keys/payloads serialize automatically in both directions — `feed.writer(name)` (declares + opens a session), `feed.reader(name, maxPageSize)` (decoded `fetch`/`ack`), `feed.fetch`/`records`/`load` (decoded ad-hoc reads), `feed.subscribe` (decoded batches). Same codec vocabulary as `stateStore`: `String`/`byte[]`/`JsonNode`/Jackson classes out of the box, protobuf/Avro/custom via `dev.whitewater.codec`/`Codecs.of` |
| `domain.feedOps()` | Raw-byte ad-hoc Feed operations: committed record reads, anonymous temporary fetches (`tail`/`wait_ms`/`new_only`), ad-hoc appends, `load(feed, start, sink)` one-shot history reads — the escape hatch under `domain.feed(...)` |
| `domain.subscribe(reader, feed, start, onRecords)` | Managed durable consume: provisions the Reader, runs open → fetch → deliver → ack on a background thread; `Subscription.close()` to stop |
| `domain.stateStore(name[, type/codec])` / `domain.defineStateStore(spec, source)` | Typed server-side StateStore facade — durable RF3 rows on the Nodes, serialized by class (String/JSON) or bound `Codec`s (String keys + UTF-8 `String` payloads by default, `byte[]` via `byte[].class`/`Codecs.bytes()`): `put`/`get`/`getBy`/`iterate`/`delete`/`clear`/`drop`/`sync()`/`rebuild()`. Optional protobuf/Avro codecs in `dev.whitewater.codec` |
| `client.health()` / `client.checkCompatible()` | Liveness plus the protocol block (`major`/`minor`) and advertised transport limits; `checkCompatible()` refuses a major-version mismatch |

```java
var accounts = client.createDomain("accounts");
var spec = StateStoreSpec.named("employees")
        .addKey("passport", true).addKey("department", false);          // up to 10 named keys
var store = accounts.defineStateStore(spec, null);                    // durable store on the Nodes

store.put("emp-1", payload, Map.of("passport", "P123", "department", "eng"));
var row = store.get("emp-1");                          // StoreRow | null + freshness
var rows = store.getBy("passport", "P123");            // named key — a list
store.iterate(r -> export(r));
```

`domain.stateStore(name)` binds String keys + `byte[]` payloads by
default; `domain.stateStore(name, Codecs.json(Employee.class))` puts and
reads back your own objects — no `getBytes()` in application code.
`domain.feed(name, Employee.class)` binds Feeds the same way: writers
serialize, readers/fetches/subscriptions deserialize, and the codec
vocabulary (UTF-8, raw bytes, JSON classes, protobuf, Avro, custom) is
shared across feeds and stores:

```java
var demo = client.createDomain("demo");
Feed<String, Order> orders = demo.feed("orders", Order.class);
orders.writer("ingest").append("order-1", new Order("order-1", 42));
try (var reader = orders.reader("audit", 64)) {
    Order o = reader.fetch().records().get(0).value();   // decoded
}
StateStore<String, Order> latest = demo.stateStore("orders_latest", Order.class);
latest.put("order-1", order);                            // same codec seam
```

## Domains — the strongly-typed front door

A Domain owns every Feed it prefixes (`shop` owns `shop.orders`), so the
friendliest surface starts at the Domain and navigates down — the Kafka
`groupId` + `auto.offset.reset` pairing maps to `readerId` +
`readerStartPoint`:

```java
import static dev.whitewater.model.ReaderStart.*;   // Beginning, End

client.createDomain("shop");                        // CREATE DOMAIN shop;
Domain shop = client.getDomain("shop");             // verifies + materializes now
shop.createFeed("orders");                          // CREATE FEED shop.orders;
shop.feedNames();                                 // [shop.orders, shop.returns]
shop.getFeeds("order");                           // prefix-filtered Feed handles
shop.readers(); shop.writers(); shop.subscriptions();

// Everything a service needs hangs off the Domain:
var writer = shop.createWriter("orders")
        .writerId("ingest")
        .open();
var reader = shop.createReader("orders")
        .readerId("fulfillment")                    // durable consume identity
        .readerStartPoint(Beginning)                // first-time start position
        .maxPageSize(250)
        .open();
try (var pump = reader.listen(m -> handle(m))) {    // SDK-owned thread — returns now
    // … the service's other work runs untouched; pump.failure() surfaces
    //    a handler error; pump.close() stops + closes, page unacked → replays
}
// or stay manual: fetch() → handle records → ack()

```

`readerStartPoint` applies when the Reader is *first declared* — an
existing `readerId` resumes its acknowledged frontier, exactly like
`auto.offset.reset` on a committed Kafka group. `readerId`/`writerId`
resolve inside the Domain too (`"ingest"` → `shop.ingest`), so no
randomly-named resources escape the namespace. `domain.feed("orders")`
and `domain.getFeed("orders")` resolve `shop.orders` with no I/O; the
first catalog accessor loads the whole Domain view in one batched
control read and `refresh()` re-pulls it. `client.domain(name)` is the
lazy variant for code paths that only compose names.

### Record schemas

Records stay schema-neutral bytes on the wire — but a Feed *can* declare
schema metadata the catalog tracks for tooling, validation, and
compatibility. The Domain's schema is the default every Feed inherits;
a Feed's own declaration overrides the named sides:

```java
shop.setSchema(
        FeedSchema.value(SchemaDecl.of("json", "order-v1")));
shop.setFeedSchema("orders",
        FeedSchema.key(SchemaDecl.of("text")));

shop.feedSchema("orders");          // effective: key=text, value=json/order-v1
shop.declaredFeedSchema("orders");  // the Feed's own sides only
shop.schema();                      // the Domain default
shop.clearFeedSchema("orders");     // reverts to inheritance
```

`SET SCHEMA ON DOMAIN|FEED <name> (KEY|VALUE <format> [SUBJECT 'x']
[DEFINITION 'y'])+` in WCL does the same; `SHOW SCHEMAS` lists declared
assignments and `DESCRIBE FEED` shows `schema` + `effective_schema`.
Formats today: `json`, `jsonschema`, `avro`, `protobuf`, `text`, `bytes`, or `custom.<name>` —
compatibility enforcement is still a no-op, so treat declarations as
documentation/tooling metadata until the registry contract lands.

## Permanent consumers — the Kafka loop, minus the plumbing

`feed.consumer(name)` is the durable-Reader equivalent of
`while (true) consumer.poll(...)`: per-record delivery, internal
restart/backoff, durable server-side progress, graceful shutdown.

```java
var consumer = orders.consumer("enricher")
        .onRecord(msg -> process(msg))              // one record at a time
        .onError(err -> log.warn("retrying", err)); // visibility only
Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));
consumer.run();          // blocks until close() — that's the whole loop
```

Semantics: `run()` throws only for *startup* refusals (bad names, auth)
— Kafka-style fail-fast. After the first open, a dead session re-opens
and resumes from the Reader's durable acked cursor — the app never
manages offsets. A handler exception leaves its page unacked so it
redelivers (at-least-once; route poison pills to a DLQ feed yourself).
`consumer.runAsync()` runs the same loop on a thread for several
consumers.

Store lookups strip the row wrapper when you just want the value:

```java
var user = users.find(email)                         // Optional<UserProfile>
        .or(() -> users.findBy("user_id", id));      // named-key fallback
counters.put(bucket, counters.getOrDefault(bucket, 0L) + 1);
```

A complete enrichment service — read → store lookup + named-key
fallback → enriched feed or DLQ → counters — is in
[examples/EnrichUsers.java](examples/EnrichUsers.java).

The store lives on the Nodes — the SDK holds no local copy. See
[docs/stores.md](docs/stores.md) for the full store guide and
[docs/pipes.md](docs/pipes.md) for managed stream jobs
(forward/enrich/window) built on subscriptions and stores.

Errors surface as `WhitewaterException` subtypes mapped from wire `code`
values (`stale_epoch`, `lease.busy`, `lease.stale_lease`,
`writer.stale_sequence`, `control.unavailable`, ...). Retryable transient
refusals and genuinely ambiguous results (status + timeout) are distinct
subtypes — never lumped together.

Cursors are opaque: the SDK never parses, orders, or constructs them.
`event_time_ns`/`ingest_time_ns` are signed 64-bit nanoseconds and travel
as decimal strings on the wire.

## Pipes, indexes, and operations

Managed Pipes and replicated Indexes share the same topology-free model —
names, never partitions or Nodes:

```java
// Pipe declaration inside the Domain: names resolve in-domain (orders.relay
// is ops.orders.relay when declared on ops), and cross-Domain endpoints
// ride on ScopedRef — a Feed/StateStore/SubscriptionRef handle, or
// Scoped.absolute. The server owns panes, leases and the effect journal.
var ops = client.domain("ops");
var crm = client.domain("crm");
ops.defineForwardPipe("orders.relay",
        Scoped.relative("orders-sub"), ops.feed("orders.relayed"));
ops.defineEnrichPipe("orders.enrich",
        Scoped.relative("orders-sub"), ops.feed("orders.enriched"),
        crm.stateStore("customers"),                       // cross-Domain lookup
        PipeOperation.enrich(Extractor.recordKey(),
            EnrichFreshness.LATEST, null, EnrichMiss.QUARANTINE),
        ops.feed("orders.enrich-dlq"));
ops.defineWindowPipe("orders.hourly",
        Scoped.relative("orders-sub"), Scoped.relative("stats.orders-hourly"),
        PipeOperation.window(Time.h(1),   // 1h panes
            Extractor.recordKey(), WindowAggregation.count(),
            WindowTime.EVENT,
            null,                 // maxLate
            Time.m(5),            // watermark: 5m horizon
            Time.s(30),           // grace: 30s late revisions
            Time.h(2),            // expire
            null, null),          // keyLimit, byteLimit
        ops.feed("orders.hourly-emitted"));

// Pipe lifecycle: describe, pause, resume, drop.
var pipe = ops.pipe("orders.relay");
PipeInfo info = pipe.describe();   // stage, operation, outputs, status
pipe.pause();
pipe.resume();
List<PipeInfo> all = ops.pipeInfos();

// Index queries: exact, prefix, or range predicates over decoded rows.
IndexPage page = crm.queryIndex(
        "byregion",
        IndexPredicate.range(
                List.of("emea"),
                IndexPredicate.IndexBound.inclusive(0),
                IndexPredicate.IndexBound.exclusive(99)),
        50,        // limit
        true,      // strict: wait for the committed target rather than stale
        null);     // wait_ms
for (var row : page.rows()) { /* key + payload bytes, applied cursor */ }

// Demand observability: typed rows + raw domain rollups.
DemandReport feeds = client.demandFeeds();
for (DemandRow row : feeds.rows()) {
    long appended = row.counter("appended_records");
}
client.demandReaders(); client.demandSubscriptions();
client.demandIndexes();  client.demandPipes();
List<HotKey> hot = client.hotKeys();   // dominant-key evidence per feed
```

Pipe operation definitions stay opaque JSON so server-side operation
variants evolve without an SDK bump; `queryIndex` and the raw
`demand(surface)` JsonNode methods remain the escape hatches.

## API surface

Everything the SDK exposes, grouped by capability. The typed surface
(`Domain` → `Feed` → writer/reader/consumer) covers the daily path;
`Commands`, `wcl`, and the `*SessionClient`/`Member` classes are the
explicit escape hatches for the same wire contract.

| Area | API |
|---|---|
| Client & transport | `WhitewaterClient.builder().apiKey(..).endpoints(..).timeout(..)` · `healthy()` `health()` `checkCompatible()` · `close()` |
| Domains — the only door | `createDomain(name)` declare + handle · `domain(name)` lazy / `getDomain(name)` eager · `feeds()` `getFeeds(prefix)` `feedNames()` · `feed(..)`/`getFeed(..)` handles · `createFeed(relative)` `createReader(relative)` `createWriter(relative)` — declare/open inside the Domain · `readers()` `writers()` `subscriptions()` `pipes()` `indexes()` `stateStores()` · `describe()` `historyPolicy()` `domainId()` `revision()` · `refresh()` · `describeDomain` `renameDomain` `dropDomain` `listDomains` on the client |
| Schemas | `domain.setSchema()` `clearSchema()` · `domain.setFeedSchema(name, …)` `clearFeedSchema(name)` · `domain.schema()` `feedSchema()` `declaredFeedSchema()` · `SchemaDecl`/`FeedSchema` — `json`, `jsonschema`, `avro`, `protobuf`, `text`, `bytes`, `custom.<name>` |
| Feeds | `domain.feed(name[, keyType/Codec, valueType/Codec])` → `Feed<K,V>` · `useCompression()` · `records()` `fetch(..)` · `writer(..)` `append(..)` `reader(..)` `subscribe(..)` `consumer(..)` · `domain.feedOps()` raw-byte escape hatch |
| Writing | `FeedWriter.append(k, v)` / `append(k, v, metadata)` / `append(FeedEvent)` (event time + Metadata) · `appendBatch`/`appendAll` · `appendAsync` · `reopen()` `sessionEpoch()` `useCompression()` · idempotent `request_id` · `domain.writer(name)` `domain.writerSession(name, epoch)` sessions |
| Reading | `FeedReader.listen(handler)` SDK-owned pump thread → `Pump` (`close()` `failure()` `running()`) · `forEach(handler)` blocking pump · `forEachWhile` early-stop · `fetch([limit])` `ack()` `ack(cursor)` `acknowledgedCursor()` `close()` · durable `readerId` · `ReaderStart` — `Beginning`, `End`, `Now`, `cursor`, `timestamp(Instant)` · `domain.reader(name, cap)` `domain.readerSession(name)` `domain.seekReader(name, start)` |
| Temporary fetches | `Feed.fetch(after, limit)` / `domain.feedOps().temporaryFetch(feed, options)` — `after_event_time_ns` seeks, `new_only`, `tail` + `wait_ms` long-poll · `feedOps().load(feed, start, sink)` |
| Consumers | `feed.consumer(reader)` → `FeedConsumer`: `startAt(..)` `maxPageSize(..)` `onRecord(..)` `onError(..)` `backoff(..)` `runAsync()` `lastError()` `close()` · `domain.subscribe(reader, feed, start, onRecords)` managed loop |
| Subscription members | `domain.member(name)` → `Member`: `claim([workId, leaseTicks])` `renew` `release` `transfer` `fetch(work, limit)` `ack(work, page)` — shared progress + RF3 work leases · `domain.memberClient(name)` raw member client · `domain.subscription(name)` → `seek` `memberState` `progress` |
| StateStores | `domain.stateStore(name[, class/Codec])` → `StateStore<K,V>`: `put` `get`/`getStrict` `find` `findBy` `getOrDefault` `getBy`/`getByStrict` `delete` `page` `iterate` `clear` `sync` `rebuild` · `view(codecs)` · `domain.defineStateStore(spec, source)` `domain.dropStateStore(name)` |
| Indexes | `domain.queryIndex(name, IndexPredicate)` → `IndexPage` · `IndexPredicate.exact/prefix/range` + `IndexBound` · declared via `Commands.defineIndex` `getIndex` `rebuildIndex` |
| Pipes | `domain.pipe(name)` → `describe` `pause` `resume` `drop` · `domain.pipeInfos()` · `domain.defineForwardPipe` `defineEnrichPipe` `defineWindowPipe(Time, Extractor, WindowAggregation, WindowTime, ...)` — endpoints as `ScopedRef` (`Scoped.relative`, `Scoped.absolute`, or Feed/StateStore/SubscriptionRef handles) so Pipes can cross Domains explicitly |
| Catalog lifecycle | `domain.create*` `describe*` `rename*` `drop*` `list*` for Feed, Writer, Reader, Subscription — names resolve inside the Domain · `client.listDomains` `listRoles` `listGrants` `createRole`/`describeRole`/`renameRole`/`dropRole` · `domain.openWriterSession` `domain.seekReader` |
| Auth | `Commands.grant` `revokeGrant` `issueApiKey` `revokeApiKey` `explainAccess` · `Permission`/`GrantEffect` |
| Observability | `demandFeeds()` `demandReaders()` `demandSubscriptions()` `demandIndexes()` `demandPipes()` `demand(surface)` → `DemandReport`/`DemandRow` · `hotKeys()` `demandHotKeys(feed)` → `HotKey` · `nodeMetrics()` `clusterMembers()` `subscriptionProgress(name)` |
| Storage admin | `Commands.registerStorageNode` `drainStorageNode` `undrainStorageNode` `retireStorageNode` `inspectStorageDrain` `reportStorageNodeCapacity` · `inspectPlacement(feed)` |
| Escape hatches | `wcl(script)` `wclAsync` · `commands(Commands.of()...)` fluent batch · `commands(List<Map>)` raw · `domain.feedOps()` byte-level feed ops · `domain.writerSession` `domain.readerSession` `domain.memberClient` byte-level sessions |
| Errors | `WhitewaterException` — `.Refused` `.StaleSession` `.AmbiguousResult` `.Conflict` `.Unavailable` `.LeaseConflict` · `code()` carries the wire `code` |
| Time & durations | `Time.h/.m/.s/.ms/.us/.ns/.d/.of(Duration)` for windows + delivery delays · absolute times stay epoch-ns decimal strings |
| Testing | `FakeRiverbed.start()` — loopback HTTP Riverbed implementing the real wire contract (dedup, epochs, at-least-once reads, stores, WCL/commands) |

## Build

```powershell
mvn verify          # compile + unit tests (+ conformance, if configured)
```

No local Maven? Docker works:

```powershell
docker run --rm -v "${PWD}:/work" -w /work maven:3.9-eclipse-temurin-21 mvn verify
```

## Unit-test doubles

`dev.whitewater.testing.FakeRiverbed` is an in-JVM loopback Node that
models the wire contract — dedupe, ack gating, epoch fencing, StateStore
named keys — so application tests exercise the real client path.

```java
try (var riverbed = FakeRiverbed.start();
     var client = riverbed.client()) { ... }
```

See `src/test/java/dev/whitewater/testing/FakeRiverbedTest.java` and the
[testing guide](../whitewater/docs/guides/testing-your-app.md).

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
