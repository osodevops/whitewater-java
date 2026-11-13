# whitewater-java

Java SDK for Whitewater. Implements the versioned
`whitewater-protocol` wire contract defined in the main repository under
`sdk/spec/` (`PROTOCOL.md`, `endpoints.json`, `conformance/`).

Requires Java 17+. The only runtime dependency is Jackson Databind; HTTP
uses `java.net.http` — no framework required.

A complete microservice walkthrough — provision, produce, pump-process, dead-letter, shutdown — lives in [`docs/demo-service.md`](docs/demo-service.md).

## Creating a client

```java
WhitewaterClient client = WhitewaterClient.builder()
        .apiKey(System.getenv("WHITEWATER_API_KEY"))
        .endpoints(List.of(
                URI.create("https://node1:7070"),
                URI.create("https://node2:7070"),
                URI.create("https://node3:7070")))
        .build();
```

`healthy()`/`health()` check liveness and return the protocol block
(`major`/`minor`) plus advertised transport limits; `checkCompatible()`
refuses a major-version mismatch up front. The client also carries the
flat administration surface — WCL (`wcl(script)`), typed commands
(`client.commands(Commands.of()…)`), cluster health, demand reports,
and Domain lifecycle (`createDomain`, `domain`/`getDomain`,
`describeDomain`, `renameDomain`, `dropDomain`, `listDomains`, domain
schema and role management). Everything else — Feeds, Writers, Readers,
Subscriptions, Members, Pipes, StateStores, Indexes — is reached
**only** through a `Domain` handle.

## Domains — the strongly-typed front door

A Domain is the administrative namespace every business resource lives
inside: a Feed named `shop.orders` belongs to the Domain `shop`, and
nothing outside a Domain can reach it. Whitewater uses Domains instead
of flat topic naming because the namespace is *structural*, not a
convention — Writers, Readers, Subscriptions, StateStores, Indexes, and
Pipes resolve their relative names inside the owning Domain, so a
`"fulfillment"` Reader declared on `shop` is `shop.fulfillment`
everywhere it matters: progress, leases, WCL, demand reports, and
access control all scope to it. Cross-Domain references are possible
but always explicit (`ScopedRef` handles), which makes accidental
cross-wiring of two tenants' resources a compile-time-looking mistake
rather than a silent runtime one.

```java
import static dev.whitewater.model.ReaderStart.*;   // Beginning, End

client.createDomain("shop");                        // CREATE DOMAIN shop;
Domain shop = client.getDomain("shop");             // verifies + materializes now
shop.createFeed("orders");                          // CREATE FEED shop.orders;
shop.feedNames();                                 // [shop.orders, shop.returns]
shop.getFeeds("order");                           // prefix-filtered Feed handles
shop.readers(); shop.writers(); shop.subscriptions();

// Everything a service needs hangs off the Domain:
var writer = shop.feed("orders").writer("ingest");   // durable producer identity — required
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

## Working with Feeds

### Reading — durable Readers and ad-hoc fetches

A **Reader** owns a durable, server-side Cursor — the progress identity
that survives restarts, exactly like a committed Kafka group offset but
without the group protocol:

```java
var reader = shop.createReader("orders")
        .readerId("fulfillment")            // durable consume identity
        .readerStartPoint(Beginning)        // first-time start position
        .maxPageSize(250)
        .open();
var page = reader.fetch();                  // up to maxPageSize records
for (var r : page.records()) handle(r);
reader.ack();                               // commits the delivered frontier
reader.close();                             // releases the session fence
```

`readerStartPoint` applies only when the Reader is *first declared* —
an existing `readerId` resumes its acknowledged frontier
(`auto.offset.reset` semantics). `reader.listen(m -> handle(m))` runs
the same fetch→ack cycle on an SDK-owned thread and returns a `Pump`
handle (`failure()` surfaces handler errors; `close()` stops cleanly
with the last page unacked → replayed).

For ad-hoc reads without a durable identity, `domain.feedOps()` covers
committed history: `records(feed, limit, after)`, `temporaryFetch` with
`tail`/`wait_ms` long-poll and `after_event_time_ns` seeks, and
`load(feed, start, sink)` one-shot history replays.

### Shared progress — subscriptions and members

A named **Subscription** is the durable shared frontier cooperating
readers claim work against — the Kafka consumer-group model, minus the
rebalance storm. `domain.subscription(name)` returns a `SubscriptionRef`
for declaration-level operations (`seek`, `memberState`, `progress`);
`domain.member(sub)` joins a member and runs the work loop:

```java
var member = shop.member("orders-sub");
var work = member.claim();                  // RF3-fenced work lease
var page = member.fetch(work, 256);         // records for this member
member.ack(work, page);                     // commits shared progress
member.release(work);                       // or transfer(work, otherMember, null)
```

Leases are fenced — a lost or expired claim is refused, and work
transfers cleanly to another member. `seek(position|timestamp|cursor)`
repositions the shared frontier and is refused while a member work
lease is active.

### The read loop — consumers

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

`run()` throws only for *startup* refusals (bad names, auth) —
Kafka-style fail-fast. After the first open, a dead session re-opens
and resumes from the Reader's durable acked cursor. A handler exception
leaves its page unacked so it redelivers (at-least-once; route poison
pills to a DLQ feed yourself). `consumer.runAsync()` runs the same loop
on a thread for several consumers; `domain.subscribe(reader, feed,
start, onRecords)` is the lighter managed pump when you don't need
consumer supervision knobs.

### Writing

```java
record Order(String id, int total) {}

var writer = orders.writer("ingest");          // one call — declares, binds codecs
writer.write("order-42", new Order("order-42", 99),
        result -> ack(result.messageId()));                    // per-record ack callback
writer.write("order-43", new Order("order-43", 20))
        .thenAccept(r -> commit(r.cursor()));                  // or the CompletableFuture
writer.close();                                                // flushes what is queued
```

`write()` is the default produce path — always asynchronous. The call
returns a `CompletableFuture<AppendResult>` (or fires your callback)
once the record is **majority-committed** — RF3 means at least 2 of 3
Nodes durably hold it, and `result.durability()` reports the mode. The
`AppendResult` is the acknowledgement: `messageId()` (the record's
committed identity), `cursor()`, `deduplicated()`, and server
`feedback()`.

Under the hood a daemon sender coalesces queued records into
`append-batch` commits — you never think about batching. Tune the
tradeoff on the builder: `linger(Duration)` (how long a batch stays
open for more records, default 5ms) and `maxBatchRecords(int)` (flush
early at a size bound, default 256). The session's fencing epoch binds
lazily on the first write — an idle handle holds no epoch — and
`close()` flushes queued writes before returning, so use
try-with-resources or a shutdown hook.

The manual forms stay for when you need them: `append(key, value)`
blocks until the commit lands, `appendBatch`/`appendAll` put a group of
records in one commit, `appendAsync` is the non-coalescing future form,
and `reopen()` rebinds a fresh epoch after a fenced `StaleSession`:

```java
writer.append("order-42", new Order("order-42", 99));
writer.append(FeedEvent.of("order-43", payload).withEventTimeNs(eventTimeNs));
writer.appendBatch(List.of(e1, e2));
writer.reopen();
```

`writerId` is required because it's the Writer's transactional identity,
not a label: the fencing epoch binds to it (a second process binding
`ingest` fences the first, so a partitioned zombie can't double-commit)
and the retry-dedup ledger lives under it — the `request_id` on every
record deduplicates a timed-out retry, while reusing an id with
different contents is refused. `useCompression()` opts into LZ4; record
`event_time_ns`/`ingest_time_ns` are signed 64-bit epoch nanoseconds
(decimal strings on the wire).

## Working with StateStores

StateStores are durable, replicated key/value views that live on the
Nodes — the SDK holds no local copy. Declare named keys up front and
each becomes a maintained secondary index.

### Declaring a store — named keys

```java
var accounts = client.createDomain("accounts");
var spec = StateStoreSpec.named("employees")            // → accounts.employees
        .addUniqueKey("passport")                     // one row per value
        .addNonUniqueKey("department");               // value → many rows
var store = accounts.defineStateStore(spec, null);    // durable store on the Nodes
```

**Unique** (`passport`, `email`, `order_number`) — one value maps to at
most one row; `getBy` returns the single match and a second claim on the
same value arbitrates first-committed-wins. **Non-unique**
(`department`, `status`, `region`) — one value maps to many rows;
`getBy` returns every match as a bounded page. Up to 10 named keys,
declared at creation.

Two store kinds: **manual** (your app puts rows — no extractor) and
**feed-sourced** (the Nodes replay a source Feed and project rows +
postings via `Extractor.recordKey()`, `Extractor.metadata("x")`,
`Extractor.json("$.path")` — required on feed-sourced keys, refused on
manual ones).

### Putting and deleting

The preferred form carries postings in a typed **KeySet** — one small
class per store, extending `AbstractStoreKeySet` with a setter per named
key (each just delegates to the inherited `put(name, value)`):

```java
final class PassportKeySet extends AbstractStoreKeySet {
    PassportKeySet passport(String v)   { return put("passport", v); }
    PassportKeySet department(String v) { return put("department", v); }
    static PassportKeySet with(String passport, String department) {
        return new PassportKeySet().passport(passport).department(department);
    }
    static PassportKeySet of(Consumer<PassportKeySet> builder) {
        var ks = new PassportKeySet();
        builder.accept(ks);
        return ks;
    }
}
```

Then every posting style is compile-checked against the store's named
keys:

```java
store.put("emp-1", payload, PassportKeySet.with("P123", "eng"));              // factory
store.put("emp-1", payload, PassportKeySet.of(k -> k.passport("P123").department("eng")));
store.put("emp-1", payload, new PassportKeySet().passport("P123").department("eng"));
store.withKeySet(PassportKeySet.class)
     .put("emp-1", payload, k -> k.passport("P123").department("eng"));       // bound lambda
store.put("emp-1", payload, "P123");                              // single named key
store.put("emp-1", payload, Map.of("passport", "P123", "department", "eng")); // raw map — no checking

store.delete("emp-1");                        // mutation id generated
store.delete("emp-1", stableMutationUuid);    // stable id — dedup retries
store.clear();                                // one journaled ClearAll
```

Every mutation carries an idempotent `mutation_id` — generated per call,
or pinned for caller-driven retries.

### Reading

```java
import dev.whitewater.model.IndexPredicate.IndexBound;   // posting ranges

var row = store.get("emp-1");                      // StoreRow | null + freshness
var fresh = store.getStrict("emp-1");              // bounded-waits for the
                                                  // committed frontier
var user = users.find(email)                       // Optional<Employee>
        .or(() -> users.findBy("user_id", id));    // named-key fallback
var n = counters.getOrDefault(bucket, 0L);         // counters, one line

var rows = store.getBy("department", "eng");                   // every match
var page = store.getBy("department", "eng",
        IndexBound.inclusive(100), IndexBound.exclusive(200), 25);  // ranged
var strict = store.getByStrict("department", "eng", Duration.ofSeconds(5));

store.iterate(r -> export(r));                   // primary-key order
var page = store.page(afterKey, 256);            // one bounded page
```

`row.freshness()`/`rows.freshness()` is **evidence, not a required step**: `appliedSeq` /
`committedSeq` / `behind` report whether the serving replica answered
behind its committed frontier, and `servedBy` names it — the handles
you want when a read looks stale. `get`/`getBy` read immediately;
`getStrict`/`getByStrict` bounded-wait for the replica to apply its
committed frontier rather than answering stale. A complete enrichment
service — read → store lookup + named-key fallback → enriched feed or
DLQ → counters — is in [examples/EnrichUsers.java](examples/EnrichUsers.java).
See [docs/stores.md](docs/stores.md) for the full store guide.

## Working with Pipes

Managed Pipes are server-run stream jobs: you declare the business
shape — source Subscription, output Feed, operation — and the server
owns the subscription frontier, compute lease, panes, and the effect
journal. Cross-Domain endpoints ride on `ScopedRef`: a
`Feed`/`StateStore`/`SubscriptionRef` handle or `Scoped.absolute`.

### Simple — forwarding

```java
var ops = client.domain("ops");
ops.defineForwardPipe("orders.relay",
        Scoped.relative("orders-sub"), ops.feed("orders.relayed"));
```

### Enrichment — StateStore lookups

```java
var crm = client.domain("crm");
ops.defineEnrichPipe("orders.enrich",
        Scoped.relative("orders-sub"), ops.feed("orders.enriched"),
        crm.stateStore("customers"),                       // cross-Domain lookup
        PipeOperation.enrich(Extractor.recordKey(),
            EnrichFreshness.LATEST, null, EnrichMiss.QUARANTINE),
        ops.feed("orders.enrich-dlq"));
```

### Time-based — windowing

```java
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
```

### Indexes and observability

```java
// Pipe lifecycle: describe, pause, resume, drop.
var pipe = ops.pipe("orders.relay");
PipeInfo info = pipe.describe();   // stage, operation, outputs, status
pipe.pause(); pipe.resume();
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
for (var r : page.rows()) { /* key + payload bytes, applied cursor */ }

// Demand observability: typed rows + per-domain rollups.
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
`demand(surface)` JsonNode methods remain the escape hatches. See
[docs/pipes.md](docs/pipes.md) for the full managed-job guide.

## Errors, cursors, and time

Errors surface as `WhitewaterException` subtypes mapped from wire `code`
values (`stale_epoch`, `lease.busy`, `lease.stale_lease`,
`writer.stale_sequence`, `control.unavailable`, ...). Retryable transient
refusals and genuinely ambiguous results (status + timeout) are distinct
subtypes — never lumped together.

Cursors are opaque: the SDK never parses, orders, or constructs them.
`event_time_ns`/`ingest_time_ns` are signed 64-bit nanoseconds and travel
as decimal strings on the wire — `Time.h/.m/.s/.ms/.us/.ns/.d/.of(Duration)`
covers window and delay declarations; absolute times stay epoch-ns.

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

## Full API surface

Everything the SDK exposes, grouped by capability. The typed surface
(`Domain` → `Feed` → writer/reader/consumer) covers the daily path —
binding `domain.feed(name, Order.class)` or
`domain.stateStore(name, Employee.class)` to a codec is the
**read-with-schema / write-with-schema** story: records and store rows
serialize both directions with no `getBytes()` in application code.
`Commands`, `wcl`, and the `*SessionClient`/`Member` classes are the
explicit escape hatches for the same wire contract.

| Method | Parameters | Description |
|---|---|---|
| **Client & transport** | | |
| `WhitewaterClient.builder()…build()` | `apiKey`, `endpoints(URI…)`, `timeout` | Client + transport construction |
| `healthy()` `health()` `checkCompatible()` `close()` | — | Liveness, protocol compatibility, shutdown |
| **Domains — the only door** | | |
| `createDomain(name)` | relative name | Declare + return eager handle |
| `domain(name)` / `getDomain(name)` | relative name | Lazy / eager Domain handle |
| `feed(name[, keyType, valueType])` `getFeed(name)` | name, `Class`/Codec pairs | Typed `Feed<K,V>` handle |
| `createFeed` `createReader` `createWriter` | relative name | Declare + open inside the Domain |
| `feeds()` `getFeeds(prefix)` `feedNames()` | optional prefix | Feed catalog views |
| `readers()` `writers()` `subscriptions()` `pipes()` `indexes()` `stateStores()` | — | Domain catalog lists |
| `describe()` `domainId()` `revision()` `historyPolicy()` `refresh()` | — | Domain metadata |
| `describeDomain` `renameDomain` `dropDomain` `listDomains` | name | Client-level Domain lifecycle |
| `setSchema` `clearSchema` `setFeedSchema` `clearFeedSchema` `schema` `feedSchema` `declaredFeedSchema` | `FeedSchema`/`SchemaDecl` — `json`, `avro`, `protobuf`, `text`, `bytes`, `custom.<name>` | Record-schema declarations |
| **Feeds** | | |
| `records(limit, after)` `fetch(after, limit)` | page bound, cursor | Committed history reads |
| `writer(name)` `reader(name, capacity)` `subscribe(...)` `consumer(name)` | relative name, options | Bound sessions + loops |
| `useCompression()` | — | LZ4 on subsequent writes |
| `domain.feedOps()` | — | Raw-byte escape hatch |
| **Writing** | | |
| `write(k, v)` `write(k, v, callback)` `write(FeedEvent)` | key, value, `Consumer`/`BiConsumer` | Async default — `AppendResult` on majority commit; coalesced by `linger`/`maxBatchRecords` |
| `append(k, v)` `append(k, v, metadata)` `append(FeedEvent)` | key, value, Metadata/event | Blocking single write |
| `appendBatch(events)` `appendAll(events)` `appendAsync(k, v)` | `List<FeedEvent>` | One commit / non-coalescing future |
| `reopen()` `sessionEpoch()` `useCompression()` `pending()` `close()` | — | Session + pipeline lifecycle |
| `domain.writer(name)` `domain.writerSession(name, epoch)` | name, epoch | Byte-level writer sessions |
| **Reading** | | |
| `fetch([limit])` `ack()` `ack(cursor)` `acknowledgedCursor()` `close()` | page bound, cursor | Manual durable-Reader loop |
| `listen(handler)` → `Pump` `forEach` `forEachWhile` | record handler | SDK-owned read loop |
| `ReaderStart` | `Beginning` `End` `Now` `cursor` `timestamp` | First-declare start position |
| `domain.reader(name, cap)` `readerSession(name)` `seekReader(name, start)` | name, capacity, start | Byte-level reader sessions |
| `feedOps().temporaryFetch(feed, options)` `load(feed, start, sink)` | `after_event_time_ns`, `tail`, `wait_ms` | Ad-hoc + replay reads |
| `feed.consumer(name)` → `FeedConsumer` | `startAt` `maxPageSize` `onRecord` `onError` `backoff` | Supervised durable consumer |
| `domain.subscribe(reader, feed, start, onRecords)` | names, handler | Managed pump loop |
| **Subscription members** | | |
| `domain.member(name)` → `Member` | `claim([workId, leaseTicks])` `renew` `release` `transfer` `fetch(work, limit)` `ack(work, page)` | Shared progress + RF3 work leases |
| `domain.subscription(name)` `domain.memberClient(name)` | `seek` `memberState` `progress` | Declaration ops + raw member client |
| **StateStores** | | |
| `defineStateStore(spec, source)` `dropStateStore(name)` `stateStore(name[, codecs])` | spec, source Feed, `Class`/Codec | Store lifecycle + typed handle |
| `put` `delete` `clear` | key, payload, named-key postings | Mutations |
| `get` `getStrict` `find` `findBy` `getOrDefault` `getBy` `getByStrict` `page` `iterate` | key, named key, bounds, wait | Row + named-key reads |
| `sync` `rebuild` `view(codecs)` | — | Store administration |
| **Indexes** | | |
| `queryIndex(name, predicate)` | `IndexPredicate` + `IndexBound`, limit, strict, wait | Decoded index rows |
| `Commands.defineIndex` `getIndex` `rebuildIndex` | name, extractor | Index lifecycle |
| **Pipes** | | |
| `defineForwardPipe` `defineEnrichPipe` `defineWindowPipe` | name, `ScopedRef` source/output, operation | Managed jobs |
| `pipe(name)` `pipeInfos()` | — | `describe` `pause` `resume` `drop` |
| **Auth** | | |
| `Commands.grant` `revokeGrant` `issueApiKey` `revokeApiKey` `explainAccess` | principal, `Permission`, `GrantEffect` | Access control |
| **Observability** | | |
| `demandFeeds` `demandReaders` `demandSubscriptions` `demandIndexes` `demandPipes` `demand(surface)` `hotKeys` `demandHotKeys(feed)` `nodeMetrics` `clusterMembers` `subscriptionProgress` | surface, feed, name | Demand + hot-key evidence |
| **Storage admin** | | |
| `registerStorageNode` `drainStorageNode` `undrainStorageNode` `retireStorageNode` `inspectStorageDrain` `reportStorageNodeCapacity` `inspectPlacement` | node, feed | Storage operations |
| **Escape hatches, errors, time** | | |
| `wcl(script)` `wclAsync` `commands(...)` `feedOps()` `writerSession` `readerSession` `memberClient` | script, typed/raw commands | Byte-level wire access |
| `WhitewaterException` | `code()` | `.Refused` `.StaleSession` `.AmbiguousResult` `.Conflict` `.Unavailable` `.LeaseConflict` |
| `Time.h/.m/.s/.ms/.us/.ns/.d/.of(Duration)` | duration | Window + delay declarations |
| `FakeRiverbed.start()` | — | Loopback wire-contract test double |
