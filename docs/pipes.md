# Pipes

A **Pipe** is a managed stream job: it drives one declared **Subscription**
and applies an operation to every record the Subscription delivers, writing
results to a Feed or a StateStore. Pipes are declared through the command
vocabulary and driven server-side — your process does not need to stay
alive for a Pipe to keep running.

Three operations exist:

| Operation | Reads from | Writes to | Purpose |
|---|---|---|---|
| `forward` | Subscription | Feed | copy records verbatim |
| `enrich` | Subscription + StateStore | Feed (+ optional quarantine Feed) | point-lookup enrichment |
| `window` | Subscription | StateStore (+ optional emit Feed) | fixed-window aggregation |

```java
var client = WhitewaterClient.builder()
        .endpoints(URI.create("http://localhost:7371"))
        .apiKey(apiKey)
        .build();
```

## 1. Prerequisites: subscriptions and stores

Every Pipe names a **Subscription** — the durable delivery lease it pulls
input through. Declare one per Pipe (sharing a Subscription across Pipes
shares its frontier):

```java
import dev.whitewater.admin.Commands;
import dev.whitewater.model.ReaderStart;

client.commands(Commands.of()
        .createSubscription("orders.sub", "ops.orders", ReaderStart.beginning()));
```

A Subscription can also cover a whole Domain (every Feed in it, present
and future):

```java
client.commands(Commands.of()
        .createDomainSubscription("ops.sub", "ops", ReaderStart.beginning()));
```

`enrich` and `window` pipes write to or read from a **StateStore**. Declare
one up front (manual or feed-sourced — see `docs/stores.md`):

```java
import dev.whitewater.model.Extractor;
import dev.whitewater.model.StateStoreSpec;

var spec = StateStoreSpec.named("ops.customers")
        .addKey("email", true, Extractor.json("$.email"))
        .addKey("tier", false, Extractor.json("$.tier"));
client.stateStores().define(spec, "ops.customer.changes"); // feed-sourced
```

## 2. Forward pipes

The copy pipe — every delivered record lands on the output Feed with its
key, payload, and metadata intact. New cursor and ingest time on the
output side; the durable frontier advances on the Subscription.

```java
client.commands(Commands.of()
        .defineForwardPipe(
                "ops.orders.relay",      // pipe name (dotted resource name)
                "orders.sub",            // source subscription
                "ops.orders.relayed"));  // output feed
```

## 3. Enrich pipes

Point-lookup enrichment: for each input record, project a lookup key with
an `Extractor`, resolve it against a StateStore, and copy the matched
row's payload into output metadata as `enrich.payload` plus
`enrich.matched = true|false`. Lookup resolution rides inside the effect
journal — replayed outputs are byte-identical.

```java
import dev.whitewater.model.PipeOperation;

client.commands(Commands.of().defineEnrichPipe(
        "ops.orders.enrich",
        "orders.sub",
        "ops.enriched",       // output feed
        "ops.customers",                  // lookup store
        PipeOperation.enrich(
                Extractor.json("$.customerId"),   // project record -> store key
                EnrichFreshness.LATEST,           // LATEST or STRICT
                "email",                          // named key to hit (null = primary)
                EnrichMiss.FORWARD),              // or QUARANTINE
        "ops.enrich.misses"));              // quarantine feed (required if QUARANTINE)
```

- `Extractor` — `recordKey()`, `metadata(name)`, or `json(field)`
- `EnrichFreshness.LATEST` serves replica state; `STRICT` fails rather
  than serve a stale row
- `EnrichMiss.FORWARD` forwards with `enrich.matched=false`; `DROP`
  consumes the record without emitting (the frontier still commits);
  `QUARANTINE` diverts the record to the quarantine feed you name in the
  final argument

## 4. Window pipes

Fixed-window aggregation. Each delivered record projects a window key and
a value; the pipe accumulates panes in a declared StateStore view and can
emit closed panes to an output feed.

A window pipe's output store must exist before the pipe declares — the
server refuses `resource not found` otherwise — and it must carry
**non-unique named keys `key` and `pane`**. Pane rows post their group key
and pane index under those two reserved keys so the view stays enumerable
without knowing composite row keys.

```java
import dev.whitewater.model.NamedKey;

client.commands(Commands.of()
        // Pane view: 'key' and 'pane' are required, non-unique.
        .defineStateStore("ops.order.counts", null, List.of(
                new NamedKey("key", false),
                new NamedKey("pane", false)))
        .defineWindowPipe(
        "ops.orders.hourly",
        "orders.sub",
        "ops.order.counts",               // output store (declared above)
        PipeOperation.window(
                Time.h(1),                             // 1h panes
                Extractor.json("$.region"),            // window key
                WindowAggregation.sum(Extractor.json("$.total")),
                WindowTime.EVENT,                      // EVENT or INGEST clock
                Time.m(5),                             // maxLate: 5min ingest bound
                Time.d(1),                             // expire: keep panes 24h
                10_000L,                               // keyLimit
                null),                                 // byteLimit
        "ops.hourly.emits"));              // emit feed (optional)
```

Every duration a pipe declares is a `Time` — `Time.h`, `Time.m`,
`Time.s`, `Time.ms`, `Time.us`, `Time.d`, `Time.ns` (or
`Time.of(Duration)`). The SDK extracts nanoseconds internally for the
wire; callers never spell raw nanosecond counts.

### Lateness models — pick exactly one

`PipeOperation.window` has two mutually exclusive lateness models:

- **`maxLate`** — ingest-clock bound. Events arriving more than
  `maxLate` behind quarantine into a countable `late` row.
- **`watermark`** — event-time horizon `max(event_time) - watermark`
  closes a pane once it passes the pane end. Refused under
  `WindowTime.INGEST`. Combine with `grace` to let late events *revise*
  an emitted pane (re-emits under a bumped revision) before counting into
  `late`.

```java
// watermark + grace
PipeOperation.window(Time.m(1), Extractor.recordKey(),
        WindowAggregation.count(),
        WindowTime.EVENT,
        null,              // maxLate stays null — watermark model instead
        Time.s(30),        // watermark: close a minute 30s after its end
        Time.s(15),        // grace: revise emitted panes up to 15s late
        null, null, null)
```

Convenience overloads: `window(time, key, agg)`, `window(time, key, agg,
watermark)`, `window(time, key, agg, watermark, grace)`.

### Aggregations

```java
WindowAggregation.count()                          // rows per pane
WindowAggregation.sum(Extractor.json("$.total"))   // sums the extracted value
WindowAggregation.average(Extractor.json("$.rtt")) // running mean
```

`expire` bounds pane retention once a pane provably closes;
`keyLimit`/`byteLimit` bound store growth.

## 5. Lifecycle

```java
var pipe = client.pipe("ops.orders.relay");

PipeInfo info = pipe.describe();  // stage, operation, outputs, status
pipe.pause();                     // cooperative: in-flight effects finish
pipe.resume();                    // restarts from the durable frontier
pipe.drop();                      // retires declaration + compute lease

List<PipeInfo> all = client.pipes();
```

`pause` is durable and idempotent — the frontier stays put so resume is
replay-safe. `describe()` returns `stage` (`declared`/`paused`), the
opaque `operation` node, and output Feed/Store IDs.

## 6. Watching demand

`demandPipes()` reports the declared-vs-driven spread so you can tell at a
glance whether a Pipe is actually being driven:

```java
DemandReport demand = client.demandPipes();
demand.rows().forEach(r ->
        System.out.printf("%s stage=%s%n", r.name(), r.stage()));
```

## 7. Failure semantics

- Declarations are **committed atomically** through the command batch —
  `client.commands(Commands.of().createSubscription(...).defineForwardPipe(...))`
  applies both or neither.
- Pipe delivery is **at-least-once** on the Subscription; effects journal
  lookups/accumulation so replayed outputs are byte-identical.
- A paused pipe keeps its durable frontier; nothing is dropped.
