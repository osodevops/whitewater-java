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
| `ReaderStart` | Reader/Subscription start positions: `beginning()`, `now()`, `cursor(c)`, `timestampNs(ns)` |
| `client.writer(name, epoch)` | Idempotent single and batch appends (`request_id` dedup) |
| `client.reader(name)` | Named-Reader sessions: open / fetch / ack / close / resume |
| `client.subscriptionMember(sub)` | Join, claim, renew, fetch, ack, release under fenced leases |
| `client.feeds()` | Committed record reads, anonymous temporary fetches (`tail`/`wait_ms`/`new_only`), ad-hoc appends |

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
