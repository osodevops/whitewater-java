# Demo: order-fulfillment service

A complete microservice shape: provision a Domain + Feed, append
orders, and pump them through a processor with at-least-once
semantics, a dead-letter path, and graceful shutdown.

The contract in one line: **return = processed, throw = replay** — the
SDK acks a page only after every record in it has been handled.

```java
import dev.whitewater.WhitewaterClient;
import java.net.URI;
import static dev.whitewater.model.ReaderStart.*;

var client = WhitewaterClient.builder()
        .apiKey(System.getenv("WHITEWATER_API_KEY"))
        .endpoints(URI.create("http://localhost:7371"),
                  URI.create("http://localhost:7372"))
        .build();

// ---- provision -----------------------------------------------------
client.createDomain("shop");
var shop = client.getDomain("shop");
shop.createFeed("orders");
shop.createFeed("orders.dlq");            // dead-letter feed

// ---- produce (usually a different service) -------------------------
// Bind codecs on the Feed handle — the SDK serializes, never getBytes.
var orders = shop.feed("orders", String.class);       // JSON text payloads
var writer = shop.createWriter(orders).writerId("orders.ingest").open();
writer.append("order-1", "{\"total\":42}");

// ---- consume --------------------------------------------------------
var dlq = shop.feed("orders.dlq", String.class);
var deadLetter = shop.createWriter(dlq)
        .writerId("fulfillment.dlq").open();
var reader = shop.createReader(orders)
        .readerId("fulfillment")          // durable id — survives restarts
        .readerStartPoint(Beginning)      // first-declare only
        .maxPageSize(250)                 // max records per fetch page
        .open();

// SDK-owned thread — returns immediately; the service keeps running.
var pump = reader.listen(m -> {
    try {
        ordersDb.upsert(orderId(m), order(m));      // idempotent side effect
    } catch (RuntimeException e) {
        if (isPermanent(e)) {
            deadLetter.append(m.key(), m.value());  // quarantine → continue → acked
            return;
        }
        throw e;                                    // transient → stop → replay
    }
});

// ---- shutdown -------------------------------------------------------
// pump.close() stops the thread + closes the session; the in-flight
// page stays unacked and replays on the next open.
Runtime.getRuntime().addShutdownHook(new Thread(pump::close));

// Supervise: a dead pump surfaces through pump.failure() — restart or
// alert from a health check / scheduled task:
//   if (pump.failure() != null) { restart(); }
```

## Notes

- `readerId` is durable application state — the Kafka `groupId`
  analogue. `readerStartPoint` applies only on first declare; restarts
  resume the acknowledged frontier.
- Acks are per fetched page. A crash mid-page replays the whole page —
  that's what makes it at-least-once. Process idempotently.
- `maxPageSize(250)` bounds both the page and the replay window.
- Manual mode stays available: `fetch()` → process → `ack()`. Never
  ack before processing — that's at-most-once.
