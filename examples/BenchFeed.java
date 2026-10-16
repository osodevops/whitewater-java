import dev.whitewater.Feed;
import dev.whitewater.FeedWriter;
import dev.whitewater.WhitewaterClient;
import dev.whitewater.model.Cursor;
import dev.whitewater.model.FeedEvent;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Ad-hoc throughput/latency benchmark through the Java SDK — the Java
 * counterpart to scripts/bench-feed.py and scripts/bench-kafka.py in the
 * whitewater repository, so the same workload is comparable end-to-end:
 * Whitewater wire protocol vs Java SDK overhead vs Kafka's client path.
 *
 * <p>Run it against a live Riverbed (defaults match bench-feed.py's
 * whitewater-bench fixture on 7771-7773):
 *
 * <pre>{@code
 * mvn -q package
 * javac -cp target/classes:$(cat target/cp.txt) -d target/examples examples/BenchFeed.java
 * java -cp "target/examples;target/classes;$(cat target/cp.txt)" BenchFeed
 * }</pre>
 *
 * <p>Env knobs mirror the Python drivers: WHITEWATER_ENDPOINTS (csv),
 * WHITEWATER_API_KEY, BENCH_RECORDS, BENCH_BYTES, BENCH_WRITERS,
 * BENCH_BATCH, BENCH_MODE (write|read|all). Prints BENCH_RESULT {json}
 * lines under the shared metric contract.
 */
public class BenchFeed {

    static int envInt(String name, int fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : Integer.parseInt(value.trim());
    }

    static List<URI> endpoints() {
        String raw = System.getenv("WHITEWATER_ENDPOINTS");
        if (raw == null || raw.isBlank()) {
            raw = "http://localhost:7771,http://localhost:7772,http://localhost:7773";
        }
        return Arrays.stream(raw.split("[,;]"))
                .map(String::trim)
                .filter(item -> !item.isEmpty())
                .map(item -> item.contains("://") ? item : "http://" + item)
                .map(URI::create)
                .toList();
    }

    /** Shared JSON result contract with the Python/Kafka drivers. */
    static String resultJson(
            String mode, long records, int bytesPerRecord, double seconds,
            List<Long> latencyNanos, int writers, int batch) {
        double recordsPerSec = seconds > 0 ? records / seconds : 0;
        double mibPerSec = seconds > 0 ? records * bytesPerRecord / seconds / 1048576.0 : 0;
        var json = new StringBuilder(String.format(
                java.util.Locale.ROOT,
                "{\"system\":\"whitewater-java\",\"mode\":\"%s\",\"records\":%d,"
                        + "\"bytes_per_record\":%d,\"seconds\":%.3f,"
                        + "\"records_per_sec\":%.1f,\"mib_per_sec\":%.2f",
                mode, records, bytesPerRecord, seconds, recordsPerSec, mibPerSec));
        if (!latencyNanos.isEmpty()) {
            var ordered = new ArrayList<>(latencyNanos);
            Collections.sort(ordered);
            java.util.function.DoubleFunction<Double> pct = q ->
                    ordered.get((int) Math.min(ordered.size() - 1,
                            Math.max(0, Math.round(q * (ordered.size() - 1))))) / 1e6;
            json.append(String.format(
                    java.util.Locale.ROOT,
                    ",\"count\":%d,\"min_ms\":%.3f,\"p50_ms\":%.3f,\"p95_ms\":%.3f,"
                            + "\"p99_ms\":%.3f,\"max_ms\":%.3f",
                    ordered.size(), ordered.get(0) / 1e6, pct.apply(0.50),
                    pct.apply(0.95), pct.apply(0.99),
                    ordered.get(ordered.size() - 1) / 1e6));
        }
        if (writers > 0) {
            json.append(String.format(java.util.Locale.ROOT,
                    ",\"writers\":%d,\"batch\":%d", writers, batch));
        }
        return json.append('}').toString();
    }

    public static void main(String[] args) throws Exception {
        int records = envInt("BENCH_RECORDS", 2000);
        int bytes = envInt("BENCH_BYTES", 1024);
        int writers = envInt("BENCH_WRITERS", 4);
        int batch = Math.max(1, envInt("BENCH_BATCH", 50));
        String mode = System.getenv().getOrDefault("BENCH_MODE", "all");

        var client = WhitewaterClient.builder()
                .endpoints(endpoints().toArray(URI[]::new))
                .apiKey(System.getenv().getOrDefault(
                        "WHITEWATER_API_KEY", "whitewater-local-development-admin-key"))
                .build();

        String suffix = String.valueOf(System.currentTimeMillis());
        String domain = "jbench" + suffix.substring(suffix.length() - 10);
        String feedName = domain + ".events";
        client.createDomain(domain);
        client.createFeed(feedName);
        Feed<String, byte[]> feed = client.feed(feedName);

        byte[] payload = new byte[bytes];
        for (int i = 0; i < bytes; i++) {
            payload[i] = (byte) ('a' + i % 26);
        }

        if (mode.equals("write") || mode.equals("all")) {
            // One Writer per thread: a Writer's record sequence is ordered,
            // so concurrent producers keep disjoint shares and keyspaces.
            var latencies = new ConcurrentLinkedQueue<Long>();
            var errors = new CopyOnWriteArrayList<Exception>();
            var acknowledged = new java.util.concurrent.atomic.AtomicLong();
            var refused = new java.util.concurrent.atomic.AtomicLong();
            int attempts = envInt("BENCH_BATCH_ATTEMPTS", 8);
            var threads = new ArrayList<Thread>();
            int per = records / writers, extra = records % writers;
            long started = System.nanoTime();
            for (int w = 0; w < writers; w++) {
                int share = per + (w < extra ? 1 : 0);
                long offset = (long) w * records;
                FeedWriter<String, byte[]> writer = feed.writer("benchw" + w);
                Thread thread = new Thread(() -> {
                    for (int sent = 0; sent < share; ) {
                        int take = Math.min(batch, share - sent);
                        // Events are built once per attempt set — pinned
                        // request_ids make a retry after an ambiguous
                        // failure deduplicate server-side.
                        var events = new ArrayList<FeedEvent<String, byte[]>>(take);
                        for (int i = 0; i < take; i++) {
                            events.add(new FeedEvent<>(
                                    String.format("key-%012d", offset + sent + i), payload));
                        }
                        boolean acked = false;
                        for (int attempt = 0; attempt < attempts && !acked; attempt++) {
                            long t0 = System.nanoTime();
                            try {
                                if (take > 1) {
                                    writer.appendBatch(events);
                                } else {
                                    writer.append(events.get(0));
                                }
                                acked = true;
                            } catch (Exception error) {
                                if (attempt == attempts - 1) {
                                    errors.add(error);
                                }
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException interrupted) {
                                    Thread.currentThread().interrupt();
                                    return;
                                }
                            } finally {
                                latencies.add(System.nanoTime() - t0);
                            }
                        }
                        if (acked) {
                            acknowledged.addAndGet(take);
                        } else {
                            refused.addAndGet(take);
                        }
                        sent += take;
                    }
                });
                thread.start();
                threads.add(thread);
            }
            for (Thread thread : threads) {
                thread.join();
            }
            double seconds = (System.nanoTime() - started) / 1e9;
            long ackedRecords = acknowledged.get();
            String json = resultJson("write", ackedRecords, bytes, seconds,
                    new ArrayList<>(latencies), writers, batch);
            json = json.substring(0, json.length() - 1) + String.format(
                    java.util.Locale.ROOT,
                    ",\"requested\":%d,\"refused\":%d,\"errors\":%d}",
                    records, refused.get(), errors.size());
            System.out.println("BENCH_RESULT " + json);
            System.out.printf(java.util.Locale.ROOT,
                    "  whitewater-java write: %,.0f records/s acked=%d refused=%d errors=%d%n",
                    seconds > 0 ? ackedRecords / seconds : 0,
                    ackedRecords, refused.get(), errors.size());
            for (Exception error : errors) {
                error.printStackTrace();
            }
            if (!errors.isEmpty()) {
                throw new AssertionError("append errors during benchmark");
            }
        }

        if (mode.equals("read") || mode.equals("all")) {
            long started = System.nanoTime();
            long count = 0;
            long bytesRead = 0;
            Cursor cursor = null;
            while (true) {
                var page = feed.fetch(cursor, 1000);
                if (page.records().isEmpty()) {
                    break;
                }
                count += page.records().size();
                for (var message : page.records()) {
                    bytesRead += message.value() == null ? 0 : message.value().length;
                }
                if (page.nextCursor() == null || page.nextCursor().equals(cursor)) {
                    break;
                }
                cursor = page.nextCursor();
            }
            double seconds = (System.nanoTime() - started) / 1e9;
            int perRecord = count > 0 ? (int) (bytesRead / count) : bytes;
            String json = resultJson("read", count, perRecord, seconds,
                    List.of(), 0, 0);
            System.out.println("BENCH_RESULT " + json);
            System.out.printf(java.util.Locale.ROOT,
                    "  whitewater-java read: %,.0f records/s%n",
                    seconds > 0 ? count / seconds : 0);
        }

        System.out.println("BENCH PASSED");
    }
}
