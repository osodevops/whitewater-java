package dev.whitewater;

import dev.whitewater.internal.Wire;
import dev.whitewater.model.AppendResult;
import dev.whitewater.model.Codec;
import dev.whitewater.model.FeedEvent;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * A session-bound Writer with the feed's {@link Codec}s applied — the
 * {@code session_epoch} and per-record {@code request_id} identities
 * stay inside the handle, and keys/payloads serialize automatically:
 *
 * <pre>{@code
 * FeedWriter<String, Order> writer = orders.writer("orders.ingest");
 * writer.write("order-1", order, (result, err) -> ack(result.messageId()));
 * writer.append("order-2", order2);                 // blocking, encoded
 * writer.append(new FeedEvent<>("order-3", order3)
 *         .withTextMetadata(Map.of("tenant", "acme")));
 * }</pre>
 *
 * <p>{@link #write} is the default produce path — always asynchronous:
 * each call returns a {@link CompletableFuture} that completes with the
 * record's {@link AppendResult} once the commit lands, and a daemon
 * sender coalesces queued records into {@code append-batch} commits by
 * {@code linger}/{@code maxBatchRecords} so callers never think about
 * batching. {@link #append}/{@link #appendBatch} remain the manual
 * forms for synchronous writes and explicit commit groups.
 *
 * <p>Retry an ambiguous append by repeating the same call after
 * {@link WhitewaterException.AmbiguousResult}/{@link WhitewaterException.Unavailable};
 * pin a request identity with {@link FeedEvent#FeedEvent(UUID, Object, Object)}
 * to keep retries deduplicating. On {@link WhitewaterException.StaleSession}
 * the session was fenced (another client opened the Writer); call
 * {@link #reopen()} to bind a fresh epoch. {@link #close()} flushes
 * queued writes before returning — use try-with-resources.
 */
public final class FeedWriter<K, V> implements AutoCloseable {

    /** Default coalescing window for {@link #write}. */
    public static final Duration DEFAULT_LINGER = Duration.ofMillis(5);
    /** Default records-per-commit ceiling for {@link #write}. */
    public static final int DEFAULT_MAX_BATCH_RECORDS = 256;

    private static final long RETRY_BACKOFF_MS = 100;
    private static final long CLOSE_JOIN_MS = 10_000;

    private record Pending(Writer.Record record, CompletableFuture<AppendResult> future) {}

    private final Writer writer;
    private final Codec<K> keyCodec;
    private final Codec<V> valueCodec;
    private final long lingerNs;
    private final int maxBatch;
    private final BlockingQueue<Pending> pending = new LinkedBlockingQueue<>();
    private final Thread sender;
    private volatile boolean closed;

    FeedWriter(Writer writer, Codec<K> keyCodec, Codec<V> valueCodec) {
        this(writer, keyCodec, valueCodec, DEFAULT_LINGER, DEFAULT_MAX_BATCH_RECORDS);
    }

    FeedWriter(Writer writer, Codec<K> keyCodec, Codec<V> valueCodec,
               Duration linger, int maxBatchRecords) {
        this.writer = Objects.requireNonNull(writer);
        this.keyCodec = Objects.requireNonNull(keyCodec, "keyCodec");
        this.valueCodec = Objects.requireNonNull(valueCodec, "valueCodec");
        this.lingerNs = Math.max(0, Objects.requireNonNull(linger, "linger").toNanos());
        this.maxBatch = maxBatchRecords;
        this.sender = new Thread(this::run, "whitewater-writer-" + writer.writer());
        this.sender.setDaemon(true);
        this.sender.start();
    }

    /** The bound Writer's name. */
    public String writer() {
        return writer.writer();
    }

    /** The session's fencing epoch — 0 until the session binds on first write. */
    public long sessionEpoch() {
        return writer.sessionEpoch();
    }

    /** Reopen after {@link WhitewaterException.StaleSession}; binds a fresh epoch. */
    public synchronized void reopen() {
        writer.reopen();
    }

    /** Enable wire compression on subsequent appends. */
    public FeedWriter<K, V> useCompression() {
        writer.useCompression();
        return this;
    }

    /** The underlying wire-level Writer — escape hatch for raw bytes. */
    public Writer raw() {
        return writer;
    }

    /** Records queued for delivery by the next coalesced commit. */
    public int pending() {
        return pending.size();
    }

    // ---- the pipelined produce path --------------------------------------

    /**
     * Enqueue a record for the next coalesced commit — returns
     * immediately; the future completes with the record's
     * {@link AppendResult} once the Riverbed commits it
     * ({@code durability()} reports the commit level).
     */
    public CompletableFuture<AppendResult> write(K key, V value) {
        return write(new FeedEvent<>(key, value));
    }

    /** {@link #write(Object, Object)} with Metadata. */
    public CompletableFuture<AppendResult> write(K key, V value, Map<String, byte[]> metadata) {
        return write(new FeedEvent<>(null, key, value, null, metadata));
    }

    /** {@link #write(Object, Object)} with full event control. */
    public CompletableFuture<AppendResult> write(FeedEvent<K, V> event) {
        var future = new CompletableFuture<AppendResult>();
        if (closed) {
            future.completeExceptionally(new IllegalStateException("writer is closed"));
            return future;
        }
        pending.add(new Pending(toWire(event), future));
        return future;
    }

    /** {@link #write(Object, Object)} with a completion callback —
     * {@code write(k, v, result -> ack(result.messageId()))}. Errors
     * still surface on the returned future. */
    public CompletableFuture<AppendResult> write(K key, V value, Consumer<AppendResult> callback) {
        var future = write(key, value);
        return future.whenComplete((result, error) -> {
            if (error == null) {
                callback.accept(result);
            }
        });
    }

    /** {@link #write(Object, Object)} with a result-or-error callback —
     * {@code write(k, v, (result, err) -> ...)}. */
    public CompletableFuture<AppendResult> write(K key, V value,
                                               BiConsumer<AppendResult, Throwable> callback) {
        var future = write(key, value);
        return future.whenComplete(callback);
    }

    /** {@link #write(FeedEvent)} with a result-or-error callback. */
    public CompletableFuture<AppendResult> write(FeedEvent<K, V> event,
                                               BiConsumer<AppendResult, Throwable> callback) {
        var future = write(event);
        return future.whenComplete(callback);
    }

    // ---- the manual forms ------------------------------------------------

    /** Append one record; codecs serialize key/payload, identity auto-generates. */
    public AppendResult append(K key, V value) {
        return append(new FeedEvent<>(key, value));
    }

    /** {@link #append(Object, Object)} with Metadata. */
    public AppendResult append(K key, V value, Map<String, byte[]> metadata) {
        return append(new FeedEvent<>(null, key, value, null, metadata));
    }

    /** Append one record with explicit request identity (retry-safe). */
    public AppendResult append(FeedEvent<K, V> event) {
        return writer.append(toWire(event));
    }

    /** Append a batch under one request; per-record results + batching guidance. */
    public Writer.BatchResult appendBatch(List<FeedEvent<K, V>> events) {
        List<Writer.Record> records = events.stream().map(this::toWire).toList();
        return writer.appendBatch(records);
    }

    /** Alias for {@link #appendBatch(List)}. */
    public Writer.BatchResult appendAll(List<FeedEvent<K, V>> events) {
        return appendBatch(events);
    }

    public CompletableFuture<AppendResult> appendAsync(K key, V value) {
        return CompletableFuture.supplyAsync(() -> append(key, value), writer.executor());
    }

    /** Flush queued {@link #write}s and stop the sender thread. */
    @Override
    public void close() {
        closed = true;
        sender.interrupt();
        try {
            sender.join(CLOSE_JOIN_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (sender.isAlive()) {
            var stuck = new IllegalStateException("writer closed before delivery");
            Pending p;
            while ((p = pending.poll()) != null) {
                p.future().completeExceptionally(stuck);
            }
        }
    }

    // ---- sender ----------------------------------------------------------

    private void run() {
        var batch = new ArrayList<Pending>(maxBatch);
        while (true) {
            try {
                Pending first = closed ? pending.poll() : pending.take();
                if (first == null) {
                    return;         // closed and drained
                }
                batch.add(first);
                pending.drainTo(batch, maxBatch - 1);
                long deadline = System.nanoTime() + lingerNs;
                while (!closed && batch.size() < maxBatch) {
                    long remain = deadline - System.nanoTime();
                    if (remain <= 0) {
                        break;
                    }
                    Pending next = pending.poll(remain, TimeUnit.NANOSECONDS);
                    if (next == null) {
                        break;
                    }
                    batch.add(next);
                }
                deliver(batch);
                batch.clear();
            } catch (InterruptedException wake) {
                // The flag is already consumed; clear any second pending
                // interrupt so the flush's HTTP call doesn't die on it.
                Thread.interrupted();
                pending.drainTo(batch);
                if (!batch.isEmpty()) {
                    deliver(batch);
                    batch.clear();
                }
                if (closed) {
                    return;
                }
            }
        }
    }

    private void deliver(List<Pending> batch) {
        List<Writer.Record> records = new ArrayList<>(batch.size());
        for (Pending p : batch) {
            records.add(p.record());
        }
        Writer.BatchResult result = null;
        Throwable failure = null;
        for (int attempt = 0; ; attempt++) {
            try {
                result = writer.appendBatch(records);
                failure = null;
                break;
            } catch (WhitewaterException.StaleSession e) {
                failure = e;
                try {
                    writer.reopen();    // requestIds are stable — resubmit dedups
                } catch (RuntimeException rebind) {
                    failure = rebind;
                    if (attempt >= 2) break;
                    sleepQuiet();
                }
            } catch (WhitewaterException.Unavailable | WhitewaterException.AmbiguousResult e) {
                failure = e;
                if (attempt >= 2) break;
                sleepQuiet();
            } catch (WhitewaterException e) {
                failure = e;            // definitive refusal — no retry
                break;
            } catch (RuntimeException e) {
                failure = e;
                break;
            }
        }
        if (failure != null || result == null
                || result.results().size() != batch.size()) {
            var error = failure != null ? failure : new IllegalStateException(
                    "batch result did not cover every record");
            for (Pending p : batch) {
                p.future().completeExceptionally(error);
            }
            return;
        }
        var results = result.results();
        for (int i = 0; i < batch.size(); i++) {
            batch.get(i).future().complete(results.get(i));
        }
    }

    private void sleepQuiet() {
        try {
            Thread.sleep(RETRY_BACKOFF_MS);
        } catch (InterruptedException ignored) {
            // A close() interrupt during a retry pause: the flush
            // continues — re-arming the flag would kill the HTTP call.
        }
    }

    private Writer.Record toWire(FeedEvent<K, V> event) {
        return new Writer.Record(
                event.requestId(),
                event.key() == null ? null : keyCodec.encode(event.key()),
                event.value() == null ? null : valueCodec.encode(event.value()),
                event.eventTimeNs(),
                event.metadata());
    }
}
