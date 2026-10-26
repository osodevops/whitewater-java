package dev.whitewater;

import dev.whitewater.model.FeedMessage;
import dev.whitewater.model.ReaderStart;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * A permanent, supervised consumer: the durable-Reader equivalent of
 * Kafka's {@code while (true) consumer.poll(...)} loop, with the
 * plumbing removed.
 *
 * <pre>{@code
 * var consumer = feed.consumer("enricher")
 *         .onRecord(msg -> process(msg));
 * Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));
 * consumer.run();   // blocks until close()
 * }</pre>
 *
 * <p>Semantics:
 * <ul>
 *   <li>The named Reader's acked cursor is durable server-side — a
 *       restart (app or session) resumes where it stopped, never
 *       re-managing offsets.</li>
 *   <li>Each fetched page is delivered record-by-record to
 *       {@link #onRecord}; the page's delivered cursor is acknowledged
 *       only after the handler returns — a handler exception leaves the
 *       page unacked, so it redelivers (at-least-once).</li>
 *   <li>Transient failures retry with exponential backoff inside the
 *       supervisor; a dead session re-opens and resumes. {@link #run}
 *       only throws for first-attempt failures (misconfiguration,
 *       refused auth) — everything after a successful first open is
 *       supervised forever until {@link #close}.</li>
 *   <li>{@link #close} finishes the in-flight page, acknowledges it,
 *       closes the session, and lets {@link #run} return — the
 *       SIGTERM/graceful-shutdown path.</li>
 * </ul>
 */
public final class FeedConsumer<K, V> implements AutoCloseable {

    private static final long POLL_MS = 50;

    private final Feed<K, V> feed;
    private final String readerName;

    private ReaderStart start = ReaderStart.beginning();
    private int maxPageSize = 128;
    private Duration initialBackoff = Duration.ofMillis(250);
    private Duration maxBackoff = Duration.ofSeconds(30);
    private Consumer<FeedMessage<K, V>> onRecord;
    private Consumer<Exception> onError = e -> { };

    private volatile boolean running;
    private volatile Subscription current;
    private volatile Exception lastError;
    private volatile Thread runThread;

    FeedConsumer(Feed<K, V> feed, String readerName) {
        this.feed = feed;
        this.readerName = readerName;
    }

    /** The declared Reader's name — its durable identity. */
    public String reader() {
        return readerName;
    }

    /** Where the Reader starts on first declaration (default BEGINNING).
     * Ignored once the Reader exists — it resumes from durable progress. */
    public FeedConsumer<K, V> startAt(ReaderStart start) {
        this.start = Objects.requireNonNull(start);
        return this;
    }

    /** Max records delivered per page (default 128). */
    public FeedConsumer<K, V> maxPageSize(int maxPageSize) {
        this.maxPageSize = maxPageSize;
        return this;
    }

    /** The per-record handler — required. A thrown exception leaves the
     * page unacked so it redelivers; poison pills are the app's job
     * (catch them and route to a DLQ feed). */
    public FeedConsumer<K, V> onRecord(Consumer<FeedMessage<K, V>> handler) {
        this.onRecord = Objects.requireNonNull(handler);
        return this;
    }

    /** Error sink for visibility — plumbing failures and handler
     * exceptions alike. The loop continues regardless; stopping is only
     * via {@link #close}. */
    public FeedConsumer<K, V> onError(Consumer<Exception> handler) {
        this.onError = Objects.requireNonNull(handler);
        return this;
    }

    /** Restart backoff bounds (default 250ms -> 30s, doubling). */
    public FeedConsumer<K, V> backoff(Duration initial, Duration max) {
        this.initialBackoff = Objects.requireNonNull(initial);
        this.maxBackoff = Objects.requireNonNull(max);
        return this;
    }

    /** The last supervised failure (plumbing or handler), for
     * diagnostics. */
    public Exception lastError() {
        return lastError;
    }

    /** True while the supervise loop is active. */
    public boolean isRunning() {
        return running;
    }

    /**
     * Run the consumer on the calling thread until {@link #close}.
     * Throws only when the first open fails (misconfiguration, refused
     * auth) — Kafka-style fail-fast on startup.
     */
    public void run() {
        Objects.requireNonNull(onRecord, "onRecord handler is required");
        runThread = Thread.currentThread();
        running = true;
        long backoffMs = initialBackoff.toMillis();
        try {
            while (running) {
                Consumer<FeedMessage<K, V>> handler = onRecord;
                Subscription sub;
                try {
                    sub = feed.subscribeTyped(
                            readerName, start, maxPageSize,
                            records -> {
                                for (var record : records) {
                                    try {
                                        handler.accept(record);
                                    } catch (Exception handlerError) {
                                        lastError = handlerError;
                                        throw handlerError;
                                    }
                                }
                            },
                            error -> { /* terminal plumbing error → supervised below */ });
                } catch (WhitewaterException.Refused e) {
                    if (!isTransientRefusal(e)) {
                        throw e;        // refused = fix the request — fail fast
                    }
                    if (!running) {
                        break;
                    }
                    report(e);
                    sleep(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, maxBackoff.toMillis());
                    continue;
                } catch (WhitewaterException e) {
                    // A transient Control Plane window (quorum timeout,
                    // leader election) can fault Reader declaration before
                    // the subscription exists — same supervision as a
                    // mid-stream plumbing failure.
                    if (!running) {
                        break;
                    }
                    report(e);
                    sleep(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, maxBackoff.toMillis());
                    continue;
                }
                current = sub;
                awaitTermination(sub);
                current = null;
                if (!running) {
                    break;
                }
                var failure = sub.failure();
                if (failure instanceof WhitewaterException.Refused refused
                        && !isTransientRefusal(refused)) {
                    throw failure;      // refused = fix the request — fail fast
                }
                report(failure);
                sleep(backoffMs);
                backoffMs = Math.min(backoffMs * 2, maxBackoff.toMillis());
            }
        } finally {
            running = false;
            if (current != null) {
                current.close();
                current = null;
            }
            runThread = null;
        }
    }

    /** The same supervised consumer on its own daemon-free thread —
     * for running several consumers or non-blocking startup. */
    public Thread runAsync() {
        Thread thread = new Thread(this::run, "whitewater-consumer-" + readerName);
        thread.setDaemon(false);
        thread.start();
        return thread;
    }

    /** Graceful stop: the in-flight page finishes and acks, the session
     * closes, {@link #run} returns. Safe to call from a shutdown hook. */
    @Override
    public void close() {
        running = false;
        var thread = runThread;
        if (thread != null) {
            thread.interrupt();
        }
        var sub = current;
        if (sub != null) {
            sub.close();
        }
    }

    private void awaitTermination(Subscription sub) {
        while (running && sub.isRunning()) {
            sleep(POLL_MS);
        }
    }

    /**
     * The server can surface a transient quorum/election window — or a
     * fenced session epoch that reopening heals — as a refusal on some
     * command paths. Distinguishable from a real request fault only by
     * the message until error codes land on those paths.
     */
    private static boolean isTransientRefusal(WhitewaterException.Refused e) {
        String message = String.valueOf(e.getMessage()).toLowerCase();
        return message.contains("timed out") || message.contains("quorum")
                || message.contains("unavailable") || message.contains("election")
                || message.contains("stale");
    }

    private void report(WhitewaterException failure) {
        var error = lastError;
        if (failure != null) {
            error = failure;
            lastError = failure;
        }
        if (error != null) {
            try {
                onError.accept(error);
            } catch (Exception ignored) {
                // Error sinks must never break supervision.
            }
        }
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            running = false;
        }
    }
}
