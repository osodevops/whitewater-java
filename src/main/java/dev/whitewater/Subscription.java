package dev.whitewater;

import dev.whitewater.model.FeedRecord;
import dev.whitewater.model.ReaderStart;

import java.util.List;
import java.util.function.Consumer;

/**
 * A running feed subscription: a named Reader session polled on a
 * background thread, invoking {@code onRecords} per fetched page and
 * acknowledging durable progress after each delivered batch.
 *
 * <p>The thread is non-daemon, so a live Subscription keeps the JVM
 * running — close it (or use try-with-resources) to let the app exit.
 *
 * <p>Error policy: transient refusals ({@link WhitewaterException.Unavailable},
 * {@link WhitewaterException.AmbiguousResult}) retry after a short pause.
 * {@link WhitewaterException.StaleSession} — another client opened the same
 * Reader — terminates the subscription rather than fencing back. Any other
 * failure also terminates. Terminal errors reach {@code onError} before the
 * thread exits.
 */
public final class Subscription implements AutoCloseable {

    static final int DEFAULT_CAPACITY = 128;

    private static final long EMPTY_POLL_MS = 250;
    private static final long RETRY_MS = 1_000;

    private final Thread thread;
    private final Consumer<WhitewaterException> onError;
    private volatile boolean running = true;
    private volatile WhitewaterException failure;

    Subscription(
            WhitewaterClient client,
            String readerName,
            String feed,
            ReaderStart start,
            int capacity,
            Consumer<List<FeedRecord>> onRecords,
            Consumer<WhitewaterException> onError) {
        this.onError = onError;
        this.thread = new Thread(() -> run(client, readerName, feed, start, capacity, onRecords),
                "whitewater-subscription-" + readerName);
        this.thread.setDaemon(false);
    }

    void start() {
        thread.start();
    }

    /** False once the loop has exited (closed or failed). */
    public boolean isRunning() {
        return running && thread.isAlive();
    }

    /** The terminal error, when the loop stopped on a failure. */
    public WhitewaterException failure() {
        return failure;
    }

    /** Signal stop, close the Reader session, and join the thread. */
    @Override
    public void close() {
        running = false;
        thread.interrupt();
        try {
            thread.join(10_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void run(
            WhitewaterClient client,
            String readerName,
            String feed,
            ReaderStart start,
            int capacity,
            Consumer<List<FeedRecord>> onRecords) {
        try {
            ensureReader(client, readerName, feed, start);
            var reader = client.readerSession(readerName);
            var session = openWithRetry(reader, capacity);
            if (session == null) {
                return;         // stopped while waiting to open
            }
            try {
                while (running) {
                    try {
                        var page = reader.fetch(session, capacity);
                        if (!page.records().isEmpty()) {
                            onRecords.accept(page.records());
                            if (page.deliveredCursor() != null) {
                                session = reader.ack(session, page.deliveredCursor());
                            }
                        } else {
                            sleep(EMPTY_POLL_MS);
                        }
                    } catch (WhitewaterException e) {
                        if (!retryable(e)) {
                            throw e;
                        }
                        sleep(RETRY_MS);
                    }
                }
            } finally {
                // close() interrupts this thread; the interrupt flag must be
                // clear or the release call aborts before it reaches the wire.
                Thread.interrupted();
                try {
                    reader.close(session);
                } catch (WhitewaterException ignored) {
                    // Session may already be superseded; release is best-effort.
                }
            }
        } catch (WhitewaterException e) {
            if (running) {
                failure = e;
                if (onError != null) {
                    onError.accept(e);
                }
            }
        } finally {
            running = false;
        }
    }

    private dev.whitewater.model.ReaderSession openWithRetry(
            ReaderSessionClient reader, int capacity) {
        while (running) {
            try {
                return reader.open(capacity);
            } catch (WhitewaterException e) {
                if (!retryable(e)) {
                    throw e;
                }
                sleep(RETRY_MS);
            }
        }
        return null;
    }

    /** Declare the Reader when missing; an existing Reader is reused as-is. */
    private void ensureReader(
            WhitewaterClient client, String readerName, String feed, ReaderStart start) {
        try {
            client.createReader(readerName, feed, start);
        } catch (WhitewaterException.Refused e) {
            if (!String.valueOf(e.getMessage()).contains("exists")) {
                throw e;
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

    private boolean retryable(WhitewaterException e) {
        return e instanceof WhitewaterException.Unavailable
                || e instanceof WhitewaterException.AmbiguousResult;
    }
}
