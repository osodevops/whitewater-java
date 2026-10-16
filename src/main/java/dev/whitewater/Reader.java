package dev.whitewater;

import dev.whitewater.internal.Transport;
import dev.whitewater.internal.Wire;
import dev.whitewater.model.Cursor;
import dev.whitewater.model.ReaderPage;
import dev.whitewater.model.ReaderSession;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A session-bound named Reader: the session epoch and current delivered
 * Cursor stay inside the handle. {@link #fetch} returns a bounded page;
 * {@link #ack} commits the page's delivered Cursor durably.
 *
 * <pre>{@code
 * try (var reader = client.reader("orders.audit", 64)) {
 *     for (var page = reader.fetch(); !page.records().isEmpty(); page = reader.fetch()) {
 *         handle(page.records());
 *         reader.ack();                // commits the page's frontier
 *     }
 * }
 * }</pre>
 *
 * <p>Delivery is at-least-once: a replay between delivered and
 * acknowledged re-delivers the overlap. Reopening the named Reader
 * resumes at the last acknowledged position.
 */
public final class Reader implements AutoCloseable {

    private final Transport transport;
    private final Executor executor;
    private final String reader;
    private volatile ReaderSession session;
    private volatile ReaderPage lastPage;
    private volatile boolean closed;

    Reader(Transport transport, Executor executor, String reader) {
        this.transport = transport;
        this.executor = executor;
        this.reader = Objects.requireNonNull(reader);
    }

    /** Open the session with the bounded fetch capacity. */
    Reader open(int capacity) {
        var body = Transport.json().createObjectNode();
        body.put("request_id", UUID.randomUUID().toString());
        body.put("reader", reader);
        body.put("capacity", capacity);
        this.session = Wire.readerSession(transport.post("/v1/readers/open", body));
        return this;
    }

    public String reader() {
        return reader;
    }

    Executor executor() {
        return executor;
    }

    /** The session's fencing epoch. */
    public long sessionEpoch() {
        return session == null ? -1 : session.sessionEpoch();
    }

    /** The Cursor the last {@link #ack} durably committed, or null. */
    public Cursor acknowledgedCursor() {
        return session == null ? null : session.acknowledgedCursor();
    }

    /** Fetch up to {@code limit} committed records after the frontier. */
    public ReaderPage fetch(Integer limit) {
        ensureOpen();
        var body = Transport.json().createObjectNode();
        body.put("request_id", UUID.randomUUID().toString());
        body.put("reader", reader);
        body.put("session_epoch", session.sessionEpoch());
        if (limit != null) {
            body.put("limit", limit);
        }
        ReaderPage page = Wire.readerPage(transport.post("/v1/readers/fetch", body));
        this.lastPage = page;
        return page;
    }

    /** Fetch with the session's configured capacity. */
    public ReaderPage fetch() {
        return fetch(null);
    }

    /**
     * Durably commit the last fetched page's delivered Cursor. Call after
     * processing a page; anything unacknowledged may replay.
     */
    public ReaderSession ack() {
        ensureOpen();
        if (lastPage == null || lastPage.deliveredCursor() == null) {
            return session;
        }
        return ack(lastPage.deliveredCursor());
    }

    /** Durably commit an explicit Cursor. */
    public ReaderSession ack(Cursor cursor) {
        ensureOpen();
        var body = Transport.json().createObjectNode();
        body.put("request_id", UUID.randomUUID().toString());
        body.put("reader", reader);
        body.put("session_epoch", session.sessionEpoch());
        body.put("cursor", cursor.value());
        this.session = Wire.readerSession(transport.post("/v1/readers/ack", body));
        return session;
    }

    /** Close the session; durable acknowledged progress survives. */
    @Override
    public void close() {
        if (closed || session == null) {
            return;
        }
        closed = true;
        var body = Transport.json().createObjectNode();
        body.put("request_id", UUID.randomUUID().toString());
        body.put("reader", reader);
        body.put("session_epoch", session.sessionEpoch());
        this.session = Wire.readerSession(transport.post("/v1/readers/close", body));
    }

    private void ensureOpen() {
        if (session == null) {
            throw new IllegalStateException("Reader " + reader + " is not open");
        }
        if (closed) {
            throw new IllegalStateException("Reader " + reader + " is closed");
        }
    }

    public CompletableFuture<ReaderPage> fetchAsync(Integer limit) {
        return CompletableFuture.supplyAsync(() -> fetch(limit), executor);
    }
}
