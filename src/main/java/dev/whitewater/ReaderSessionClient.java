package dev.whitewater;

import com.fasterxml.jackson.databind.JsonNode;
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
 * One named-Reader session. Fetch returns a bounded page; acknowledge the
 * page's {@code deliveredCursor} to durably commit progress. Reopening the
 * same named Reader resumes at the last acknowledged position — delivery
 * is at-least-once, so consumers must tolerate replays between delivered
 * and acknowledged.
 */
public final class ReaderSessionClient {

    private final Transport transport;
    private final Executor executor;
    private final String reader;

    ReaderSessionClient(Transport transport, Executor executor, String reader) {
        this.transport = transport;
        this.executor = executor;
        this.reader = Objects.requireNonNull(reader);
    }

    /** Open a session; returns the fencing epoch plus durable progress. */
    public ReaderSession open(int capacity) {
        return open(capacity, UUID.randomUUID());
    }

    public ReaderSession open(int capacity, UUID requestId) {
        var body = Transport.json().createObjectNode();
        body.put("request_id", requestId.toString());
        body.put("reader", reader);
        body.put("capacity", capacity);
        return Wire.readerSession(transport.post("/v1/readers/open", body));
    }

    /** Fetch up to {@code limit} committed records after the frontier. */
    public ReaderPage fetch(ReaderSession session, Integer limit) {
        return fetch(session, limit, UUID.randomUUID());
    }

    public ReaderPage fetch(ReaderSession session, Integer limit, UUID requestId) {
        var body = Transport.json().createObjectNode();
        body.put("request_id", requestId.toString());
        body.put("reader", reader);
        body.put("session_epoch", session.sessionEpoch());
        if (limit != null) {
            body.put("limit", limit);
        }
        return Wire.readerPage(transport.post("/v1/readers/fetch", body));
    }

    /** Durably commit progress at a page's delivered cursor. */
    public ReaderSession ack(ReaderSession session, Cursor deliveredCursor) {
        return ack(session, deliveredCursor, UUID.randomUUID());
    }

    public ReaderSession ack(ReaderSession session, Cursor deliveredCursor, UUID requestId) {
        var body = Transport.json().createObjectNode();
        body.put("request_id", requestId.toString());
        body.put("reader", reader);
        body.put("session_epoch", session.sessionEpoch());
        body.put("cursor", deliveredCursor.value());
        return Wire.readerSession(transport.post("/v1/readers/ack", body));
    }

    /** Close the session; durable progress is unaffected. */
    public ReaderSession close(ReaderSession session) {
        var body = Transport.json().createObjectNode();
        body.put("request_id", UUID.randomUUID().toString());
        body.put("reader", reader);
        body.put("session_epoch", session.sessionEpoch());
        return Wire.readerSession(transport.post("/v1/readers/close", body));
    }

    public CompletableFuture<ReaderPage> fetchAsync(ReaderSession session, Integer limit) {
        return CompletableFuture.supplyAsync(() -> fetch(session, limit), executor);
    }
}
