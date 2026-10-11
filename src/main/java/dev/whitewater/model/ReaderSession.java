package dev.whitewater.model;

import java.util.UUID;

/** Opened Reader session: the epoch fences every fetch/ack/close call. */
public record ReaderSession(
        UUID readerId,
        UUID feedId,
        long sessionEpoch,
        int capacity,
        Cursor deliveredCursor,
        Cursor acknowledgedCursor) {}
