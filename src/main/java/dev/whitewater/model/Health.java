package dev.whitewater.model;

/**
 * One Node's {@code /health} response: liveness plus the protocol
 * block the client uses to negotiate compatibility and the transport
 * limits frames must respect.
 */
public record Health(
        String status,
        String nodeId,
        int members,
        Protocol protocol,
        Limits limits) {

    /** The wire protocol level this Node speaks. */
    public record Protocol(int major, int minor) {}

    /** Server-advertised frame/read/lease limits. */
    public record Limits(
            long maxFrameBytes,
            long maxCommittedReadBytes,
            long maxLogicalReadFrames,
            long maxLogicalReadRanges,
            long subscriptionMemberFetchMaxLimit,
            long subscriptionMemberMaxLeaseTicks) {}
}
