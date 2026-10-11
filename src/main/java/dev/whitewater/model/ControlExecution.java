package dev.whitewater.model;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.UUID;

/** Result of a WCL script or typed command batch. */
public record ControlExecution(
        UUID requestId,
        long revision,
        String authority,
        String warning,
        List<StatementResult> results) {

    /** One executed statement. */
    public record StatementResult(String statement, String message, JsonNode data) {}
}
