package dev.whitewater;

import com.fasterxml.jackson.databind.JsonNode;
import dev.whitewater.internal.Transport;
import dev.whitewater.internal.Wire;
import dev.whitewater.model.ControlExecution;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Logical resource administration: WCL scripts and typed commands converge
 * on the Control Plane. Identical {@code requestId} retries return the
 * original committed execution.
 */
public final class AdminClient {

    private final Transport transport;
    private final Executor executor;

    AdminClient(Transport transport, Executor executor) {
        this.transport = transport;
        this.executor = executor;
    }

    /** Execute a WCL script synchronously. */
    public ControlExecution wcl(String script) {
        return wcl(script, UUID.randomUUID());
    }

    /** Execute a WCL script under an explicit request identity. */
    public ControlExecution wcl(String script, UUID requestId) {
        var body = Transport.json().createObjectNode();
        body.put("script", script);
        if (requestId != null) {
            body.put("request_id", requestId.toString());
        }
        return Wire.controlExecution(transport.post("/v1/admin/wcl", body));
    }

    /**
     * Execute typed control commands synchronously. Commands are the same
     * JSON shape the Control Plane accepts over {@code POST /v1/admin/commands},
     * e.g. {@code Map.of("command", "show", "kind", "domains")}.
     */
    public ControlExecution commands(java.util.List<java.util.Map<String, Object>> commands) {
        return commands(commands, UUID.randomUUID());
    }

    /** Execute typed commands under an explicit request identity. */
    public ControlExecution commands(java.util.List<java.util.Map<String, Object>> commands, UUID requestId) {
        var body = Transport.json().createObjectNode();
        if (requestId != null) {
            body.put("request_id", requestId.toString());
        }
        body.set("commands", Transport.json().valueToTree(commands));
        return Wire.controlExecution(transport.post("/v1/admin/commands", body));
    }

    public CompletableFuture<ControlExecution> wclAsync(String script) {
        return CompletableFuture.supplyAsync(() -> wcl(script), executor);
    }

    /** Liveness of the contacted Node. */
    public boolean healthy() {
        JsonNode health = transport.get("/health");
        return "ok".equals(health.path("status").asText());
    }
}
