package dev.whitewater;

import com.fasterxml.jackson.databind.JsonNode;
import dev.whitewater.admin.Commands;
import dev.whitewater.model.PipeInfo;
import dev.whitewater.model.ResourceKind;
import dev.whitewater.model.ShowKind;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A declared Pipe's lifecycle surface. Pausing is cooperative — an
 * in-flight effect finishes and the frontier stays put so redelivery
 * stays replay-safe — and resume restarts driving from the same durable
 * frontier.
 *
 * <pre>{@code
 * var relay = client.pipe("orders.relay");
 * relay.pause();                 // cooperative, idempotent
 * relay.resume();                // restarts from the durable frontier
 * PipeInfo info = relay.describe(); // live catalog row
 * }</pre>
 */
public final class Pipe {

    private final WhitewaterClient client;
    private final String name;

    private Pipe(WhitewaterClient client, String name) {
        this.client = client;
        this.name = name;
    }

    /** Bind a declared Pipe by name — no call until an operation runs. */
    public static Pipe bind(WhitewaterClient client, String name) {
        return new Pipe(Objects.requireNonNull(client, "client"),
                Objects.requireNonNull(name, "name"));
    }

    /** The declared Pipe name. */
    public String name() {
        return name;
    }

    /**
     * {@code DESCRIBE PIPE}: the live catalog row including stage and
     * the opaque operation definition.
     */
    public PipeInfo describe() {
        var execution = client.commands(
                Commands.of().describe(ResourceKind.PIPE, name));
        var row = firstData(execution.results());
        var info = PipeInfo.from(row);
        if (info == null) {
            throw new WhitewaterException(0, "protocol",
                    "describe pipe returned no definition for " + name);
        }
        return info;
    }

    /** {@code PAUSE PIPE}: halts every drive cooperatively; idempotent. */
    public void pause() {
        client.commands(Commands.of().pausePipe(name));
    }

    /** {@code RESUME PIPE}: restarts driving from the durable frontier. */
    public void resume() {
        client.commands(Commands.of().resumePipe(name));
    }

    /** {@code DROP PIPE}: retires the declaration and its compute lease. */
    public void drop() {
        client.commands(Commands.of().drop(ResourceKind.PIPE, name));
    }

    /** {@code SHOW PIPES} decoded as {@link PipeInfo} rows. */
    static List<PipeInfo> list(WhitewaterClient client) {
        var execution = client.commands(Commands.of().show(ShowKind.PIPES));
        var rows = firstData(execution.results());
        var infos = new ArrayList<PipeInfo>();
        if (rows != null && rows.isArray()) {
            rows.forEach(row -> {
                var info = PipeInfo.from(row);
                if (info != null) {
                    infos.add(info);
                }
            });
        }
        return infos;
    }

    private static JsonNode firstData(List<dev.whitewater.model.ControlExecution.StatementResult> results) {
        if (results == null || results.isEmpty()) {
            return null;
        }
        return results.get(0).data();
    }
}
