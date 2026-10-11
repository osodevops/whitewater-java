package dev.whitewater.internal;

import com.fasterxml.jackson.databind.JsonNode;
import dev.whitewater.WhitewaterException;

/** Exposes the package-private error mapping to tests in other packages. */
public final class TransportTestAccess {
    private TransportTestAccess() {}

    public static WhitewaterException mapError(int status, JsonNode body) {
        return Transport.mapError(status, body);
    }
}
