package dev.whitewater;

import com.fasterxml.jackson.databind.JsonNode;
import dev.whitewater.internal.Transport;
import dev.whitewater.internal.TransportTestAccess;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ErrorMappingTest {

    private WhitewaterException map(int status, String body) throws Exception {
        JsonNode node = Transport.json().readTree(body);
        return TransportTestAccess.mapError(status, node);
    }

    @Test
    void wireCodesMapToTypedExceptions() throws Exception {
        var stale = map(409, "{\"error\":\"epoch superseded\",\"code\":\"stale_epoch\"}");
        assertInstanceOf(WhitewaterException.StaleSession.class, stale);
        assertEquals("stale_epoch", stale.code());

        var busy = map(409, "{\"error\":\"work claimed\",\"code\":\"lease.busy\"}");
        assertInstanceOf(WhitewaterException.LeaseConflict.class, busy);
        assertEquals("lease.busy", busy.code());

        var staleLease = map(409, "{\"error\":\"lease gone\",\"code\":\"lease.stale_lease\"}");
        assertInstanceOf(WhitewaterException.LeaseConflict.class, staleLease);

        var capacity = map(409, "{\"error\":\"cap\",\"code\":\"lease.capacity\"}");
        assertInstanceOf(WhitewaterException.LeaseConflict.class, capacity);
    }

    @Test
    void statusClassIsTheFallbackWithoutACode() throws Exception {
        assertInstanceOf(WhitewaterException.Conflict.class,
                map(409, "{\"error\":\"fencing conflict\"}"));
        assertInstanceOf(WhitewaterException.Unavailable.class,
                map(503, "{\"error\":\"frozen for split cutover; retry safely\"}"));
        assertInstanceOf(WhitewaterException.AmbiguousResult.class,
                map(500, "{\"error\":\"internal\"}"));
        assertInstanceOf(WhitewaterException.Refused.class,
                map(400, "{\"error\":\"bad name\"}"));
    }

    @Test
    void staleSequenceCodeStaysAConflictForRawAppends() throws Exception {
        var error = map(409, "{\"error\":\"seq hole\",\"code\":\"writer.stale_sequence\"}");
        assertInstanceOf(WhitewaterException.Conflict.class, error);
        assertEquals("writer.stale_sequence", error.code());
    }
}
