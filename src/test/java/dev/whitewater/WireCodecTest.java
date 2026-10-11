package dev.whitewater;

import com.fasterxml.jackson.databind.JsonNode;
import dev.whitewater.internal.Transport;
import dev.whitewater.internal.Wire;
import dev.whitewater.model.Cursor;
import dev.whitewater.model.FeedRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class WireCodecTest {

    @Test
    void appendRequestEncodesScalarsOnTheWire() {
        UUID requestId = UUID.fromString("019a0000-0000-7000-8000-000000000001");
        JsonNode node = Wire.appendRequest(
                requestId, "orders.wr", 7,
                "key-1".getBytes(), "payload".getBytes(),
                -9223372036854775808L, Map.of("source", "unit".getBytes()));
        assertEquals(requestId.toString(), node.get("request_id").asText());
        assertEquals("orders.wr", node.get("writer").asText());
        assertEquals(7, node.get("session_epoch").asLong());
        assertEquals("a2V5LTE=", node.get("key_base64").asText());
        assertEquals("cGF5bG9hZA==", node.get("payload_base64").asText());
        // i64 nanoseconds serialize as a decimal string, never a JSON number
        assertEquals("-9223372036854775808", node.get("event_time_ns").asText());
        assertEquals("dW5pdA==", node.get("metadata_base64").get("source").asText());
    }

    @Test
    void recordDecodesOpaqueCursorAndNanosecondStrings() throws Exception {
        JsonNode node = Transport.json().readTree("""
                {
                  "cursor": "AAAABBBBCCCCDDDD",
                  "message_id": "019a0000-0000-7000-8000-000000000002",
                  "producer_id": "019a0000-0000-7000-8000-000000000003",
                  "sequence": 42,
                  "event_time_ns": "9223372036854775807",
                  "ingest_time_ns": "-5",
                  "key_base64": "3q2+7w==",
                  "payload_base64": "AAEC",
                  "metadata_base64": {"m": "eA=="}
                }
                """);
        FeedRecord record = Wire.record(node);
        assertEquals("AAAABBBBCCCCDDDD", record.cursor().value());
        assertEquals(Long.MAX_VALUE, record.eventTimeNs());
        assertEquals(-5L, record.ingestTimeNs());
        assertArrayEquals(new byte[]{(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef}, record.key());
        assertArrayEquals(new byte[]{0, 1, 2}, record.payload());
        assertArrayEquals("x".getBytes(), record.metadata().get("m"));
    }

    @Test
    void cursorIsOpaqueAndNeverParsed() {
        String opaque = "TSLWCDHsmGBMXtKGSBS3vfBmPrsJnt1cYxWFYi4TpTA";
        Cursor cursor = Cursor.of(opaque);
        assertEquals(opaque, cursor.value());
        assertEquals(opaque, cursor.toString());
        assertNull(Cursor.of(null));
        assertThrows(IllegalArgumentException.class, () -> new Cursor(""));
    }

    @Test
    void feedbackDecodesHotKeyFlag() throws Exception {
        JsonNode node = Transport.json().readTree("""
                {
                  "recommended_batch_count": 8,
                  "recommended_batch_bytes": 65536,
                  "pressure": 0.9,
                  "retry_after_ms": 50,
                  "max_frame_bytes": 67108864,
                  "hot_key": true
                }
                """);
        var feedback = Wire.feedback(node);
        assertTrue(feedback.hotKey());
        assertEquals(8, feedback.recommendedBatchCount());
        // Older servers omit the field; default is false.
        node = Transport.json().readTree("""
                {"recommended_batch_count": 100, "recommended_batch_bytes": 1048576,
                 "pressure": 0.0, "retry_after_ms": 0, "max_frame_bytes": 67108864}
                """);
        assertFalse(Wire.feedback(node).hotKey());
    }
}
