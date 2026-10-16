package dev.whitewater.codec;

import dev.whitewater.model.Codec;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** Avro codec roundtrips — generic, specific, and reflect paths. */
class AvroCodecsTest {

    private static final Schema POINT_SCHEMA = new Schema.Parser().parse("""
            {"type": "record", "name": "Pt",
             "fields": [{"name": "x", "type": "int"}]}
            """);

    @Test
    void genericRecordsRoundTripOverBinaryWire() {
        Codec<GenericRecord> codec = AvroCodecs.generic(POINT_SCHEMA);
        var record = new GenericData.Record(POINT_SCHEMA);
        record.put("x", 7);
        byte[] wire = codec.encode(record);
        assertTrue(wire.length < 8, "binary Avro stays compact, was " + wire.length);
        assertEquals(record, codec.decode(wire));
    }

    @Test
    void specificRecordsRoundTripGeneratedClasses() {
        Codec<Pt> codec = AvroCodecs.specific(Pt.class);
        var point = new Pt();
        point.put(0, 41);
        Pt back = codec.decode(codec.encode(point));
        assertEquals(41, back.get(0));
    }

    @Test
    void reflectMapsPlainJavaTypesWithoutCodegen() {
        Codec<Pojo> codec = AvroCodecs.reflect(Pojo.class);
        var pojo = new Pojo();
        pojo.x = 1;
        assertEquals(1, codec.decode(codec.encode(pojo)).x);
    }

    @Test
    void truncatedBytesFailAsIllegalArgument() {
        Codec<GenericRecord> codec = AvroCodecs.generic(POINT_SCHEMA);
        var record = new GenericData.Record(POINT_SCHEMA);
        record.put("x", 7);
        byte[] wire = codec.encode(record);
        byte[] truncated = java.util.Arrays.copyOf(wire, Math.max(0, wire.length - 1));
        assertThrows(IllegalArgumentException.class, () -> codec.decode(truncated));
    }

    /** Plain POJO for the reflect path (records defeat ReflectData's offsets). */
    public static class Pojo {
        public int x;
    }

}
