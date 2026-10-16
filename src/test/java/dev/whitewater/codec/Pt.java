package dev.whitewater.codec;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

/** Minimal hand-written SpecificRecord — exercises the specific path without codegen. */
public class Pt extends SpecificRecordBase {
    public static final Schema SCHEMA$ = new Schema.Parser().parse("""
            {"type": "record", "name": "Pt", "namespace": "dev.whitewater.codec",
             "fields": [{"name": "x", "type": "int"}]}
            """);
    private int x;

    @Override
    public Schema getSchema() {
        return SCHEMA$;
    }

    @Override
    public Object get(int field) {
        return x;
    }

    @Override
    public void put(int field, Object value) {
        x = (Integer) value;
    }
}
