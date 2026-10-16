package dev.whitewater.codec;

import dev.whitewater.model.Codec;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;

/**
 * Avro {@link Codec}s — keys and payloads cross the wire as Avro binary
 * encoding (no schema fingerprint framing; the Riverbed is bytes-only
 * and schema identity is an application-side concern until the Control
 * Plane's Schema Policy lands).
 *
 * <p>Avro is an <b>optional</b> SDK dependency so the core client stays
 * Jackson-only: add {@code org.apache.avro:avro} to your build to use
 * this class. It is never loaded by the rest of the SDK.
 *
 * <pre>{@code
 * StateStore<String, User> users =
 *         client.stateStore("app.users", AvroCodecs.specific(User.class));
 * }</pre>
 */
public final class AvroCodecs {

    private AvroCodecs() {}

    /** Codec for a generated {@link SpecificRecord} class. */
    public static <T extends SpecificRecord> Codec<T> specific(Class<T> type) {
        Objects.requireNonNull(type, "type");
        return codec(
                new SpecificDatumWriter<>(type),
                new SpecificDatumReader<>(type),
                type.getName());
    }

    /** Codec over a caller-supplied schema for {@link GenericRecord} rows. */
    public static Codec<GenericRecord> generic(Schema schema) {
        Objects.requireNonNull(schema, "schema");
        return codec(
                new GenericDatumWriter<GenericRecord>(schema),
                new GenericDatumReader<GenericRecord>(schema),
                schema.getFullName());
    }

    /** Reflection-mapped codec for plain Java types (records, POJOs) — no codegen. */
    public static <T> Codec<T> reflect(Class<T> type) {
        Objects.requireNonNull(type, "type");
        Schema schema = ReflectData.get().getSchema(type);
        return codec(
                new ReflectDatumWriter<>(schema),
                new ReflectDatumReader<>(schema),
                type.getName());
    }

    private static <T> Codec<T> codec(DatumWriter<T> writer, DatumReader<T> reader, String name) {
        return new Codec<>() {
            @Override
            public byte[] encode(T value) {
                try {
                    var out = new ByteArrayOutputStream();
                    var encoder = EncoderFactory.get().binaryEncoder(out, null);
                    writer.write(value, encoder);
                    encoder.flush();
                    return out.toByteArray();
                } catch (IOException e) {
                    throw new IllegalArgumentException(
                            "value is not Avro-serializable as " + name, e);
                }
            }

            @Override
            public T decode(byte[] bytes) {
                try {
                    return reader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
                } catch (IOException e) {
                    throw new IllegalArgumentException(
                            "stored payload is not " + name + " Avro", e);
                }
            }
        };
    }
}
