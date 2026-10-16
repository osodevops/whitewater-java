package dev.whitewater.model;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import dev.whitewater.internal.Transport;

import java.nio.charset.StandardCharsets;
import java.util.function.Function;

/**
 * Standard {@link Codec} factories. {@link #utf8()} for String keys and
 * text payloads, {@link #bytes()} for raw binary, {@link #json(Class)}
 * for Jackson-mapped objects, and {@link #of} for anything else
 * (protobuf, Kryo, application framing).
 */
public final class Codecs {

    private Codecs() {}

    private static final Codec<byte[]> BYTES = new Codec<>() {
        @Override
        public byte[] encode(byte[] value) {
            return value;
        }

        @Override
        public byte[] decode(byte[] bytes) {
            return bytes;
        }
    };

    private static final Codec<String> UTF8 = new Codec<>() {
        @Override
        public byte[] encode(String value) {
            return value.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public String decode(byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
    };

    private static final Codec<JsonNode> JSON_TREE = new Codec<>() {
        @Override
        public byte[] encode(JsonNode value) {
            try {
                return Transport.json().writeValueAsBytes(value);
            } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                throw new IllegalArgumentException("value is not JSON-serializable", e);
            }
        }

        @Override
        public JsonNode decode(byte[] bytes) {
            try {
                return Transport.json().readTree(bytes);
            } catch (java.io.IOException e) {
                throw new IllegalArgumentException("stored payload is not JSON", e);
            }
        }
    };

    /** Identity codec — raw bytes through to the wire. */
    public static Codec<byte[]> bytes() {
        return BYTES;
    }

    /** UTF-8 text — the common case for keys and JSON/text payloads. */
    public static Codec<String> utf8() {
        return UTF8;
    }

    /** Jackson-mapped objects. */
    public static <T> Codec<T> json(Class<T> type) {
        return new Codec<>() {
            @Override
            public byte[] encode(T value) {
                try {
                    return Transport.json().writeValueAsBytes(value);
                } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                    throw new IllegalArgumentException(
                            "value is not JSON-serializable as " + type.getName(), e);
                }
            }

            @Override
            public T decode(byte[] bytes) {
                try {
                    return Transport.json().readValue(bytes, type);
                } catch (java.io.IOException e) {
                    throw new IllegalArgumentException(
                            "stored payload is not " + type.getName() + " JSON", e);
                }
            }
        };
    }

    /** Jackson-mapped objects with generic signatures (List<…>, Map<…>). */
    public static <T> Codec<T> json(TypeReference<T> type) {
        return new Codec<>() {
            @Override
            public byte[] encode(T value) {
                try {
                    return Transport.json().writeValueAsBytes(value);
                } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                    throw new IllegalArgumentException("value is not JSON-serializable", e);
                }
            }

            @Override
            public T decode(byte[] bytes) {
                try {
                    return Transport.json().readValue(bytes, type);
                } catch (java.io.IOException e) {
                    throw new IllegalArgumentException("stored payload is not JSON", e);
                }
            }
        };
    }

    /** Untyped JSON payloads — {@code row.payload().path("field")}. */
    public static Codec<JsonNode> jsonTree() {
        return JSON_TREE;
    }

    /** A codec from caller-supplied encoder/decoder functions. */
    public static <T> Codec<T> of(Function<T, byte[]> encoder, Function<byte[], T> decoder) {
        java.util.Objects.requireNonNull(encoder, "encoder");
        java.util.Objects.requireNonNull(decoder, "decoder");
        return new Codec<>() {
            @Override
            public byte[] encode(T value) {
                return encoder.apply(value);
            }

            @Override
            public T decode(byte[] bytes) {
                return decoder.apply(bytes);
            }
        };
    }
}
