package dev.whitewater.codec;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import dev.whitewater.model.Codec;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Objects;

/**
 * Protocol Buffer {@link Codec}s — keys and payloads cross the wire as
 * the message's own binary encoding.
 *
 * <p>Protobuf is an <b>optional</b> SDK dependency so the core client
 * stays Jackson-only: add {@code com.google.protobuf:protobuf-java} to
 * your build to use this class. It is never loaded by the rest of the
 * SDK.
 *
 * <pre>{@code
 * StateStore<String, User> users =
 *         client.stateStore("app.users", ProtobufCodecs.of(User.class));
 * users.put("user-1", User.newBuilder().setName("alice").build());
 * User back = users.get("user-1").payload();
 * }</pre>
 *
 * <p>The Riverbed sees bytes either way — schema validation and
 * compatibility are application-side until the Control Plane's Schema
 * Policy lands; this codec is the seam it will bind to.
 */
public final class ProtobufCodecs {

    private ProtobufCodecs() {}

    /**
     * Codec for a generated message class — resolves its static
     * {@code parseFrom(byte[])} once, so callers name the class exactly
     * like {@code stateStore(name, Class)} does for JSON.
     */
    public static <T extends MessageLite> Codec<T> of(Class<T> type) {
        Objects.requireNonNull(type, "type");
        Method parser = parser(type);
        return new Codec<>() {
            @Override
            public byte[] encode(T value) {
                return value.toByteArray();
            }

            @Override
            @SuppressWarnings("unchecked")
            public T decode(byte[] bytes) {
                try {
                    return (T) parser.invoke(null, bytes);
                } catch (ReflectiveOperationException e) {
                    throw new IllegalArgumentException(
                            "stored payload is not " + type.getName() + " protobuf", e);
                }
            }
        };
    }

    /** Codec from an explicit {@link Parser} — hand-rolled or generated. */
    public static <T extends MessageLite> Codec<T> of(Parser<T> parser) {
        Objects.requireNonNull(parser, "parser");
        return new Codec<>() {
            @Override
            public byte[] encode(T value) {
                return value.toByteArray();
            }

            @Override
            public T decode(byte[] bytes) {
                try {
                    return parser.parseFrom(bytes);
                } catch (InvalidProtocolBufferException e) {
                    throw new IllegalArgumentException(
                            "stored payload is not valid protobuf", e);
                }
            }
        };
    }

    private static Method parser(Class<? extends MessageLite> type) {
        try {
            Method parse = type.getMethod("parseFrom", byte[].class);
            if (!Modifier.isStatic(parse.getModifiers())) {
                throw new IllegalArgumentException(
                        type.getName() + ".parseFrom is not static — pass a Parser instead");
            }
            return parse;
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(
                    type.getName() + " declares no static parseFrom(byte[]) — "
                            + "pass ProtobufCodecs.of(parser) instead", e);
        }
    }
}
