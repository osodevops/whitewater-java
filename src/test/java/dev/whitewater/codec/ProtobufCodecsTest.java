package dev.whitewater.codec;

import com.google.protobuf.StringValue;
import dev.whitewater.model.Codec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Protobuf codec roundtrips. {@link StringValue} is a well-known type
 * shipped inside protobuf-java itself — no codegen needed for the test.
 */
class ProtobufCodecsTest {

    @Test
    void classBindingResolvesStaticParseFrom() {
        Codec<StringValue> codec = ProtobufCodecs.of(StringValue.class);
        byte[] wire = codec.encode(StringValue.of("héllo wörld"));
        assertEquals(StringValue.of("héllo wörld"), codec.decode(wire));
    }

    @Test
    void parserBindingWorksForExplicitParsers() {
        Codec<StringValue> codec = ProtobufCodecs.of(StringValue.parser());
        assertEquals(StringValue.of("x"), codec.decode(codec.encode(StringValue.of("x"))));
    }

    @Test
    void classesWithoutStaticParseFromAreRefused() {
        var thrown = assertThrows(IllegalArgumentException.class,
                () -> ProtobufCodecs.of(NoParse.class));
        assertTrue(thrown.getMessage().contains("parseFrom"));
    }

    @Test
    void garbageBytesFailAsIllegalArgument() {
        Codec<StringValue> codec = ProtobufCodecs.of(StringValue.parser());
        assertThrows(IllegalArgumentException.class,
                () -> codec.decode(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF}));
    }

    /** A MessageLite with no parseFrom — forced through the reflection path. */
    private abstract static class NoParse implements com.google.protobuf.MessageLite {}
}
