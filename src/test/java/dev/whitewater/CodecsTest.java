package dev.whitewater;

import com.fasterxml.jackson.core.type.TypeReference;
import dev.whitewater.model.Codec;
import dev.whitewater.model.Codecs;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** Codec roundtrips — the byte plumbing that stays inside the SDK. */
class CodecsTest {

    record Employee(String name, String department) {}

    @Test
    void utf8RoundtripsText() {
        Codec<String> codec = Codecs.utf8();
        byte[] wire = codec.encode("héllo wörld");
        assertEquals("héllo wörld", new String(wire, StandardCharsets.UTF_8));
        assertEquals("héllo wörld", codec.decode(wire));
    }

    @Test
    void bytesIsIdentity() {
        Codec<byte[]> codec = Codecs.bytes();
        byte[] raw = {0, 1, (byte) 0xFF, 42};
        assertSame(raw, codec.encode(raw));
        assertSame(raw, codec.decode(raw));
    }

    @Test
    void jsonMapsObjectsBothWays() {
        Codec<Employee> codec = Codecs.json(Employee.class);
        byte[] wire = codec.encode(new Employee("alice", "eng"));
        assertEquals("{\"name\":\"alice\",\"department\":\"eng\"}",
                new String(wire, StandardCharsets.UTF_8));
        assertEquals(new Employee("alice", "eng"), codec.decode(wire));
    }

    @Test
    void jsonTypeReferenceHandlesGenerics() {
        Codec<List<Map<String, Long>>> codec = Codecs.json(new TypeReference<>() {});
        List<Map<String, Long>> value = List.of(Map.of("a", 1L), Map.of("b", 2L));
        assertEquals(value, codec.decode(codec.encode(value)));
    }

    @Test
    void jsonTreeDecodesUntypedPayloads() {
        var node = Codecs.jsonTree().decode("{\"dept\":\"eng\",\"n\":3}".getBytes(StandardCharsets.UTF_8));
        assertEquals("eng", node.path("dept").asText());
        assertEquals(3, node.path("n").asInt());
    }

    @Test
    void ofBuildsACodecFromFunctions() {
        Codec<Integer> ints = Codecs.of(
                i -> Integer.toString(i).getBytes(StandardCharsets.UTF_8),
                bytes -> Integer.parseInt(new String(bytes, StandardCharsets.UTF_8)));
        assertEquals(41, ints.decode(ints.encode(41)));
        // Object keys work through the same door — any caller encoding.
        Codec<Employee> employees = Codecs.of(
                e -> e.name().getBytes(StandardCharsets.UTF_8),
                bytes -> new Employee(new String(bytes, StandardCharsets.UTF_8), "?"));
        assertEquals(new Employee("alice", "?"), employees.decode(employees.encode(new Employee("alice", "eng"))));
    }

    @Test
    void forClassPicksTheRightCodec() {
        assertSame(Codecs.utf8().getClass(), Codecs.forClass(String.class).getClass());
        assertSame(Codecs.bytes(), Codecs.forClass(byte[].class));
        assertSame(Codecs.jsonTree().getClass(), Codecs.forClass(com.fasterxml.jackson.databind.JsonNode.class).getClass());
        // Every other class Jackson-maps — just naming the type binds it.
        Codec<Employee> codec = Codecs.forClass(Employee.class);
        assertEquals(new Employee("alice", "eng"),
                codec.decode(codec.encode(new Employee("alice", "eng"))));
        assertThrows(NullPointerException.class, () -> Codecs.forClass(null));
    }

    @Test
    void malformedStoredBytesFailAsIllegalArgument() {
        Codec<Employee> codec = Codecs.json(Employee.class);
        var thrown = assertThrows(IllegalArgumentException.class,
                () -> codec.decode("not json".getBytes(StandardCharsets.UTF_8)));
        assertTrue(thrown.getMessage().contains("Employee"));

        assertThrows(IllegalArgumentException.class,
                () -> Codecs.jsonTree().decode("nope".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void unserializableValuesFailBeforeTheWire() {
        Codec<Object> codec = Codecs.json(Object.class);
        Object self = new Object() {
            @SuppressWarnings("unused")
            public Object getSelf() { return this; }
        };
        assertThrows(IllegalArgumentException.class, () -> codec.encode(self));
    }
}
