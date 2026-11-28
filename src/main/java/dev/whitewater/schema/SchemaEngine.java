package dev.whitewater.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Strategy contract for validating schema definitions, compiling payload
 * validators, and evaluating schema evolution compatibility for a single
 * {@link SchemaType}.
 */
public interface SchemaEngine {

    /** Compiled, thread-safe payload validator for one schema definition. */
    @FunctionalInterface
    interface CompiledSchema {
        /** Returns an empty list if valid, or human-readable violation messages. */
        List<String> validatePayload(byte[] payload);
    }

    SchemaType type();

    /** Validate that {@code definition} is a well-formed schema of {@link #type()}. */
    List<String> validateDefinition(String definition);

    /** Compile {@code definition} into a fast reusable payload validator. */
    CompiledSchema compile(String definition);

    /**
     * Check whether data written with {@code writerDefinition} can be read by
     * {@code readerDefinition}. Returns an empty list when compatible, or
     * human-readable incompatibility messages otherwise.
     */
    List<String> checkBackwardCompatible(String readerDefinition, String writerDefinition);

    static SchemaEngine forType(SchemaType type) {
        Objects.requireNonNull(type, "type is required");
        return switch (type) {
            case JSON -> JsonSchemaEngine.INSTANCE;
            case AVRO -> AvroSchemaEngine.INSTANCE;
            case PROTOBUF -> ProtobufSchemaEngine.INSTANCE;
        };
    }

    /**
     * Evaluate {@code mode} compatibility of {@code candidateDefinition}
     * ({@code candidateType}) against {@code targetVersions}.
     */
    static SchemaCompatibilityResult evaluateCompatibility(
            SchemaType candidateType,
            String candidateDefinition,
            List<SchemaVersion> targetVersions,
            SchemaCompatibility mode) {
        SchemaEngine engine = forType(candidateType);
        List<String> defErrors = engine.validateDefinition(candidateDefinition);
        if (!defErrors.isEmpty()) {
            return SchemaCompatibilityResult.incompatible(defErrors);
        }
        SchemaCompatibility effectiveMode = mode == null ? SchemaCompatibility.BACKWARD : mode;
        if (effectiveMode == SchemaCompatibility.NONE || targetVersions == null || targetVersions.isEmpty()) {
            return SchemaCompatibilityResult.ok();
        }
        List<SchemaVersion> toCheck = effectiveMode.isTransitive()
                ? targetVersions
                : List.of(targetVersions.get(targetVersions.size() - 1));
        List<String> issues = new ArrayList<>();
        for (SchemaVersion prev : toCheck) {
            if (prev.type() != candidateType) {
                issues.add("schema type changed from " + prev.type().wire()
                        + " (v" + prev.version() + ") to " + candidateType.wire());
                continue;
            }
            if (effectiveMode.checksBackward()) {
                for (String err : engine.checkBackwardCompatible(candidateDefinition, prev.definition())) {
                    issues.add("BACKWARD vs v" + prev.version() + ": " + err);
                }
            }
            if (effectiveMode.checksForward()) {
                for (String err : engine.checkBackwardCompatible(prev.definition(), candidateDefinition)) {
                    issues.add("FORWARD vs v" + prev.version() + ": " + err);
                }
            }
        }
        return issues.isEmpty()
                ? SchemaCompatibilityResult.ok()
                : SchemaCompatibilityResult.incompatible(issues);
    }
}
