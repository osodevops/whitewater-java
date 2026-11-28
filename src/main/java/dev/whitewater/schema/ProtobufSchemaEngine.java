package dev.whitewater.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import dev.whitewater.internal.Transport;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Protocol Buffers (.proto) schema definition validator, payload validator
 * (supporting both JSON and Protobuf binary wire format), and evolution
 * compatibility checker.
 */
final class ProtobufSchemaEngine implements SchemaEngine {

    static final ProtobufSchemaEngine INSTANCE = new ProtobufSchemaEngine();

    private static final Pattern SYNTAX_RE = Pattern.compile("syntax\\s*=\\s*['\"]proto[23]['\"]\\s*;");
    private static final Pattern BLOCK_RE = Pattern.compile("^(message|enum|service|extend|oneof)\\s+([A-Za-z_]\\w*)$");
    private static final Pattern FIELD_RE = Pattern.compile(
            "^(optional|required|repeated)?\\s*(map\\s*<[^<>]+>|[A-Za-z_][\\w.]*)\\s+([A-Za-z_]\\w*)\\s*=\\s*(-?\\d+)\\s*(\\[[^\\]]*\\])?\\s*;$");
    private static final Pattern ENUM_VAL_RE = Pattern.compile(
            "^([A-Za-z_]\\w*)\\s*=\\s*(-?\\d+)\\s*(\\[[^\\]]*\\])?\\s*;$");

    record ProtoField(String label, String type, String name, String camelName, int number) {
        boolean isRepeated() {
            return "repeated".equals(label);
        }
        boolean isRequired() {
            return "required".equals(label);
        }
        boolean isMap() {
            return type.startsWith("map<") || type.startsWith("map ");
        }
    }

    record ProtoMessage(String name, Map<Integer, ProtoField> fieldsByNumber, Map<String, ProtoField> fieldsByName) {}

    record ParsedProto(
            Map<String, ProtoMessage> messages,
            Map<String, Set<String>> enums,
            ProtoMessage rootMessage,
            List<String> issues) {}

    private ProtobufSchemaEngine() {
    }

    @Override
    public SchemaType type() {
        return SchemaType.PROTOBUF;
    }

    @Override
    public List<String> validateDefinition(String definition) {
        if (definition == null || definition.isBlank()) {
            return List.of("Protobuf schema definition must not be empty");
        }
        return parseProto(definition).issues();
    }

    private static String stripComments(String raw) {
        String normalized = raw.replaceAll("[\uFEFF\u200B-\u200D\u2060]", "").replace('\u00A0', ' ');
        StringBuilder out = new StringBuilder(normalized.length());
        int i = 0;
        String mode = "normal";
        while (i < normalized.length()) {
            char ch = normalized.charAt(i);
            char next = (i + 1 < normalized.length()) ? normalized.charAt(i + 1) : '\0';
            if ("normal".equals(mode)) {
                if (ch == '/' && next == '/') { mode = "line"; out.append("  "); i += 2; continue; }
                if (ch == '/' && next == '*') { mode = "block"; out.append("  "); i += 2; continue; }
                if (ch == '\'') mode = "squote";
                else if (ch == '"') mode = "dquote";
                out.append(ch);
                i++;
            } else if ("line".equals(mode)) {
                if (ch == '\n') { mode = "normal"; out.append('\n'); } else out.append(' ');
                i++;
            } else if ("block".equals(mode)) {
                if (ch == '*' && next == '/') { mode = "normal"; out.append("  "); i += 2; continue; }
                out.append(ch == '\n' ? '\n' : ' ');
                i++;
            } else {
                if (ch == '\\' && i + 1 < normalized.length()) {
                    out.append(ch).append(normalized.charAt(i + 1));
                    i += 2;
                    continue;
                }
                if (("squote".equals(mode) && ch == '\'') || ("dquote".equals(mode) && ch == '"')) mode = "normal";
                out.append(ch);
                i++;
            }
        }
        return out.toString();
    }

    private static String toCamelCase(String snake) {
        StringBuilder sb = new StringBuilder(snake.length());
        boolean upper = false;
        for (int i = 0; i < snake.length(); i++) {
            char c = snake.charAt(i);
            if (c == '_') upper = true;
            else if (upper) { sb.append(Character.toUpperCase(c)); upper = false; }
            else sb.append(c);
        }
        return sb.toString();
    }

    private record ScopeFrame(String kind, String name, Set<Integer> numbers) {}

    static ParsedProto parseProto(String text) {
        List<String> issues = new ArrayList<>();
        String cleaned = stripComments(text);
        if (!SYNTAX_RE.matcher(cleaned).find()) {
            issues.add("missing syntax declaration, e.g. syntax = \"proto3\";");
        }
        Map<String, ProtoMessage> messages = new LinkedHashMap<>();
        Map<String, Set<String>> enums = new LinkedHashMap<>();
        Deque<ScopeFrame> stack = new ArrayDeque<>();
        boolean sawDef = false;
        String[] lines = cleaned.split("\n");
        String pendingKind = null;
        String pendingName = null;

        for (int lineIdx = 0; lineIdx < lines.length; lineIdx++) {
            String line = lines[lineIdx].trim();
            if (line.isEmpty()) continue;
            for (String rawTok : splitBraces(line)) {
                String stmt = rawTok.trim();
                if (stmt.isEmpty()) continue;
                if ("}".equals(stmt)) {
                    if (stack.isEmpty()) issues.add("line " + (lineIdx + 1) + ": unexpected '}'");
                    else stack.pop();
                    continue;
                }
                if (stmt.endsWith("{")) {
                    String header = stmt.substring(0, stmt.length() - 1).trim();
                    String kind = pendingKind;
                    String name = pendingName;
                    pendingKind = null;
                    pendingName = null;
                    if (!header.isEmpty()) {
                        Matcher bm = BLOCK_RE.matcher(header);
                        if (bm.matches()) {
                            kind = bm.group(1);
                            name = bm.group(2);
                        } else {
                            issues.add("line " + (lineIdx + 1) + ": invalid block declaration '" + header + "'");
                            kind = "block";
                            name = "unknown";
                        }
                    }
                    if (kind == null) {
                        issues.add("line " + (lineIdx + 1) + ": unexpected '{'");
                        kind = "block";
                        name = "unknown";
                    }
                    stack.push(new ScopeFrame(kind, name, new HashSet<>()));
                    if ("message".equals(kind)) {
                        messages.putIfAbsent(name, new ProtoMessage(name, new LinkedHashMap<>(), new LinkedHashMap<>()));
                        sawDef = true;
                    } else if ("enum".equals(kind)) {
                        enums.putIfAbsent(name, new HashSet<>());
                        sawDef = true;
                    }
                    continue;
                }
                Matcher pm = BLOCK_RE.matcher(stmt);
                if (pm.matches()) {
                    pendingKind = pm.group(1);
                    pendingName = pm.group(2);
                    continue;
                }
                handleStatement(stmt, lineIdx + 1, stack, messages, enums, issues);
            }
        }
        if (!stack.isEmpty()) issues.add("unclosed '" + stack.peek().kind() + "' block");
        if (!sawDef) issues.add("schema defines no 'message' or 'enum'");
        ProtoMessage root = messages.values().stream().findFirst().orElse(null);
        return new ParsedProto(messages, enums, root, issues);
    }

    private static void handleStatement(
            String stmt, int lineNo, Deque<ScopeFrame> stack,
            Map<String, ProtoMessage> messages, Map<String, Set<String>> enums, List<String> issues) {
        ScopeFrame top = stack.peek();
        if (top == null) {
            if (!stmt.matches("^(syntax|package|import|option|edition)\\b.*;$")) {
                issues.add("line " + lineNo + ": unexpected top-level statement '" + stmt + "'");
            }
            return;
        }
        if ("enum".equals(top.kind())) {
            if (stmt.startsWith("option ") || stmt.startsWith("reserved ") || ";".equals(stmt)) return;
            Matcher em = ENUM_VAL_RE.matcher(stmt);
            if (!em.matches()) {
                issues.add("line " + lineNo + ": invalid enum value '" + stmt + "'");
            } else {
                int num = Integer.parseInt(em.group(2));
                if (!top.numbers().add(num)) issues.add("line " + lineNo + ": duplicate enum number " + num);
                enums.get(top.name()).add(em.group(1));
            }
        } else if ("message".equals(top.kind()) || "oneof".equals(top.kind())) {
            if (stmt.startsWith("option ") || stmt.startsWith("reserved ") || ";".equals(stmt)) return;
            Matcher fm = FIELD_RE.matcher(stmt);
            if (!fm.matches()) {
                issues.add("line " + lineNo + ": invalid field declaration '" + stmt + "'");
                return;
            }
            String label = fm.group(1);
            String ftype = fm.group(2).replaceAll("\\s+", "");
            String fname = fm.group(3);
            int fnum = Integer.parseInt(fm.group(4));
            if (fnum <= 0 || (fnum >= 19000 && fnum <= 19999)) {
                issues.add("line " + lineNo + ": invalid field number " + fnum);
            }
            ScopeFrame msgFrame = findEnclosingMessage(stack);
            if (msgFrame != null) {
                if (!msgFrame.numbers().add(fnum)) {
                    issues.add("line " + lineNo + ": duplicate field number " + fnum + " in " + msgFrame.name());
                }
                ProtoMessage pm = messages.get(msgFrame.name());
                if (pm != null) {
                    ProtoField pf = new ProtoField(label, ftype, fname, toCamelCase(fname), fnum);
                    pm.fieldsByNumber().put(fnum, pf);
                    pm.fieldsByName().put(fname, pf);
                    pm.fieldsByName().put(pf.camelName(), pf);
                }
            }
        }
    }

    private static ScopeFrame findEnclosingMessage(Deque<ScopeFrame> stack) {
        for (ScopeFrame f : stack) {
            if ("message".equals(f.kind())) return f;
        }
        return null;
    }

    private static List<String> splitBraces(String line) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (ch == '{') {
                out.add(cur + "{");
                cur.setLength(0);
            } else if (ch == '}') {
                if (!cur.toString().isBlank()) out.add(cur.toString());
                out.add("}");
                cur.setLength(0);
            } else {
                cur.append(ch);
            }
        }
        if (!cur.toString().isBlank()) out.add(cur.toString());
        return out;
    }

    @Override
    public CompiledSchema compile(String definition) {
        ParsedProto parsed = parseProto(definition);
        if (!parsed.issues().isEmpty()) {
            throw new IllegalArgumentException(String.join("; ", parsed.issues()));
        }
        return payload -> {
            if (payload == null || payload.length == 0) {
                return List.of("Protobuf payload is empty");
            }
            ProtoMessage root = parsed.rootMessage();
            if (root == null) {
                return List.of();
            }
            if (looksLikeJsonObject(payload)) {
                try {
                    JsonNode json = Transport.json().readTree(new String(payload, StandardCharsets.UTF_8));
                    List<String> issues = new ArrayList<>();
                    validateProtoJson(parsed, root, json, "$", issues);
                    return issues;
                } catch (IOException ignored) {
                    // Fall through to binary wire check
                }
            }
            return validateProtoWire(root, payload);
        };
    }

    private static boolean looksLikeJsonObject(byte[] payload) {
        int i = 0;
        while (i < payload.length && (payload[i] == ' ' || payload[i] == '\t' || payload[i] == '\n' || payload[i] == '\r')) {
            i++;
        }
        return i < payload.length && payload[i] == '{';
    }

    private void validateProtoJson(
            ParsedProto parsed, ProtoMessage msg, JsonNode node, String path, List<String> issues) {
        if (!node.isObject()) {
            issues.add(path + ": expected JSON object for protobuf message " + msg.name());
            return;
        }
        for (ProtoField f : msg.fieldsByNumber().values()) {
            if (f.isRequired() && !node.has(f.name()) && !node.has(f.camelName())) {
                issues.add(path + ": missing required protobuf field '" + f.name() + "'");
            }
        }
        Iterator<String> names = node.fieldNames();
        while (names.hasNext()) {
            String fn = names.next();
            ProtoField pf = msg.fieldsByName().get(fn);
            if (pf == null) {
                issues.add(path + ": unknown field '" + fn + "' for protobuf message " + msg.name());
                continue;
            }
            JsonNode val = node.get(fn);
            if (val == null || val.isNull()) continue;
            if (pf.isRepeated()) {
                if (!val.isArray()) {
                    issues.add(path + "." + fn + ": expected array for repeated field");
                } else {
                    for (int i = 0; i < val.size(); i++) {
                        validateProtoFieldValue(parsed, pf.type(), val.get(i), path + "." + fn + "[" + i + "]", issues);
                    }
                }
            } else if (pf.isMap()) {
                if (!val.isObject()) issues.add(path + "." + fn + ": expected object for map field");
            } else {
                validateProtoFieldValue(parsed, pf.type(), val, path + "." + fn, issues);
            }
        }
    }

    private void validateProtoFieldValue(
            ParsedProto parsed, String ftype, JsonNode val, String path, List<String> issues) {
        switch (ftype) {
            case "int32", "sint32", "sfixed32", "uint32", "fixed32" -> {
                if (!val.isIntegralNumber()) issues.add(path + ": expected integer for " + ftype);
            }
            case "int64", "sint64", "sfixed64", "uint64", "fixed64" -> {
                if (!val.isIntegralNumber() && !val.isTextual()) issues.add(path + ": expected integer or decimal string for " + ftype);
            }
            case "float", "double" -> {
                if (!val.isNumber()) issues.add(path + ": expected number for " + ftype);
            }
            case "bool" -> {
                if (!val.isBoolean()) issues.add(path + ": expected boolean for bool");
            }
            case "string", "bytes" -> {
                if (!val.isTextual()) issues.add(path + ": expected string for " + ftype);
            }
            default -> {
                String simple = ftype.contains(".") ? ftype.substring(ftype.lastIndexOf('.') + 1) : ftype;
                if (parsed.enums().containsKey(simple)) {
                    Set<String> symbols = parsed.enums().get(simple);
                    if (val.isTextual() && !symbols.contains(val.asText())) {
                        issues.add(path + ": unknown enum value '" + val.asText() + "' for " + simple);
                    } else if (!val.isTextual() && !val.isIntegralNumber()) {
                        issues.add(path + ": expected enum string or number for " + simple);
                    }
                } else if (parsed.messages().containsKey(simple)) {
                    validateProtoJson(parsed, parsed.messages().get(simple), val, path, issues);
                }
            }
        }
    }

    private List<String> validateProtoWire(ProtoMessage root, byte[] payload) {
        List<String> issues = new ArrayList<>();
        try {
            CodedInputStream in = CodedInputStream.newInstance(payload);
            int seenTags = 0;
            while (!in.isAtEnd()) {
                int tag = in.readTag();
                if (tag == 0) break;
                int fieldNumber = WireFormat.getTagFieldNumber(tag);
                int wireType = WireFormat.getTagWireType(tag);
                ProtoField pf = root.fieldsByNumber().get(fieldNumber);
                if (pf == null) {
                    issues.add("unknown protobuf field number " + fieldNumber + " on message " + root.name());
                } else if (!isWireTypeCompatible(pf, wireType)) {
                    issues.add("field '" + pf.name() + "' (#" + fieldNumber + ") has incompatible wire type " + wireType);
                }
                in.skipField(tag);
                seenTags++;
            }
            if (seenTags == 0) {
                issues.add("Protobuf payload contained no valid wire fields");
            }
        } catch (IOException e) {
            issues.add("Invalid Protobuf wire payload: " + e.getMessage());
        }
        return issues;
    }

    private static boolean isWireTypeCompatible(ProtoField pf, int wireType) {
        if (pf.isRepeated() && wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
            return true; // packed repeated fields use length-delimited
        }
        return switch (pf.type()) {
            case "int32", "int64", "uint32", "uint64", "sint32", "sint64", "bool" ->
                    wireType == WireFormat.WIRETYPE_VARINT;
            case "fixed64", "sfixed64", "double" ->
                    wireType == WireFormat.WIRETYPE_FIXED64;
            case "fixed32", "sfixed32", "float" ->
                    wireType == WireFormat.WIRETYPE_FIXED32;
            default -> wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED
                    || wireType == WireFormat.WIRETYPE_VARINT;
        };
    }

    @Override
    public List<String> checkBackwardCompatible(String readerDefinition, String writerDefinition) {
        ParsedProto reader = parseProto(readerDefinition);
        ParsedProto writer = parseProto(writerDefinition);
        if (!reader.issues().isEmpty()) return reader.issues();
        if (!writer.issues().isEmpty()) return writer.issues();
        List<String> issues = new ArrayList<>();
        ProtoMessage rRoot = reader.rootMessage();
        ProtoMessage wRoot = writer.rootMessage();
        if (rRoot == null || wRoot == null) return issues;
        for (ProtoField rf : rRoot.fieldsByNumber().values()) {
            ProtoField wf = wRoot.fieldsByNumber().get(rf.number());
            if (wf == null) {
                if (rf.isRequired()) {
                    issues.add("reader requires field '" + rf.name() + "' (#" + rf.number() + ") not present in writer");
                }
            } else {
                if (!typesCompatible(rf.type(), wf.type())) {
                    issues.add("field #" + rf.number() + " changed type from '" + wf.type() + "' to '" + rf.type() + "'");
                }
                if (!rf.name().equals(wf.name())) {
                    issues.add("field #" + rf.number() + " changed name from '" + wf.name() + "' to '" + rf.name() + "'");
                }
                if (rf.isRepeated() != wf.isRepeated()) {
                    issues.add("field #" + rf.number() + " changed repeated cardinality");
                }
            }
        }
        for (Map.Entry<String, Set<String>> wEnum : writer.enums().entrySet()) {
            Set<String> rSymbols = reader.enums().get(wEnum.getKey());
            if (rSymbols != null) {
                for (String sym : wEnum.getValue()) {
                    if (!rSymbols.contains(sym)) {
                        issues.add("enum '" + wEnum.getKey() + "' removed symbol '" + sym + "'");
                    }
                }
            }
        }
        return issues;
    }

    private static boolean typesCompatible(String readerType, String writerType) {
        if (readerType.equals(writerType)) return true;
        Set<String> varintFamily = Set.of("int32", "uint32", "int64", "uint64", "bool");
        return varintFamily.contains(readerType) && varintFamily.contains(writerType);
    }
}
