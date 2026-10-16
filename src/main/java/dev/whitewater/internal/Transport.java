package dev.whitewater.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.whitewater.WhitewaterException;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Minimal JSON-over-HTTP transport: round-robins the configured Node
 * endpoints, attaches the bearer key, and maps (status, code) to typed
 * exceptions. Wire serialization lives here and nowhere else.
 */
public final class Transport {

    private static final ObjectMapper JSON = new ObjectMapper()
            .setPropertyNamingStrategy(
                    com.fasterxml.jackson.databind.PropertyNamingStrategies.SNAKE_CASE);
    public static ObjectMapper json() {
        return JSON;
    }

    private final List<URI> endpoints;
    private final String apiKey;
    private final HttpClient http;
    private final AtomicInteger next = new AtomicInteger();

    public Transport(List<URI> endpoints, String apiKey, Duration timeout) {
        if (endpoints.isEmpty()) {
            throw new IllegalArgumentException("at least one endpoint is required");
        }
        this.endpoints = List.copyOf(endpoints);
        this.apiKey = apiKey;
        this.http = HttpClient.newBuilder()
                .connectTimeout(timeout)
                .build();
    }

    private URI endpoint() {
        return endpoints.get(Math.floorMod(next.getAndIncrement(), endpoints.size()));
    }

    public JsonNode get(String path) {
        return roundTrip(() -> HttpRequest.newBuilder(endpoint().resolve(path))
                .GET()
                .header("authorization", "Bearer " + apiKey)
                .header("accept", "application/json")
                .timeout(Duration.ofSeconds(60))
                .build());
    }

    public JsonNode post(String path, JsonNode body) {
        return roundTrip(() -> {
            try {
                return HttpRequest.newBuilder(endpoint().resolve(path))
                        .POST(HttpRequest.BodyPublishers.ofByteArray(JSON.writeValueAsBytes(body)))
                        .header("authorization", "Bearer " + apiKey)
                        .header("content-type", "application/json")
                        .header("accept", "application/json")
                        .timeout(Duration.ofSeconds(60))
                        .build();
            } catch (IOException e) {
                throw new IllegalStateException("request serialization failed", e);
            }
        });
    }

    /** One HTTP exchange as status + parsed body, without error mapping. */
    public record Reply(int status, JsonNode body) {}

    /**
     * Send a request and return the raw {@link Reply} instead of mapping
     * non-2xx responses to typed exceptions. Conformance-style callers that
     * must observe the wire status (e.g. {@code error_status} assertions)
     * use this; application code should prefer {@link #get}/{@link #post}.
     */
    public Reply exchange(String method, String path, JsonNode body) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(endpoint().resolve(path))
                .method(method, body == null
                        ? HttpRequest.BodyPublishers.noBody()
                        : publish(body))
                .header("authorization", "Bearer " + apiKey)
                .header("accept", "application/json")
                .timeout(Duration.ofSeconds(60));
        if (body != null) {
            builder.header("content-type", "application/json");
        }
        HttpResponse<byte[]> response;
        try {
            response = http.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
        } catch (IOException e) {
            throw new WhitewaterException.Unavailable(0, null,
                    "Node unreachable: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new WhitewaterException.Unavailable(0, null, "interrupted while awaiting response");
        }
        return new Reply(response.statusCode(), parse(response.body()));
    }

    private static HttpRequest.BodyPublisher publish(JsonNode body) {
        try {
            return HttpRequest.BodyPublishers.ofByteArray(JSON.writeValueAsBytes(body));
        } catch (IOException e) {
            throw new IllegalStateException("request serialization failed", e);
        }
    }

    private JsonNode roundTrip(java.util.function.Supplier<HttpRequest> requestSupplier) {
        HttpRequest request = requestSupplier.get();
        HttpResponse<byte[]> response;
        try {
            response = http.send(request, HttpResponse.BodyHandlers.ofByteArray());
        } catch (IOException e) {
            throw new WhitewaterException.Unavailable(0, null,
                    "Node unreachable: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new WhitewaterException.Unavailable(0, null, "interrupted while awaiting response");
        }
        JsonNode body = parse(response.body());
        if (response.statusCode() / 100 == 2) {
            return body;
        }
        throw mapError(response.statusCode(), body);
    }

    private static JsonNode parse(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return JSON.nullNode();
        }
        try {
            return JSON.readTree(bytes);
        } catch (IOException e) {
            return JSON.nullNode();
        }
    }

    /** Map a non-2xx {@link Reply} to the typed exception it represents. */
    public static WhitewaterException mapError(int status, JsonNode body) {
        String message = body.path("error").asText("http " + status);
        String code = body.hasNonNull("code") ? body.get("code").asText() : null;
        if (code != null) {
            switch (code) {
                case "stale_epoch" -> {
                    return new WhitewaterException.StaleSession(status, code, message);
                }
                case "lease.busy", "lease.stale_lease", "lease.capacity" -> {
                    return new WhitewaterException.LeaseConflict(status, code, message);
                }
            }
        }
        if (status == 409) {
            return new WhitewaterException.Conflict(status, code, message);
        }
        if (status == 503 || status == 0) {
            return new WhitewaterException.Unavailable(status, code, message);
        }
        if (status >= 500) {
            return new WhitewaterException.AmbiguousResult(status, code, message);
        }
        return new WhitewaterException.Refused(status, code, message);
    }
}
