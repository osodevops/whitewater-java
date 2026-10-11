package dev.whitewater;

import dev.whitewater.internal.Transport;

import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

/**
 * Entry point for a Whitewater Riverbed: logical Admin (WCL), Writer,
 * Reader, and Subscription member clients. Any request may reach any
 * configured Node — routing is a server implementation detail.
 *
 * <pre>{@code
 * var client = WhitewaterClient.builder()
 *         .endpoints(URI.create("http://localhost:7071"))
 *         .apiKey(System.getenv("WHITEWATER_API_KEY"))
 *         .build();
 * var writer = client.writer("orders.checkout", 1);
 * var result = writer.append(UUID.randomUUID(), "order-1".getBytes(), payload);
 * }</pre>
 */
public final class WhitewaterClient implements AutoCloseable {

    public static final String SPEC_VERSION = "1.0.0";
    public static final int PROTOCOL_MAJOR = 1;

    private final Transport transport;
    private final AdminClient admin;
    private final Executor executor;

    private WhitewaterClient(Transport transport, Executor executor) {
        this.transport = transport;
        this.executor = executor;
        this.admin = new AdminClient(transport, executor);
    }

    public static Builder builder() {
        return new Builder();
    }

    public AdminClient admin() {
        return admin;
    }

    /** Ad-hoc Feed operations: committed reads, temporary fetches, appends. */
    public FeedClient feeds() {
        return new FeedClient(transport, executor);
    }

    /**
     * A Writer session facade. The epoch comes from
     * {@code OPEN WRITER SESSION <name>} via {@link AdminClient#wcl};
     * it fences every append in this session.
     */
    public WriterSessionClient writer(String writer, long sessionEpoch) {
        return new WriterSessionClient(transport, executor, writer, sessionEpoch);
    }

    /** A named-Reader session facade. */
    public ReaderSessionClient reader(String reader) {
        return new ReaderSessionClient(transport, executor, reader);
    }

    /** A cooperating member of a named Subscription. */
    public SubscriptionMemberClient subscriptionMember(String subscription) {
        return new SubscriptionMemberClient(transport, subscription);
    }

    @Override
    public void close() {
        // java.net.http.HttpClient needs no explicit shutdown.
    }

    public static final class Builder {
        private List<URI> endpoints = List.of();
        private String apiKey;
        private Duration timeout = Duration.ofSeconds(10);
        private Executor executor = ForkJoinPool.commonPool();

        public Builder endpoints(URI... endpoints) {
            this.endpoints = Arrays.asList(endpoints);
            return this;
        }

        public Builder endpoints(List<URI> endpoints) {
            this.endpoints = endpoints;
            return this;
        }

        public Builder apiKey(String apiKey) {
            this.apiKey = apiKey;
            return this;
        }

        public Builder connectTimeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder executor(Executor executor) {
            this.executor = executor;
            return this;
        }

        public WhitewaterClient build() {
            Objects.requireNonNull(apiKey, "apiKey is required");
            return new WhitewaterClient(new Transport(endpoints, apiKey, timeout), executor);
        }
    }
}
