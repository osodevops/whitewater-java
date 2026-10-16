import dev.whitewater.WhitewaterClient;
import dev.whitewater.admin.Commands;
import dev.whitewater.model.Codecs;
import dev.whitewater.model.StateStoreSpec;

import java.net.URI;
import java.time.Duration;
import java.util.Map;

/**
 * Typed StateStore ergonomics: the SDK serializes keys and payloads —
 * no getBytes() in application code.
 *
 * <p>Bind by class — {@code stateStore(name, String.class, Employee.class)}
 * picks codecs for you — or pass explicit {@link dev.whitewater.model.Codec}s:
 * {@link Codecs#utf8()} for text, {@link Codecs#bytes()} for raw binary,
 * {@link Codecs#of} for your own serialization.
 */
public class WorkWithStores {

    /** One application payload object — Jackson maps it for you. */
    public record Employee(String name, String department, String passport) {}

    public static void main(String[] args) {
        var client = WhitewaterClient.builder()
                .endpoints(URI.create("http://localhost:7371"))
                .apiKey(System.getenv().getOrDefault(
                        "WHITEWATER_API_KEY", "whitewater-local-development-admin-key"))
                .build();

        // 1. Declare the store — named secondary keys at creation.
        client.commands(Commands.of().createDomain("accounts"));
        var spec = StateStoreSpec.named("accounts.employees")
                .addKey("passport", true)                    // unique
                .addKey("department");                       // non-unique
        client.stateStores().define(spec, null);              // manual store

        // 2. Bind the typed facade — just name the classes; the SDK
        //    picks the codec (String -> UTF-8, objects -> Jackson JSON).
        var store = client.stateStore("accounts.employees",
                String.class, Employee.class);

        // 3. Write — objects go in, named postings ride along.
        store.put("emp-1", new Employee("alice", "eng", "P111"),
                Map.of("passport", "P111", "department", "eng"));
        store.put("emp-2", new Employee("bob", "eng", "P222"),
                Map.of("passport", "P222", "department", "eng"));
        store.put("emp-3", new Employee("cara", "ops", "P333"),
                Map.of("passport", "P333", "department", "ops"));

        // 4. Read back — objects come out. get() is the fast path;
        //    getStrict bounded-waits for the committed frontier.
        var row = store.getStrict("emp-1", Duration.ofSeconds(5));
        Employee alice = row.payload();                      // decoded for you
        System.out.println(alice.name() + " works in " + alice.department());
        System.out.println("freshness: " + row.freshness().servedBy()
                + (row.freshness().behind() ? " (behind)" : ""));

        // 5. Named-key group lookup — every eng row.
        var eng = store.getByStrict("department", "eng", Duration.ofSeconds(5));
        eng.rows().forEach(r -> System.out.println("eng: " + r.key() + " -> " + r.payload().name()));

        // 6. Iterate in primary-key order — keys arrive decoded.
        store.iterate(r -> System.out.println("row " + r.key()));

        // 7. Clean up.
        store.clear();
        store.drop();
        client.close();
    }
}
