import dev.whitewater.WhitewaterClient;
import dev.whitewater.model.AbstractStoreKeySet;
import dev.whitewater.model.Codecs;
import dev.whitewater.model.StateStoreSpec;

import java.net.URI;
import java.time.Duration;
import java.util.function.Consumer;

/**
 * Typed StateStore ergonomics: the SDK serializes keys and payloads —
 * no getBytes() in application code — and {@link AbstractStoreKeySet}
 * makes secondary-key postings strongly typed and self-completing.
 */
public class WorkWithStores {

    /** One application payload object — Jackson maps it for you. */
    public record Employee(String name, String department, String passport) {}

    /**
     * Typed secondary-key set for the employees StateStore — supports
     * static factory, fluent chaining, and lambda builder styles.
     */
    public static final class PassportKeySet extends AbstractStoreKeySet {
        public PassportKeySet setPassport(String v)   { return put("passport", v); }
        public PassportKeySet setDepartment(String v) { return put("department", v); }

        public PassportKeySet passport(String v)      { return setPassport(v); }
        public PassportKeySet department(String v)    { return setDepartment(v); }

        public static PassportKeySet with(String passport, String department) {
            return new PassportKeySet().setPassport(passport).setDepartment(department);
        }

        public static PassportKeySet withPassport(String passport) {
            return new PassportKeySet().setPassport(passport);
        }

        public static PassportKeySet of(Consumer<PassportKeySet> builder) {
            var ks = new PassportKeySet();
            builder.accept(ks);
            return ks;
        }
    }

    public static void main(String[] args) {
        var client = WhitewaterClient.builder()
                .endpoints(URI.create("http://localhost:7371"))
                .apiKey(System.getenv().getOrDefault(
                        "WHITEWATER_API_KEY", "whitewater-local-development-admin-key"))
                .build();

        // 1. Declare the store inside the accounts Domain — named
        //    secondary keys at creation. The spec name resolves in-domain.
        var accounts = client.createDomain("accounts");
        var spec = StateStoreSpec.named("employees")
                .addUniqueKey("passport")                    // unique (default key)
                .addNonUniqueKey("department");                       // non-unique
        accounts.defineStateStore(spec, (String) null);       // manual store

        // 2. Bind the typed facade — just name the classes; the SDK
        //    picks the codec (String -> UTF-8, objects -> Jackson JSON).
        var store = accounts.stateStore("employees",
                String.class, Employee.class);

        // 3. Write — pick whichever StoreKeySet style reads cleanest:
        //    Style A: Static factory one-liner
        store.put("emp-1", new Employee("alice", "eng", "P111"),
                PassportKeySet.with("P111", "eng"));
        //    Style B: Static starter + fluent chaining (no `new`)
        store.put("emp-2", new Employee("bob", "eng", "P222"),
                PassportKeySet.withPassport("P222").department("eng"));
        //    Style C: Lambda / anonymous Consumer block
        store.put("emp-3", new Employee("cara", "ops", "P333"),
                PassportKeySet.of(k -> k.passport("P333").department("ops")));

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
