package dev.whitewater;

import dev.whitewater.model.FeedRecord;
import dev.whitewater.model.ReaderStart;
import dev.whitewater.model.RecordStore;

import java.util.function.BiConsumer;

/**
 * Bulk materialization of Feed history into a {@link RecordStore} —
 * returned by {@link WhitewaterClient#loadStore}. The projection callback
 * decides each row's primary key and named-key values:
 *
 * <pre>{@code
 * var store = RecordStore.named("employee-store").addKey("passport");
 * long loaded = client.loadStore(store).fromFeed("hr.employees",
 *         ReaderStart.beginning(),
 *         (s, record) -> s.upsert(record)
 *                 .withKey(record.key())
 *                 .withNamedKey("passport", record.metadata().get("pp")));
 * }</pre>
 */
public final class LoadStore {

    private final WhitewaterClient client;
    private final RecordStore store;

    LoadStore(WhitewaterClient client, RecordStore store) {
        this.client = client;
        this.store = store;
    }

    /**
     * Read committed history of {@code feed} from {@code start}, invoking
     * {@code projection} per record until the frontier is drained; returns
     * the number of records loaded.
     */
    public long fromFeed(
            String feed,
            ReaderStart start,
            BiConsumer<RecordStore, FeedRecord> projection) {
        return client.feeds().load(feed, start, records -> {
            for (FeedRecord record : records) {
                projection.accept(store, record);
            }
        });
    }
}
