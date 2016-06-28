package org.dcache.pool.repository.meta.db;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.ClassCatalog;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DiskOrderedCursor;
import com.sleepycat.je.OperationStatus;

import java.util.stream.Collector;

import diskCacheV111.vehicles.StorageInfo;

/**
 * MetaDataRepositoryViews encapsulates creation of views into
 * MetaDataRepositoryDatabase.
 */
class ReplicaStoreView
{
    private final ReplicaStoreDatabase db;
    private final StoredMap<String, StorageInfo> storageInfoMap;
    private final StoredMap<String, CacheRepositoryEntryState> stateMap;
    private final EntryBinding<String> keyBinding;
    private final EntryBinding<StorageInfo> storageInfoBinding;
    private final EntryBinding<CacheRepositoryEntryState> stateBinding;

    public ReplicaStoreView(ReplicaStoreDatabase db)
    {
        this.db = db;
        ClassCatalog catalog = db.getClassCatalog();
        keyBinding =
                new SerialBinding<>(catalog, String.class);
        storageInfoBinding =
                new SerialBinding<>(catalog, StorageInfo.class);
        stateBinding =
                new SerialBinding<>(catalog, CacheRepositoryEntryState.class);
        storageInfoMap =
            new StoredMap<>(db.getStorageInfoDatabase(),
                            keyBinding, storageInfoBinding, true);
        stateMap =
            new StoredMap<>(db.getStateDatabase(),
                            keyBinding, stateBinding, true);
    }

    public final StoredMap<String, StorageInfo> getStorageInfoMap()
    {
        return storageInfoMap;
    }

    public final StoredMap<String, CacheRepositoryEntryState> getStateMap()
    {
        return stateMap;
    }

    public final <A,R> R collectKeys(Collector<String,A,R> collector)
    {
        A accumulator = collector.supplier().get();
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        try (DiskOrderedCursor cursor = db.openKeyCursor()) {
            while (cursor.getNext(key, data, null) == OperationStatus.SUCCESS) {
                collector.accumulator().accept(accumulator, keyBinding.entryToObject(key));
            }
        }
        return collector.finisher().apply(accumulator);
    }
}
