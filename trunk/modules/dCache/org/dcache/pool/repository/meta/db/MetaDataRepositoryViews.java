package org.dcache.pool.repository.meta.db;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.ClassCatalog;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.collections.StoredMap;

import diskCacheV111.vehicles.StorageInfo;

/**
 * MetaDataRepositoryViews encapsulates creation of views into
 * MetaDataRepositoryDatabase. 
 */ 
class MetaDataRepositoryViews
{
    private StoredMap storageInfoMap;
    private StoredMap stateMap;     

    public MetaDataRepositoryViews(MetaDataRepositoryDatabase db)
    {
        ClassCatalog catalog = db.getClassCatalog();
        EntryBinding stringBinding =
            new SerialBinding(catalog, String.class);
        EntryBinding storageInfoBinding =
            new SerialBinding(catalog, StorageInfo.class);
        EntryBinding stateBinding =
            new SerialBinding(catalog, CacheRepositoryEntryState.class);

        storageInfoMap =
            new StoredMap(db.getStorageInfoDatabase(),
                          stringBinding, storageInfoBinding, true);
        stateMap =
            new StoredMap(db.getStateDatabase(),
                          stringBinding, stateBinding, true);
    }

    public final StoredMap getStorageInfoMap()
    {
        return storageInfoMap;
    }

    public final StoredMap getStateMap()
    {
        return stateMap;
    }
}
