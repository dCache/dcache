package org.dcache.pool.repository.meta.db;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.ClassCatalog;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.collections.StoredMap;

import diskCacheV111.vehicles.StorageInfo;

import org.dcache.pool.repository.StickyRecord;

import java.util.Map;
import java.io.PrintWriter;
import java.net.URI;

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

    public void toYaml(PrintWriter out, PrintWriter error)
    {
        for (Object id : getStateMap().keySet()) {
            try {
                CacheRepositoryEntryState state =
                    (CacheRepositoryEntryState)getStateMap().get(id);
                StorageInfo info =
                    (StorageInfo)getStorageInfoMap().get(id);

                out.format("%s:\n", id);
                out.format("  state: %s\n", state.toString());
                out.format("  sticky:\n");
                for (StickyRecord record : state.stickyRecords()) {
                    out.format("    %s: %d\n", record.owner(), record.expire());
                }
                if (info != null) {
                    out.format("  storageclass: %s\n", info.getStorageClass());
                    out.format("  cacheclass: %s\n", info.getCacheClass());
                    out.format("  bitfileid: %s\n", info.getBitfileId());
                    out.format("  locations:\n");
                    for (URI location : info.locations()) {
                        out.format("    - %s\n", location);
                    }
                    out.format("  hsm: %s\n", info.getHsm());
                    out.format("  filesize: %s\n", info.getFileSize());
                    out.format("  map:\n");
                    for (Map.Entry<String,String> entry : info.getMap().entrySet()) {
                        out.format("    %s: %s\n", entry.getKey(), entry
                                .getValue());
                    }
                    out.format("  retentionpolicy: %s\n", info.getRetentionPolicy());
                    out.format("  accesslatency: %s\n", info.getAccessLatency());
                }
            } catch (Throwable e) {
                error.println("Failed to read " + id + ": " + e.getMessage());
            }
        }
        out.flush();
        error.flush();
    }
}
