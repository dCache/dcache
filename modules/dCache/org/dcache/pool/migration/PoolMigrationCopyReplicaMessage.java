package org.dcache.pool.migration;

import java.util.List;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.StickyRecord;

/**
 * MigrationModuleServer message to request that a replica is
 * transfered.
 */
public class PoolMigrationCopyReplicaMessage extends PoolMigrationMessage
{
    static final long serialVersionUID = 6328444770149191656L;

    private final StorageInfo _storageInfo;
    private final EntryState _state;
    private final List<StickyRecord> _stickyRecords;

    public PoolMigrationCopyReplicaMessage(String pool, PnfsId pnfsId, long taskId,
                                           StorageInfo storageInfo,
                                           EntryState state,
                                           List<StickyRecord> stickyRecords)
    {
        super(pool, pnfsId, taskId);
        _storageInfo = storageInfo;
        _state = state;
        _stickyRecords = stickyRecords;
    }

    public StorageInfo getStorageInfo()
    {
        return _storageInfo;
    }

    public EntryState getState()
    {
        return _state;
    }

    public List<StickyRecord> getStickyRecords()
    {
        return _stickyRecords;
    }
}