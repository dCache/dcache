package org.dcache.pool.migration;

import java.util.List;
import java.util.UUID;

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
    private static final long serialVersionUID = 6328444770149191656L;

    private final StorageInfo _storageInfo;
    private final EntryState _state;
    private final List<StickyRecord> _stickyRecords;
    private final boolean _computeChecksumOnUpdate;

    public PoolMigrationCopyReplicaMessage(UUID uuid, String pool,
                                           PnfsId pnfsId,
                                           StorageInfo storageInfo,
                                           EntryState state,
                                           List<StickyRecord> stickyRecords,
                                           boolean computeChecksumOnUpdate)
    {
        super(uuid, pool, pnfsId);
        _storageInfo = storageInfo;
        _state = state;
        _stickyRecords = stickyRecords;
        _computeChecksumOnUpdate = computeChecksumOnUpdate;
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

    public boolean getComputeChecksumOnUpdate()
    {
        return _computeChecksumOnUpdate;
    }
}