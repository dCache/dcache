package org.dcache.pool.migration;

import java.util.List;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;

import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.StickyRecord;

/**
 * MigrationModuleServer message to request that the state of a
 * replica is updated.
 */
public class PoolMigrationUpdateReplicaMessage extends Message
{
    static final long serialVersionUID = 287454169121364779L;

    private final PnfsId _pnfsId;
    private final EntryState _state;
    private final List<StickyRecord> _stickyRecords;
    private final long _ttl;

    public PoolMigrationUpdateReplicaMessage(PnfsId pnfsId, EntryState state,
                                             List<StickyRecord> stickyRecords,
                                             long ttl)
    {
        _pnfsId = pnfsId;
        _state = state;
        _stickyRecords = stickyRecords;
        _ttl = ttl;
    }

    public PnfsId getPnfsId()
    {
        return _pnfsId;
    }

    public EntryState getState()
    {
        return _state;
    }

    public long getTimeToLive()
    {
        return _ttl;
    }

    public List<StickyRecord> getStickyRecords()
    {
        return _stickyRecords;
    }
}