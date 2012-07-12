package org.dcache.pool.classic;

import java.util.List;
import java.util.Collections;

import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.StickyRecord;

import diskCacheV111.vehicles.StorageInfo;

/**
 * A ReplicaStatePolicy for volatile pools. Files are marked CACHED
 * and not sticky.
 */
public class VolatileReplicaStatePolicy implements ReplicaStatePolicy
{
    @Override
    public List<StickyRecord> getStickyRecords(StorageInfo info)
    {
        return Collections.emptyList();
    }

    @Override
    public EntryState getTargetState(StorageInfo info)
    {
        return EntryState.CACHED;
    }
}