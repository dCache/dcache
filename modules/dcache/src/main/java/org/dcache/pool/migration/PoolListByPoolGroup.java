package org.dcache.pool.migration;

import diskCacheV111.vehicles.PoolManagerGetPoolsByPoolGroupMessage;
import diskCacheV111.vehicles.PoolManagerGetPoolsMessage;

import org.dcache.cells.CellStub;

import static com.google.common.base.Preconditions.checkNotNull;

class PoolListByPoolGroup
    extends PoolListFromPoolManager
{
    private final CellStub _poolManager;
    private final Iterable<String> _poolGroups;

    public PoolListByPoolGroup(CellStub poolManager, Iterable<String> poolGroups)
    {
        _poolManager = checkNotNull(poolManager);
        _poolGroups = checkNotNull(poolGroups);
    }

    @Override
    public void refresh()
    {
        _poolManager.send(new PoolManagerGetPoolsByPoolGroupMessage(_poolGroups),
                          PoolManagerGetPoolsMessage.class,
                          this);
    }

    @Override
    public String toString()
    {
        return String.format("pool groups %s, %d pools",
                             _poolGroups, _pools.size());
    }
}
