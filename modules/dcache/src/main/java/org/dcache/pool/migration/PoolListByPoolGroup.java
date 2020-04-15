package org.dcache.pool.migration;

import com.google.common.util.concurrent.MoreExecutors;

import diskCacheV111.vehicles.PoolManagerGetPoolsByPoolGroupMessage;

import org.dcache.cells.CellStub;

import static java.util.Objects.requireNonNull;

class PoolListByPoolGroup
    extends PoolListFromPoolManager
{
    private final CellStub _poolManager;
    private final Iterable<String> _poolGroups;

    public PoolListByPoolGroup(CellStub poolManager, Iterable<String> poolGroups)
    {
        _poolManager = requireNonNull(poolManager);
        _poolGroups = requireNonNull(poolGroups);
    }

    @Override
    public void refresh()
    {
        CellStub.addCallback(_poolManager.send(new PoolManagerGetPoolsByPoolGroupMessage(_poolGroups)),
                             this, MoreExecutors.directExecutor());
    }

    @Override
    public String toString()
    {
        return String.format("pool groups %s, %d pools",
                             _poolGroups, _pools.size());
    }
}
