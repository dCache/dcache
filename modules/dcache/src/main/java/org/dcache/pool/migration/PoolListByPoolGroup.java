package org.dcache.pool.migration;

import diskCacheV111.vehicles.PoolManagerGetPoolsByPoolGroupMessage;
import diskCacheV111.vehicles.PoolManagerGetPoolsMessage;

import org.dcache.cells.CellStub;

class PoolListByPoolGroup
    extends PoolListFromPoolManager
{
    private final CellStub _poolManager;
    private final String _poolGroup;

    public PoolListByPoolGroup(CellStub poolManager, String poolGroup)
    {
        _poolManager = poolManager;
        _poolGroup = poolGroup;
    }

    @Override
    public void refresh()
    {
        _poolManager.send(new PoolManagerGetPoolsByPoolGroupMessage(_poolGroup),
                          PoolManagerGetPoolsMessage.class,
                          this);
    }

    @Override
    public String toString()
    {
        return String.format("pool group %s, %d pools",
                             _poolGroup, _pools.size());
    }
}
