package org.dcache.pool.migration;

import com.google.common.util.concurrent.MoreExecutors;

import java.util.Collection;

import diskCacheV111.vehicles.PoolManagerGetPoolsByNameMessage;

import org.dcache.cells.CellStub;

public class PoolListByNames
    extends PoolListFromPoolManager
{
    private final CellStub _poolManager;
    private final Collection<String> _names;

    public PoolListByNames(CellStub poolManager, Collection<String> pools)
    {
        _poolManager = poolManager;
        _names = pools;
    }

    @Override
    public void refresh()
    {
        CellStub.addCallback(_poolManager.send(new PoolManagerGetPoolsByNameMessage(_names)),
                             this, MoreExecutors.sameThreadExecutor());
    }

    @Override
    public String toString()
    {
        if (_pools.isEmpty()) {
            return "";
        }

        StringBuilder s = new StringBuilder();
        s.append(_pools.get(0).getName());
        for (int i = 1; i < _pools.size(); i++) {
            s.append(',').append(_pools.get(i).getName());
        }
        return s.toString();
    }
}
