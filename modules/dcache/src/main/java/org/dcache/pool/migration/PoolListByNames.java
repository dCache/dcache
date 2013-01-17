package org.dcache.pool.migration;

import java.util.Collection;
import org.dcache.cells.CellStub;
import diskCacheV111.vehicles.PoolManagerGetPoolsByNameMessage;
import diskCacheV111.vehicles.PoolManagerGetPoolsMessage;

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
        _poolManager.send(new PoolManagerGetPoolsByNameMessage(_names),
                          PoolManagerGetPoolsMessage.class,
                          this);
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
