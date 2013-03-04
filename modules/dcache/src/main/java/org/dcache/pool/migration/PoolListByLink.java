package org.dcache.pool.migration;

import diskCacheV111.vehicles.PoolManagerGetPoolsByLinkMessage;
import diskCacheV111.vehicles.PoolManagerGetPoolsMessage;

import org.dcache.cells.CellStub;

class PoolListByLink
    extends PoolListFromPoolManager
{
    private final CellStub _poolManager;
    private final String _link;

    public PoolListByLink(CellStub poolManager, String link)
    {
        _poolManager = poolManager;
        _link = link;
    }

    @Override
    public void refresh()
    {
        _poolManager.send(new PoolManagerGetPoolsByLinkMessage(_link),
                          PoolManagerGetPoolsMessage.class,
                          this);
    }

    @Override
    public String toString()
    {
        return String.format("link %s, %d pools", _link, _pools.size());
    }
}
