package org.dcache.pool.migration;

import java.util.Collection;
import java.util.regex.Pattern;
import org.dcache.cells.CellStub;
import diskCacheV111.vehicles.PoolManagerGetPoolsByLinkMessage;
import diskCacheV111.vehicles.PoolManagerGetPoolsMessage;

import org.apache.commons.jexl2.Expression;

class PoolListByLink
    extends PoolListFromPoolManager
{
    private final CellStub _poolManager;
    private final String _link;

    public PoolListByLink(CellStub poolManager,
                          Collection<Pattern> exclude,
                          Expression excludeWhen,
                          Collection<Pattern> include,
                          Expression includeWhen,
                          double spaceFactor,
                          double cpuFactor,
                          String link)
    {
        super(exclude, excludeWhen, include, includeWhen,
              spaceFactor, cpuFactor);
        _poolManager = poolManager;
        _link = link;
    }

    public void refresh()
    {
        _poolManager.send(new PoolManagerGetPoolsByLinkMessage(_link),
                          PoolManagerGetPoolsMessage.class,
                          this);
    }

    public String toString()
    {
        return String.format("link %s, %d pools", _link, _pools.size());
    }
}
