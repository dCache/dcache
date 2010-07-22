package org.dcache.pool.migration;

import java.util.Collection;
import java.util.regex.Pattern;
import org.dcache.cells.CellStub;
import diskCacheV111.vehicles.PoolManagerGetPoolsByNameMessage;
import diskCacheV111.vehicles.PoolManagerGetPoolsMessage;

import org.apache.commons.jexl2.Expression;

public class PoolListByNames
    extends PoolListFromPoolManager
{
    private final CellStub _poolManager;
    private final Collection<String> _names;

    public PoolListByNames(CellStub poolManager,
                           Collection<Pattern> exclude,
                           Expression excludeWhen,
                           Collection<Pattern> include,
                           Expression includeWhen,
                           double spaceFactor,
                           double cpuFactor,
                           Collection<String> pools)
    {
        super(exclude, excludeWhen, include, includeWhen,
              spaceFactor, cpuFactor);
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
        if (_pools.isEmpty())
            return "";

        StringBuilder s = new StringBuilder();
        s.append(_pools.get(0).path.getCellName());
        for (int i = 1; i < _pools.size(); i++) {
            s.append(',').append(_pools.get(i).path.getCellName());
        }
        return s.toString();
    }
}