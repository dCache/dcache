package org.dcache.pool.migration;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

import dmg.cells.nucleus.CellPath;

/**
 * Pool list describing a fixed set of pools. All pools will have the
 * same cost.
 */
public class FixedPoolList implements RefreshablePoolList
{
    private final List<PoolCostPair> _pools;

    public FixedPoolList(List<String> pools)
    {
        List<PoolCostPair> list = new ArrayList();
        for (String pool: pools) {
            list.add(new PoolCostPair(new CellPath(pool), 1.0));
        }
        _pools = Collections.unmodifiableList(list);
    }

    public void refresh()
    {
    }

    public List<PoolCostPair> getPools()
    {
        return _pools;
    }

    public String toString()
    {
        return _pools.toString();
    }
}