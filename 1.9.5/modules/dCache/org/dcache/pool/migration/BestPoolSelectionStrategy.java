package org.dcache.pool.migration;

import java.util.List;
import java.util.Comparator;
import java.util.Collections;

/**
 * Pool selection strategy selecting the pool with the lowest cost.
 */
public class BestPoolSelectionStrategy
    implements PoolSelectionStrategy
{
    private final static Comparator<PoolCostPair> comparator =
        new Comparator<PoolCostPair>()
        {
            public int compare(PoolCostPair o1, PoolCostPair o2)
            {
                return (int)Math.signum(o1.cost - o2.cost);
            }
        };

    synchronized public PoolCostPair select(List<PoolCostPair> pools)
    {
        return Collections.min(pools, comparator);
    }
}
