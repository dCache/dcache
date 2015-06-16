package org.dcache.pool.migration;

import com.google.common.base.Predicate;

import java.util.List;
import java.util.Random;

import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.vehicles.PoolManagerPoolInformation;

import static com.google.common.collect.Iterables.*;

/**
 * Implements a pool selection strategy in which the pool is selected
 * randomly from the list.
 */
public class RandomPoolSelectionStrategy
    implements PoolSelectionStrategy
{
    private final Random _random = new Random();

    @Override
    public PoolManagerPoolInformation
        select(List<PoolManagerPoolInformation> pools)
    {
        Iterable<PoolManagerPoolInformation> nonFullPools =
                filter(pools, new Predicate<PoolManagerPoolInformation>()
                {
                    @Override
                    public boolean apply(PoolManagerPoolInformation pool)
                    {
                        PoolCostInfo.PoolSpaceInfo info = pool.getPoolCostInfo().getSpaceInfo();
                        return info.getFreeSpace() + info.getRemovableSpace() >= info.getGap();
                    }
                });
        if (isEmpty(nonFullPools)) {
            return null;
        }
        return get(nonFullPools, _random.nextInt(size(nonFullPools)));
    }
}
