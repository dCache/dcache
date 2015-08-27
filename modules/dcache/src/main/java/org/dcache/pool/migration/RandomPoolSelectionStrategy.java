package org.dcache.pool.migration;

import java.util.List;
import java.util.Random;

import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.vehicles.PoolManagerPoolInformation;

import static java.util.stream.Collectors.toList;

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
        List<PoolManagerPoolInformation> nonFullPools =
                pools.stream().filter(RandomPoolSelectionStrategy::hasAvailableSpace).collect(toList());
        if (nonFullPools.isEmpty()) {
            return null;
        }
        return nonFullPools.get(_random.nextInt(nonFullPools.size()));
    }

    private static boolean hasAvailableSpace(PoolManagerPoolInformation pool)
    {
        PoolCostInfo.PoolSpaceInfo info = pool.getPoolCostInfo().getSpaceInfo();
        return info.getFreeSpace() + info.getRemovableSpace() >= info.getGap();
    }
}
