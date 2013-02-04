package org.dcache.pool.migration;

import java.util.List;
import java.util.Random;

import diskCacheV111.vehicles.PoolManagerPoolInformation;

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
        return pools.get(_random.nextInt(pools.size()));
    }
}
