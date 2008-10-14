package org.dcache.pool.migration;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;

/**
 * Implements a pool selection strategy in which the pool is selected
 * randomly from the list.
 */
public class RandomPoolSelectionStrategy
    implements PoolSelectionStrategy
{
    private final Random _random = new Random();

    synchronized public PoolCostPair select(List<PoolCostPair> pools)
    {
        return pools.get(_random.nextInt(pools.size()));
    }
}
