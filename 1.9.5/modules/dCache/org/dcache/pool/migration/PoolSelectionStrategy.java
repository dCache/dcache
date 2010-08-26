package org.dcache.pool.migration;

import java.util.List;

/**
 * A strategy for selecting a pool from a list.
 */
public interface PoolSelectionStrategy
{
    PoolCostPair select(List<PoolCostPair> pools);
}