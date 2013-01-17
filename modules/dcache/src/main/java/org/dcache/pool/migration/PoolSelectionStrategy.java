package org.dcache.pool.migration;

import java.util.List;

import diskCacheV111.vehicles.PoolManagerPoolInformation;

/**
 * A strategy for selecting a pool from a list.
 */
public interface PoolSelectionStrategy
{
    PoolManagerPoolInformation select(List<PoolManagerPoolInformation> pools);
}
