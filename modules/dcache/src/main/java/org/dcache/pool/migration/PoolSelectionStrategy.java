package org.dcache.pool.migration;

import javax.annotation.Nullable;

import java.util.List;

import diskCacheV111.vehicles.PoolManagerPoolInformation;

/**
 * A strategy for selecting a pool from a list.
 */
public interface PoolSelectionStrategy
{
    @Nullable
    PoolManagerPoolInformation select(List<PoolManagerPoolInformation> pools);
}
