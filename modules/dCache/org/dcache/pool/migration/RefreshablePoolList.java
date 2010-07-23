package org.dcache.pool.migration;

import org.dcache.util.ImmutableList;
import diskCacheV111.vehicles.PoolManagerPoolInformation;

/**
 * A list of pools.
 *
 * Each pool is described by a PoolManagerPoolInformation
 * instance. The list can be refreshed. The exact definition of
 * refresh is implementation dependant, but the operation may block,
 * and will typically involve fetching the list from another component
 * (e.g. fetching the list of pools in a pool group from the
 * PoolManager).
 */
public interface RefreshablePoolList
{
    ImmutableList<PoolManagerPoolInformation> getPools();
    void refresh();
}



