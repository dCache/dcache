package org.dcache.pool.migration;

import diskCacheV111.vehicles.PoolManagerPoolInformation;

import com.google.common.collect.ImmutableList;

/**
 * A list of pools.
 *
 * Each pool is described by a PoolManagerPoolInformation
 * instance. The list can be refreshed.
 */
public interface RefreshablePoolList
{
    /**
     * Whether information about pools is available. Since information
     * may be fetched asynchronously after a refresh it may be
     * unavailable at first.
     */
    boolean isValid();

    /**
     * Returns information about pools in the list.
     */
    ImmutableList<PoolManagerPoolInformation> getPools();

    /**
     * Initiates a refresh. The exact semantics of refresh is
     * implementation dependant, but the operation may or may not
     * block, and will typically involve fetching the list from
     * another component (e.g. fetching the list of pools in a pool
     * group from the PoolManager).
     */
    void refresh();
}



