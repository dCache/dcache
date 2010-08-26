package org.dcache.pool.migration;

import java.util.List;

/**
 * A list of pools.
 *
 * Each pool is associated with a cost. The list can be refreshed. The
 * exact definition of refresh is implementation dependant, but the
 * operation may block, and will typically involve fetching the list
 * from another component (e.g. fetching the list of pool in a pool
 * group from the PoolManager).
 */
interface RefreshablePoolList
{
    List<PoolCostPair> getPools();
    void refresh();
}



