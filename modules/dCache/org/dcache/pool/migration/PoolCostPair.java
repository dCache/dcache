package org.dcache.pool.migration;

import dmg.cells.nucleus.CellPath;

/**
 * Immutable record class used as part of the RefreshablePoolList
 * interface.
 *
 * The record encapsulates a pool name and a cost value.
 */
class PoolCostPair
{
    public final CellPath path;
    public final double cost;

    public PoolCostPair(CellPath path, double cost)
    {
        this.path = path;
        this.cost = cost;
    }
}