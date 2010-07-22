package org.dcache.pool.migration;

import diskCacheV111.vehicles.PoolManagerPoolInformation;
import diskCacheV111.pools.PoolCostInfo;

/**
 * A facade for PoolManagerPoolInformation exposing various
 * information about a pool. The only purpose is to allow those values
 * to be injected into a JexlContext for reference from user
 * expressions.
 */
public class PoolValues
{
    private final PoolManagerPoolInformation _info;

    public PoolValues(PoolManagerPoolInformation info)
    {
        _info = info;
    }

    public String getName()
    {
        return _info.getName();
    }

    public double getSpaceCost()
    {
        return _info.getSpaceCost();
    }

    public double getCpuCost()
    {
        return _info.getCpuCost();
    }

    public long getFree()
    {
        PoolCostInfo cost = _info.getPoolCostInfo();
        return (cost == null) ? 0 : cost.getSpaceInfo().getFreeSpace();
    }

    public long getTotal()
    {
        PoolCostInfo cost = _info.getPoolCostInfo();
        return (cost == null) ? 0 : cost.getSpaceInfo().getTotalSpace();
    }

    public long getRemovable()
    {
        PoolCostInfo cost = _info.getPoolCostInfo();
        return (cost == null) ? 0 : cost.getSpaceInfo().getRemovableSpace();
    }

    public long getUsed()
    {
        PoolCostInfo cost = _info.getPoolCostInfo();
        return (cost == null) ? 0 : cost.getSpaceInfo().getUsedSpace();
    }
}