package org.dcache.pool.migration;

import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.vehicles.PoolManagerPoolInformation;

public class SymbolTable extends org.dcache.util.expression.SymbolTable
{
    private static final long serialVersionUID = -1053411895556490661L;

    public void put(String name, PoolManagerPoolInformation info)
    {
        PoolCostInfo cost = info.getPoolCostInfo();
        put(name + ".name", info.getName());
        put(name + ".cpuCost", info.getCpuCost());
        put(name + ".free",
            (cost == null) ? 0 : cost.getSpaceInfo().getFreeSpace());
        put(name + ".total",
            (cost == null) ? 0 : cost.getSpaceInfo().getTotalSpace());
        put(name + ".removable",
            (cost == null) ? 0 : cost.getSpaceInfo().getRemovableSpace());
        put(name + ".used",
            (cost == null) ? 0 : cost.getSpaceInfo().getUsedSpace());
    }
}
