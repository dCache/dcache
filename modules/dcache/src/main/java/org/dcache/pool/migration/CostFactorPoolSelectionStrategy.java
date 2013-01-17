package org.dcache.pool.migration;

import diskCacheV111.vehicles.PoolManagerPoolInformation;

public abstract class CostFactorPoolSelectionStrategy
    implements PoolSelectionStrategy
{
    private final double _spaceCostFactor;
    private final double _cpuCostFactor;

    public CostFactorPoolSelectionStrategy(double spaceCostFactor,
                                           double cpuCostFactor)
    {
        _spaceCostFactor = spaceCostFactor;
        _cpuCostFactor = cpuCostFactor;
    }

    protected double cost(PoolManagerPoolInformation pool)
    {
        double space =
            (_spaceCostFactor > 0 ? pool.getSpaceCost() * _spaceCostFactor : 0);
        double cpu =
            (_cpuCostFactor > 0 ? pool.getCpuCost() * _cpuCostFactor : 0);
        return space + cpu;
    }
}
