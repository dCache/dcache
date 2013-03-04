package org.dcache.pool.migration;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import diskCacheV111.vehicles.PoolManagerPoolInformation;

/**
 * Pool selection strategy selecting the pool with the lowest cost.
 */
public class BestPoolSelectionStrategy
    extends CostFactorPoolSelectionStrategy
{
    public BestPoolSelectionStrategy(double spaceCostFactor,
                                     double cpuCostFactor)
    {
        super(spaceCostFactor, cpuCostFactor);
    }

    private final Comparator<PoolManagerPoolInformation> comparator =
        new Comparator<PoolManagerPoolInformation>()
        {
            @Override
            public int compare(PoolManagerPoolInformation p1,
                               PoolManagerPoolInformation p2)
            {
                return (int) Math.signum(cost(p1) - cost(p2));
            }
        };

    @Override
    public PoolManagerPoolInformation
        select(List<PoolManagerPoolInformation> pools)
    {
        return Collections.min(pools, comparator);
    }
}
