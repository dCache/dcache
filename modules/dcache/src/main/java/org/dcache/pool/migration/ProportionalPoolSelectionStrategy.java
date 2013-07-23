package org.dcache.pool.migration;

import com.google.common.base.Function;

import java.util.List;

import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.vehicles.PoolManagerPoolInformation;

import org.dcache.poolmanager.WeightedAvailableSpaceSelection;

/**
 * Selects pools by weighted available space selection (WASS).
 */
public class ProportionalPoolSelectionStrategy
    implements PoolSelectionStrategy
{
    private static final Function<PoolManagerPoolInformation,PoolCostInfo> GET_COST =
            new Function<PoolManagerPoolInformation, PoolCostInfo>()
            {
                @Override
                public PoolCostInfo apply(PoolManagerPoolInformation pool)
                {
                    return pool.getPoolCostInfo();
                }
            };

    private final WeightedAvailableSpaceSelection wass =
            new WeightedAvailableSpaceSelection(1.0, 1.0);

    @Override
    public PoolManagerPoolInformation
        select(List<PoolManagerPoolInformation> pools)
    {
        return wass.selectByAvailableSpace(pools, 0, GET_COST);
    }
}
