package org.dcache.pool.migration;

import diskCacheV111.vehicles.PoolManagerPoolInformation;
import java.util.List;
import org.dcache.poolmanager.WeightedAvailableSpaceSelection;

/**
 * Selects pools by weighted available space selection (WASS).
 */
public class ProportionalPoolSelectionStrategy
      implements PoolSelectionStrategy {

    private final WeightedAvailableSpaceSelection wass =
          new WeightedAvailableSpaceSelection(1.0, 1.0);

    @Override
    public PoolManagerPoolInformation
    select(List<PoolManagerPoolInformation> pools) {
        return wass.selectByAvailableSpace(pools, 0, PoolManagerPoolInformation::getPoolCostInfo);
    }
}
