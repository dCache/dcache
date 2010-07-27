package org.dcache.pool.migration;

import java.util.List;
import java.util.Random;

import diskCacheV111.vehicles.PoolManagerPoolInformation;

/**
 * Implements cost inversely proportionate selection.
 *
 * Chooses a pool with a probabiliy inversely proportional to the
 * cost.
 *
 * See Wikipedia artile on "Fitness proportionate selection" or
 * "roulette-wheel selection".
 */
public class ProportionalPoolSelectionStrategy
    extends CostFactorPoolSelectionStrategy
{
    private final static Random random = new Random();

    public ProportionalPoolSelectionStrategy(double spaceCostFactor,
                                             double cpuCostFactor)
    {
        super(spaceCostFactor, cpuCostFactor);
    }

    @Override
    public PoolManagerPoolInformation
        select(List<PoolManagerPoolInformation> pools)
    {
        double sum = 0;
        for (PoolManagerPoolInformation pool: pools) {
            sum += 1.0 / cost(pool);
        }

        double threshold = random.nextDouble() * sum;

        sum = 0;
        for (PoolManagerPoolInformation pool: pools) {
            sum += 1.0 / cost(pool);
            if (sum >= threshold) {
                return pool;
            }
        }

        return pools.get(pools.size() - 1);
    }
}
