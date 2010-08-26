package org.dcache.pool.migration;

import java.util.List;
import java.util.Random;

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
    implements PoolSelectionStrategy
{
    private final static Random random = new Random();

    synchronized public PoolCostPair select(List<PoolCostPair> pools)
    {
        double sum = 0;
        for (PoolCostPair pair: pools) {
            sum += 1.0 / pair.cost;
        }

        double threshold = random.nextDouble() * sum;

        sum = 0;
        for (PoolCostPair pair: pools) {
            sum += 1.0 / pair.cost;
            if (sum >= threshold) {
                return pair;
            }
        }

        return pools.get(pools.size() - 1);
    }
}
