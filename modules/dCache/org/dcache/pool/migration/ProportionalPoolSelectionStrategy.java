package org.dcache.pool.migration;

import java.util.List;
import java.util.Random;
import static java.util.concurrent.TimeUnit.*;

import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolCostInfo.PoolSpaceInfo;
import diskCacheV111.vehicles.PoolManagerPoolInformation;

/**
 * Implements proportionate selection over available space in pools.
 *
 * Chooses a pool with a probability proportional to available space.
 * The available space is a combination of free space and
 * exponentially decayed removable space.
 *
 * See Wikipedia article on "Fitness proportionate selection" or
 * "roulette-wheel selection".
 */
public class ProportionalPoolSelectionStrategy
    implements PoolSelectionStrategy
{
    private final static double SECONDS_IN_WEEK = DAYS.toSeconds(7);
    private final static double LOG2 = Math.log(2);

    private final Random _random = new Random();

    private synchronized double random()
    {
        return _random.nextDouble();
    }

    long getAvailable(PoolSpaceInfo space)
    {
        long free = space.getFreeSpace();
        double removable = space.getRemovableSpace();
        double breakeven = space.getBreakEven();
        double lru = space.getLRUSeconds();
        long gap = space.getGap();

        /* We treat removable space as decaying at an expontential
         * rate. Ie the longer a removable file has not been accessed,
         * the less space we consider it to occupy.
         */

        /* Breakeven is traditionally defined as the classic space
         * cost after one week, ie when lru equals one week.
         *
         * We interpret this as a halflife for removable space such
         * that breakeven specifies the undecayed fraction of the
         * least recently accessed removable byte.
         *
         * There is an even older interpretation of breakeven that
         * applies when it is 1.0 or larger. Pool manager used it as a
         * corrective factor for space cost computation. That is not
         * translatable to a halflife and hence we use a constant
         * instead.
         *
         * See also diskCacheV111.pools.CostCalculationV5.
         */
        double halflife;
        if (breakeven >= 1.0) {
            halflife = SECONDS_IN_WEEK * 2;
        } else if (breakeven > 0.0) {
            halflife = SECONDS_IN_WEEK * -LOG2 / Math.log(breakeven);
        } else {
            /* Breakeven of zero means that we don't want to take the
             * age of removable space into account. Hence we just
             * consider it available.
             */
            long available = free + (long) removable;
            return (available > gap) ? available : 0;
        }

        /* The exponential decay process is defined as
         *
         *     N(t) = n * 0.5 ^ (t / T)
         *
         * where T is the halflife and n is the size of the removable
         * file. Ie. at age t only N(t) of a removable file "still
         * exists" (figurably).
         *
         * Ideally we would know the last access time of each
         * removable file on the pool. We do however only know the
         * number of removable bytes, r, and the last access time, l,
         * of the least recently used removable file.
         *
         * We linearly interpolate this data such that the age of the
         * youngest removable byte is zero and the age of the oldest
         * removable byte is lru. That is
         *
         *     age(x) = (l / r) * x
         *
         * for x being the index of a removable byte.
         *
         * Combining the above two expressions gives us:
         *
         *     N(x) = 0.5 ^ (age(x) / T) = 0.5 ^ ((l * x) / (r * T))
         *
         * Here N(x) is the fraction of the x'th byte that isn't
         * decayed yet. Note that the file size, n, is no longer in
         * the expression because we interpolated the age for each
         * byte, not for each file.
         *
         * We now want the definite integral of N from 0 to r. First,
         * the indefinite integral of N is:
         *
         *                r T
         *     -(---------------------)
         *        (l x)/(r T)
         *       2            l Log[2]
         *
         * The definite integral from 0 to r then becomes
         *
         *      r * T * (1 - 2 ^ (-l/T)) / (l * Log(2))
         *
         */
        double undecayed;
        if (lru > 0) {
            undecayed =
                removable * halflife * (1 - Math.pow(2.0, -lru / halflife)) /
                (lru * LOG2);
        } else {
            undecayed = removable;
        }

        double decayed = removable - undecayed;

        /* The amount of available space on a pool is the sum of
         * whatever is free and decayed removable space.
         */
        long available = free + (long) decayed;

        /* If available space is less than the gap then the pool is
         * considered full.
         */
        return (available > gap) ? available : 0;
    }

    @Override
    public PoolManagerPoolInformation
        select(List<PoolManagerPoolInformation> pools)
    {
        long[] available = new long[pools.size()];
        double sum = 0.0;

        for (int i = 0; i < available.length; i++) {
            PoolCostInfo info = pools.get(i).getPoolCostInfo();
            available[i] = getAvailable(info.getSpaceInfo());
            sum += available[i];
        }

        double threshold = random() * sum;

        sum = 0.0;
        for (int i = 0; i < available.length; i++) {
            sum += available[i];
            if (sum >= threshold) {
                return pools.get(i);
            }
        }

        return pools.get(pools.size() - 1);
    }
}
