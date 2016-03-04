package org.dcache.poolmanager;

import com.google.common.base.Function;

import java.io.Serializable;
import java.security.SecureRandom;
import java.util.List;

import diskCacheV111.pools.PoolCostInfo;

import static java.util.concurrent.TimeUnit.DAYS;

/**
 * Pool selection algorithm using Weighted Available Space Selection (WASS).
 *
 * The weighted available space is defined as:
 *
 *                  scf
 *        available
 *     ----------------------
 *          (pcf writers)
 *        2
 *
 * where available is the unweighted available space, writers the
 * current number of write movers, pcf is the performance cost
 * factor, and scf is the space cost factor.
 *
 * The space cost factor adjusts the preference for using pools by
 * available space. A space cost factor of 0 means that the
 * selection is independent of available space. A value of 1 means
 * that the preference of a pool is proportional to the amount of
 * available space. The higher the value the more the selection is
 * skewed to pools with lots of free space (negative values mean
 * the selection is more skewed towards pools with little free
 * space; that's unlikely to be useful).
 *
 * A selection purely guided by space risks accumulating writers
 * on a pool, eventually causing pools to become overloaded.  To
 * add a feedback from write activity we reduce the available
 * space exponentially with the number of writers.
 *
 * Intuitively the reciprocal of pcf is the number of writers it
 * takes to half the weighted available space.
 *
 * Setting pcf to 0 means the available space will be unweighted, ie
 * load does not affect pool selection. A value of 1 would mean
 * that every write half the available space. The useful range of
 * pcg is probably 0 to 1.
 *
 * The performance cost factor used in the expression is the
 * product of a per pool value and the performance cost factor of
 * the partition. A per pool value makes it possible to specify
 * how quickly a pool degrades with load.
 *
 * Note that setting both factors to zero causes pool selection to
 * become random. This it the same behaviour as with the classic
 * partition.
 */
public class WeightedAvailableSpaceSelection implements Serializable
{
    private static final long serialVersionUID = 6196398425106858164L;

    /* SecureRandom is a higher quality source for randomness than
     * Random.
     */
    protected static final SecureRandom RANDOM = new SecureRandom();

    static final double SECONDS_IN_WEEK = DAYS.toSeconds(7);
    static final double LOG2 = Math.log(2);

    private final double performanceCostFactor;
    private final double spaceCostFactor;

    public WeightedAvailableSpaceSelection(double performanceCostFactor, double spaceCostFactor)
    {
        this.performanceCostFactor = performanceCostFactor;
        this.spaceCostFactor = spaceCostFactor;
    }

    protected double random()
    {
        return RANDOM.nextDouble();
    }

    /**
     * Returns the amount of removable space considered available for writes.
     * <p/>
     * We treat removable space as decaying at an exponential rate. Ie the longer a removable file
     * has not been accessed, the less space we consider it to occupy.
     */
    protected double getAvailableRemovable(PoolCostInfo.PoolSpaceInfo space)
    {
        double removable = space.getRemovableSpace();
        double breakeven = space.getBreakEven();
        double lru = space.getLRUSeconds();

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
            return removable;
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

        return removable - undecayed;
    }

    /**
     * Returns the available space of a pool.
     * <p/>
     * Available space includes free space and removable space deemed available for writes. The gap
     * parameter of the pool is respected.
     */
    protected double getAvailable(PoolCostInfo.PoolSpaceInfo space, long filesize)
    {
        long free = space.getFreeSpace();
        long gap = space.getGap();

        /* If the pool cannot hold the file without eating into the gap, the
         * pool is considered full.
         */
        if (free + space.getRemovableSpace() - filesize <= gap) {
            return 0;
        }

        double removable = getAvailableRemovable(space);

        /* The amount of available space on a pool is the sum of
         * whatever is free and decayed removable space.
         */
        return free + removable;
    }

    protected int getWriters(PoolCostInfo info)
    {
        int writers = 0;
        if (info.getStoreQueue() != null) {
            writers += info.getStoreQueue().getWriters();
        }
        if (info.getRestoreQueue() != null) {
            writers += info.getRestoreQueue().getWriters();
        }
        if (info.getP2pQueue() != null) {
            writers += info.getP2pQueue().getWriters();
        }
        if (info.getP2pClientQueue() != null) {
            writers += info.getP2pClientQueue().getWriters();
        }
        for (PoolCostInfo.PoolQueueInfo queue : info.getExtendedMoverHash().values()) {
            writers += queue.getWriters();
        }
        return writers;
    }

    protected double getWeightedAvailable(PoolCostInfo info, double available, double load)
    {
        return (available == 0) ? 0 : (Math.pow(available, spaceCostFactor) / Math.pow(2.0, load));
    }

    private double getLoad(PoolCostInfo info)
    {
        return performanceCostFactor * info.getMoverCostFactor() * getWriters(info);
    }

    /**
     * Selects a pool from a list using the WASS algorithm.
     * <p/>
     * Returns null if all pools are full.
     */
    public <P> P selectByAvailableSpace(List<P> pools, long filesize,
                                        Function<P, PoolCostInfo> getCost)
    {
        int length = pools.size();
        double[] available = new double[length];

        /* Calculate available space adjusted by space cost factor. Determine the smallest
         * load of all pools able to hold the file.
         */
        double minLoad = Double.POSITIVE_INFINITY;
        for (int i = 0; i < length; i++) {
            PoolCostInfo info = getCost.apply(pools.get(i));
            double free = getAvailable(info.getSpaceInfo(), filesize);
            if (free > 0) {
                available[i] = free;
                minLoad = Math.min(minLoad, getLoad(info));
            }
        }

        if (minLoad == Double.POSITIVE_INFINITY) {
            return null;
        }

        /* Weight available space by normalized load. Load is normalized to ensure that at least
         * for one pool we maintain enough precision to not reduce available space to zero.
         */
        double sum = 0.0;
        for (int i = 0; i < length; i++) {
            PoolCostInfo info = getCost.apply(pools.get(i));
            double normalizedLoad = getLoad(info) - minLoad;
            double weightedAvailable = getWeightedAvailable(info, available[i], normalizedLoad);
            sum += weightedAvailable;
            available[i] = sum;
        }

        /* Randomly choose one of the pools.
         */
        double threshold = random() * sum;
        for (int i = 0; i < length; i++) {
            if (threshold < available[i]) {
                return pools.get(i);
            }
        }

        if (sum == Double.POSITIVE_INFINITY) {
            throw new IllegalStateException("WASS overflow: Configured space cost factor (" + spaceCostFactor + ") is too large.");
        }

        throw new RuntimeException("Unreachable statement.");
    }
}
