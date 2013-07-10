package org.dcache.poolmanager;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolCostInfo.PoolSpaceInfo;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CostException;
import diskCacheV111.util.DestinationCostException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.SourceCostException;

import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.*;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static java.util.concurrent.TimeUnit.DAYS;

/**
 * Partition that implements the probabilistic weighted available
 * space selection (WASS) algorithm.
 *
 * Experimental. Details will likely change. At the moment only
 * pools to which data is written are selected according to the WASS
 * algorithm. For reads the classic selection algorithm is used.
 */
public class WassPartition extends ClassicPartition
{
    private static final double SECONDS_IN_WEEK = DAYS.toSeconds(7);
    private static final double LOG2 = Math.log(2);

    static final String TYPE = "wass";

    /* SecureRandom is a higher quality source for randomness than
     * Random.
     */
    protected static final SecureRandom _random = new SecureRandom();
    private static final long serialVersionUID = -3587599095801229561L;

    protected transient Function<PoolInfo,String> _getHost;
    protected transient Function<PoolInfo,String> _getName;

    public WassPartition()
    {
        this(NO_PROPERTIES);
    }

    public WassPartition(Map<String,String> inherited)
    {
        this(inherited, NO_PROPERTIES);
    }

    protected WassPartition(Map<String,String> inherited,
                            Map<String,String> properties)
    {
        super(inherited, properties);
        initTransientFields();
    }

    @Override
    protected Partition create(Map<String,String> inherited,
                               Map<String,String> properties)
    {
        return new WassPartition(inherited, properties);
    }

    @Override
    public String getType()
    {
        return TYPE;
    }

    protected double random()
    {
        return _random.nextDouble();
    }

    /**
     * Returns the amount of removable space considered available for
     * writes.
     *
     * We treat removable space as decaying at an expontential
     * rate. Ie the longer a removable file has not been accessed, the
     * less space we consider it to occupy.
     */
    protected double getAvailableRemovable(PoolSpaceInfo space)
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
     *
     * Available space includes free space and removable space deem
     * available for writes. The gap parameter of the pool is
     * respected.
     */
    protected double getAvailable(PoolSpaceInfo space, long filesize)
    {
        long free = space.getFreeSpace();
        long gap = space.getGap();
        double removable = getAvailableRemovable(space);

        /* The amount of available space on a pool is the sum of
         * whatever is free and decayed removable space.
         */
        double available = free + removable;

        /* If available space is less than the gap then the pool is
         * considered full.
         */
        return (available - filesize > gap) ? available : 0;
    }

    /**
     * Returns the available space of a pool weighted by load.
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
     * Intuitively the reciprocal of psc is the number of writers it
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
    protected double getWeightedAvailable(PoolCostInfo info, long filesize)
    {
        double available = getAvailable(info.getSpaceInfo(), filesize);
        double load = _performanceCostFactor *
            info.getMoverCostFactor() * info.getWriters();
        return Math.pow(available, _spaceCostFactor) / Math.pow(2.0, load);
    }

    /**
     * Selects a pool from a list using the WASS algorithm.
     *
     * Returns null if all pools are full.
     */
    protected PoolInfo
        selectByAvailableSpace(List<PoolInfo> pools, long size)
    {
        double[] available = new double[pools.size()];
        double sum = 0.0;

        for (int i = 0; i < available.length; i++) {
            sum += getWeightedAvailable(pools.get(i).getCostInfo(), size);
            available[i] = sum;
        }

        double threshold = random() * sum;

        for (int i = 0; i < available.length; i++) {
            if (threshold < available[i]) {
                return pools.get(i);
            }
        }

        return null;
    }

    @Override
    public PoolInfo selectWritePool(CostModule cm,
                                    List<PoolInfo> pools,
                                    FileAttributes attributes,
                                    long preallocated)
        throws CacheException
    {
        PoolInfo pool = selectByAvailableSpace(pools, preallocated);
        if (pool == null) {
            throw new CacheException(21, "All pools are full");
        }
        return pool;
    }

    /* REVISIT: The current implementation is a mix of the read pool
     * selection from ClassicPartition and write pool selection using
     * WASS. Code can probably be shared with ClassicPartition, but
     * since I hope to refine read pool selection, any code
     * duplication should be temporary and it is not worth refactoring
     * ClassicPartition.
     */
    @Override
    public P2pPair selectPool2Pool(CostModule cm,
                                   List<PoolInfo> src,
                                   List<PoolInfo> dst,
                                   FileAttributes attributes,
                                   boolean force)
        throws CacheException
    {
        checkState(!src.isEmpty());
        checkState(!dst.isEmpty());

        /* The maximum number of replicas can be limited.
         */
        if (src.size() >= _maxPnfsFileCopies) {
            throw new PermissionDeniedCacheException("P2P denied: already too many copies (" + src.size() + ")");
        }

        /* Randomise order of pools with equal cost. In particular
         * important when cost factors are 0.
         */
        Collections.shuffle(src);

        /* Source pools are only selected by performance cost, because
         * we will only read from the pool
         */
        List<PoolCost> sources =
            _byPerformanceCost.sortedCopy(transform(src, toPoolCost()));
        if (!force && isAlertCostExceeded(sources.get(0).performanceCost)) {
            throw new SourceCostException("P2P denied: All source pools are too busy (performance cost > " + _alertCostCut + ")");
        }

        /* The target pool must be below specified cost limits;
         * otherwise we woulnd't be able to read the file afterwards
         * without triggering another p2p.
         */
        double maxTargetCost =
            (_slope > 0.01)
            ? _slope * sources.get(0).performanceCost
            : getCurrentCostCut(cm);
        if (!force && maxTargetCost > 0.0) {
            Predicate<PoolInfo> condition =
                compose(performanceCostIsBelow(maxTargetCost),
                        toPoolCost());
            dst = Lists.newArrayList(filter(dst, condition));
        }

        if (dst.isEmpty()) {
            throw new DestinationCostException("P2P denied: All destination pools are too busy (performance cost > " + maxTargetCost + ")");
        }

        if (_allowSameHostCopy != SameHost.NOTCHECKED) {
            /* Loop over all sources and find the most appropriate
             * destination such that same host constraints are
             * satisfied.
             */
            for (PoolCost source: sources) {
                List<PoolInfo> destinations;
                if (source.host == null) {
                    destinations = dst;
                } else {
                    Predicate<PoolInfo> notSameHost =
                        compose(not(equalTo(source.host)), _getHost);
                    destinations = Lists.newArrayList(filter(dst, notSameHost));
                }

                PoolInfo destination =
                    selectByAvailableSpace(destinations, attributes.getSize());
                if (destination != null) {
                    return new P2pPair(source.pool, destination);
                }
            }

            /* We could not find a pair on different hosts, what now?
             */
            if (_allowSameHostCopy == SameHost.NEVER) {
                throw new PermissionDeniedCacheException("P2P denied: sameHostCopy is 'never' and no matching pool found");
            }
        }

        PoolInfo destination = selectByAvailableSpace(dst, attributes.getSize());
        if (destination == null) {
            throw new DestinationCostException("All pools are full");
        }
        return new P2pPair(sources.get(0).pool, destination);
    }

    private PoolInfo selectByPrevious(List<PoolInfo> pools,
                                      String previousPool,
                                      String previousHost,
                                      FileAttributes attributes)
    {
        Predicate<PoolInfo> notSamePool =
            compose(not(equalTo(previousPool)), _getName);
        if (previousHost != null && _allowSameHostRetry != SameHost.NOTCHECKED) {
            Predicate<PoolInfo> notSameHost =
                compose(not(equalTo(previousHost)), _getHost);
            List<PoolInfo> filteredPools =
                Lists.newArrayList(filter(pools, and(notSamePool, notSameHost)));
            PoolInfo pool = selectByAvailableSpace(filteredPools, attributes.getSize());
            if (pool != null) {
                return pool;
            }
            if (_allowSameHostRetry == SameHost.NEVER) {
                return null;
            }
        }

        if (previousPool != null) {
            List<PoolInfo> filteredPools =
                Lists.newArrayList(filter(pools, notSamePool));
            PoolInfo pool = selectByAvailableSpace(filteredPools, attributes.getSize());
            if (pool != null) {
                return pool;
            }
        }

        return selectByAvailableSpace(pools, attributes.getSize());
    }

    @Override
    public PoolInfo selectStagePool(CostModule cm,
                                    List<PoolInfo> pools,
                                    String previousPool,
                                    String previousHost,
                                    FileAttributes attributes)
        throws CacheException
    {
        if (_fallbackCostCut > 0.0) {
            /* Filter by fallback cost; ensures that the file does not
             * get staged to a pool from which cost prevents us from
             * reading it.
             */
            Predicate<PoolInfo> belowFallback =
                compose(performanceCostIsBelow(_fallbackCostCut),
                        toPoolCost());
            List<PoolInfo> filtered =
                Lists.newArrayList(filter(pools, belowFallback));
            PoolInfo pool =
                selectByPrevious(filtered, previousPool, previousHost, attributes);
            if (pool != null) {
                return pool;
            }

            /* Didn't find a pool. Redo the selection from the full
             * set, but signal that the caller should fall back to
             * other links if possible.
             */
            pool = selectByPrevious(pools, previousPool, previousHost, attributes);
            if (pool == null) {
                throw new CostException("All pools full",
                                        null, true, false);
            } else {
                throw new CostException("Fallback cost exceeded",
                                        pool, true, false);
            }
        } else {
            PoolInfo pool =
                selectByPrevious(pools, previousPool, previousHost, attributes);
            if (pool == null) {
                throw new CostException("All pools full",
                                        null, true, false);
            }
            return pool;
        }
    }

    private void initTransientFields()
    {
        _getHost =
            new Function<PoolInfo,String>()
            {
                @Override
                public String apply(PoolInfo pool) {
                    return pool.getHostName();
                }
            };
        _getName =
            new Function<PoolInfo,String>()
            {
                @Override
                public String apply(PoolInfo pool) {
                    return pool.getName();
                }
            };
    }

    private void readObject(ObjectInputStream stream)
        throws IOException, ClassNotFoundException
    {
        stream.defaultReadObject();
        initTransientFields();
    }
}
