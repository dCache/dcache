package org.dcache.poolmanager;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.pools.PoolCostInfo;
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
    static final String TYPE = "wass";

    private static final long serialVersionUID = -3587599095801229561L;

    private static final Function<PoolInfo,PoolCostInfo> GET_COST =
            new Function<PoolInfo, PoolCostInfo>()
            {
                @Override
                public PoolCostInfo apply(PoolInfo pool)
                {
                    return pool.getCostInfo();
                }
            };

    private static final Function<PoolInfo,String> GET_HOST =
            new Function<PoolInfo,String>()
            {
                @Override
                public String apply(PoolInfo pool) {
                    return pool.getHostName();
                }
            };

    private static final Function<PoolInfo,String> GET_NAME =
            new Function<PoolInfo,String>()
            {
                @Override
                public String apply(PoolInfo pool) {
                    return pool.getName();
                }
            };

    private final WeightedAvailableSpaceSelection wass;

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
        wass = new WeightedAvailableSpaceSelection(_performanceCostFactor, _spaceCostFactor);
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

    @Override
    public PoolInfo selectWritePool(CostModule cm,
                                    List<PoolInfo> pools,
                                    FileAttributes attributes,
                                    long preallocated)
        throws CacheException
    {
        PoolInfo pool = wass.selectByAvailableSpace(pools, preallocated, GET_COST);
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
         * otherwise we wouldn't be able to read the file afterwards
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
                        compose(not(equalTo(source.host)), GET_HOST);
                    destinations = Lists.newArrayList(filter(dst, notSameHost));
                }

                PoolInfo destination =
                        wass.selectByAvailableSpace(destinations, attributes.getSize(), GET_COST);
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

        PoolInfo destination = wass.selectByAvailableSpace(dst, attributes.getSize(), GET_COST);
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
            compose(not(equalTo(previousPool)), GET_NAME);
        if (previousHost != null && _allowSameHostRetry != SameHost.NOTCHECKED) {
            Predicate<PoolInfo> notSameHost =
                compose(not(equalTo(previousHost)), GET_HOST);
            List<PoolInfo> filteredPools =
                Lists.newArrayList(filter(pools, and(notSamePool, notSameHost)));
            PoolInfo pool = wass
                    .selectByAvailableSpace(filteredPools, attributes.getSize(), GET_COST);
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
            PoolInfo pool = wass.selectByAvailableSpace(filteredPools, attributes.getSize(), GET_COST);
            if (pool != null) {
                return pool;
            }
        }

        return wass.selectByAvailableSpace(pools, attributes.getSize(), GET_COST);
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
}
