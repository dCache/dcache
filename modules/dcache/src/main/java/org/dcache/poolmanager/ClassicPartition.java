package org.dcache.poolmanager;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.pools.CostCalculatable;
import diskCacheV111.pools.CostCalculationV5;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CostException;
import diskCacheV111.util.DestinationCostException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.SourceCostException;

import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;

/**
 * Partition that provides classic dCache pool selection semantics.
 *
 */
public class ClassicPartition extends Partition
{
    private static final long serialVersionUID = 8239030345609342048L;

    static final String TYPE = "classic";

    private static final double MAX_WRITE_COST = 1000000.0;

    public enum SameHost { NEVER, BESTEFFORT, NOTCHECKED }

    /**
     * COSTFACTORS
     *
     *   spacecostfactor   double
     *   cpucostfactor     double
     *
     * COSTCUTS
     *
     *   idle      double
     *   p2p       double
     *   alert     double
     *   halt      double
     *   slope     double
     *   fallback  double
     *
     * OTHER
     *   max-copies     int
     *
     *    Options  |  Description
     *  -------------------------------------------------------------------
     *      idle   |  below 'idle' : 'reduce duplicate' mode
     *      p2p    |  above : start pool to pool mode
     *             |  If p2p value is a percent then p2p is dynamically
     *             |  assigned that percentile value of pool performance costs
     *      alert  |  stop pool 2 pool mode, start stage only mode
     *      halt   |  suspend system
     *    fallback |  Allow fallback in Permission matrix on high load
     */
    private final static Map<String,String> DEFAULTS =
        ImmutableMap.<String,String>builder()
        .put("max-copies", "3")
        .put("p2p", "0.0")
        .put("alert", "0.0")
        .put("halt", "0.0")
        .put("fallback", "0.0")
        .put("spacecostfactor", "1.0")
        .put("cpucostfactor", "1.0")
        .put("sameHostCopy", "besteffort")
        .put("sameHostRetry", "besteffort")
        .put("slope", "0.0")
        .put("idle", "0.0")
        .build();

    public final SameHost _allowSameHostCopy;
    public final SameHost _allowSameHostRetry;
    public final long _maxPnfsFileCopies;

    public final double  _costCut;
    public final boolean _costCutIsPercentile;
    public final double  _alertCostCut;
    public final double  _panicCostCut;
    public final double  _fallbackCostCut;

    public final double  _spaceCostFactor;
    public final double  _performanceCostFactor;

    public final double  _slope;
    public final double  _minCostCut;

    /**
     * Order by performance cost.
     */
    protected transient Ordering<PoolCost> _byPerformanceCost;

    /**
     * Order by linear combination of performance cost and space cost.
     */
    protected transient Ordering<PoolCost> _byFullCost;

    public ClassicPartition()
    {
        this(NO_PROPERTIES);
    }

    public ClassicPartition(Map<String,String> inherited)
    {
        this(inherited, NO_PROPERTIES);
    }

    protected ClassicPartition(Map<String,String> inherited,
                               Map<String,String> properties)
    {
        this(DEFAULTS, inherited, properties);
    }

    protected ClassicPartition(Map<String,String> defaults,
                               Map<String,String> inherited,
                               Map<String,String> properties)
    {
        super(defaults, inherited, properties);

        initTransientFields();

        _allowSameHostCopy =
            SameHost.valueOf(getProperty("sameHostCopy").toUpperCase());
        _allowSameHostRetry =
            SameHost.valueOf(getProperty("sameHostRetry").toUpperCase());
        _maxPnfsFileCopies = getLong("max-copies");
        _alertCostCut = getDouble("alert");
        _panicCostCut = getDouble("halt");
        _fallbackCostCut = getDouble("fallback");
        _spaceCostFactor = getDouble("spacecostfactor");
        _performanceCostFactor = getDouble("cpucostfactor");
        _slope = getDouble("slope");
        _minCostCut = getDouble("idle");

        String costCut = getProperty("p2p");
        if (costCut.endsWith("%")) {
            String numberPart = costCut.substring(0, costCut.length() - 1);
            _costCut = Double.parseDouble(numberPart) / 100;
            _costCutIsPercentile = true;
            if (_costCut <= 0) {
                throw new IllegalArgumentException("Number " + _costCut + " is too small; must be > 0%");
            }
            if (_costCut >= 1) {
                throw new IllegalArgumentException("Number " + _costCut + " is too large; must be < 100%");
            }
        } else {
            _costCut = Double.parseDouble(costCut);
            _costCutIsPercentile = false;
        }
    }

    @Override
    protected Partition create(Map<String,String> inherited,
                               Map<String,String> properties)
    {
        return new ClassicPartition(inherited, properties);
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
        checkState(!pools.isEmpty());

        /* Randomise order of pools with equal cost. In particular
         * important when performance cost factor and space cost
         * factor are 0.
         */
        Collections.shuffle(pools);

        /* We use the cheapest pool according to the combined space
         * and performance cost.
         */
        PoolCost best =
            _byFullCost.min(transform(pools, toPoolCost(preallocated)));

        /* Note that
         *
         *    !(bestCost  <= MAX_WRITE_COST)  != (bestCost > MAX_WRITE_COST)
         *
         * when using floating point arithmetic!
         */
        double cost = getWeightedFullCost(best);
        if (!(cost <= MAX_WRITE_COST)) {
            throw new CacheException(21, "Best pool <" + best.pool.getName() +
                                     "> too high : " + cost);
        }

        return best.pool;
    }

    @Override
    public PoolInfo selectReadPool(CostModule cm,
                                   List<PoolInfo> pools,
                                   FileAttributes attributes)
        throws CacheException
    {
        checkState(!pools.isEmpty());

        /* Randomise order of pools with equal cost. In particular
         * important when performance cost factor and space cost
         * factor are 0.
         */
        Collections.shuffle(pools);

        /* TODO: We could possibly define an Ordering and do a regular
         * min call.
         */
        PnfsId pnfsId = attributes.getPnfsId();
        double bestCost = Double.POSITIVE_INFINITY;
        PoolInfo bestPool = null;
        for (PoolInfo pool: pools) {
            CostCalculatable calculatable =
                new CostCalculationV5(pool.getCostInfo());
            calculatable.recalculate(0);

            double cost =
                Math.abs(_performanceCostFactor *
                         calculatable.getPerformanceCost());
            if (bestCost >= _minCostCut && cost <= bestCost) {
                /* As long as at least one of the pools is above min
                 * cost cut, we search for the pool with the lowest
                 * cost.
                 */
                bestCost = cost;
                bestPool = pool;
            } else if (cost < _minCostCut &&
                       bestCost < _minCostCut &&
                       minCostCutPosition(pnfsId, pool) <
                       minCostCutPosition(pnfsId, bestPool)) {
                /* Once both pools are below cost cust the order
                 * changes to an arbitrary but deterministic order.
                 *
                 * The goal is that for pools with low load we always
                 * select the same pool to allow other replicas to
                 * age.
                 */
                bestCost = cost;
                bestPool = pool;
            }
        }

        CostCalculatable calculatable =
            new CostCalculationV5(bestPool.getCostInfo());
        calculatable.recalculate(0);
        double cost = calculatable.getPerformanceCost();
        boolean isPanicCostExceeded = isPanicCostExceeded(cost);
        boolean isFallbackCostExceeded = isFallbackCostExceeded(cost);
        boolean isCostCutExceeded = isCostCutExceeded(cm, cost);
        if (isPanicCostExceeded) {
            throw new CostException("Cost limit exceeded", null,
                                    isFallbackCostExceeded, isCostCutExceeded);
        }
        if (isFallbackCostExceeded || isCostCutExceeded) {
            throw new CostException("Cost limit exceeded", bestPool,
                                    isFallbackCostExceeded, isCostCutExceeded);
        }

        return bestPool;
    }

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
         * important when performance cost factor and space cost
         * factor are 0.
         */
        Collections.shuffle(src);
        Collections.shuffle(dst);

        /* Source pools are only selected by performance cost, because
         * we will only read from the pool
         */
        List<PoolCost> sources =
            _byPerformanceCost.sortedCopy(transform(src, toPoolCost(0)));
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
        Iterable<PoolCost> unsortedDestinations =
            transform(dst, toPoolCost(attributes.getSize()));
        if (!force && maxTargetCost > 0.0) {
            unsortedDestinations =
                filter(unsortedDestinations,
                       performanceCostIsBelow(maxTargetCost));
        }

        List<PoolCost> destinations =
            _byFullCost.sortedCopy(unsortedDestinations);

        if (destinations.isEmpty()) {
            throw new DestinationCostException("P2P denied: All destination pools are too busy (performance cost > " + maxTargetCost + ")");
        }

        /* Loop over all (source,destination) combinations and find
         * the most appropriate for source.host != destination.host.
         */
        PoolCost sourcePool = null;
        PoolCost destinationPool = null;
        PoolCost bestEffortSourcePool = null;
        PoolCost bestEffortDestinationPool = null;
        outer: for (PoolCost source: sources) {
            for (PoolCost destination: destinations) {
                if (_allowSameHostCopy == SameHost.NOTCHECKED) {
                    // we take the pair with the least cost without
                    // further hostname checking
                    sourcePool = source;
                    destinationPool = destination;
                    break outer;
                }

                // save the pair with the least cost for later reuse
                if (bestEffortSourcePool == null) {
                    bestEffortSourcePool = source;
                }
                if (bestEffortDestinationPool == null) {
                    bestEffortDestinationPool = destination;
                }

                if (source.host != null && !source.host.equals(destination.host)) {
                    // we take the first src/dest-pool pair not
                    // residing on the same host
                    sourcePool = source;
                    destinationPool = destination;
                    break outer;
                }
            }
        }

        if (sourcePool == null || destinationPool == null) {
            // ok, we could not find a pair on different hosts, what now?
            switch (_allowSameHostCopy) {
            case BESTEFFORT:
                sourcePool = bestEffortSourcePool;
                destinationPool = bestEffortDestinationPool;
                break;
            case NEVER:
                throw new PermissionDeniedCacheException("P2P denied: sameHostCopy is 'never' and no matching pool found");
            default:
                throw new RuntimeException("P2P denied: coding error, bad state");
            }
        }

        return new P2pPair(sourcePool.pool, destinationPool.pool);
    }

    @Override
    public PoolInfo selectStagePool(CostModule cm,
                                    List<PoolInfo> pools,
                                    String previousPool,
                                    String previousHost,
                                    FileAttributes attributes)
        throws CacheException
    {
        checkState(!pools.isEmpty());

        /* Randomise order of pools with equal cost. In particular
         * important when performance cost factor and space cost
         * factor are 0.
         */
        Collections.shuffle(pools);

        /* We order by full cost, except that we place the previously
         * used pool or pools on the same host last.
         */
        List<Ordering<PoolCost>> order = Lists.newArrayListWithCapacity(3);
        if (previousPool != null) {
            order.add(thisPoolLast(previousPool));
        }
        if (previousHost != null && _allowSameHostRetry != SameHost.NOTCHECKED) {
            order.add(thisHostLast(previousHost));
        }
        order.add(_byFullCost);

        long size = attributes.getSize();
        PoolCost best =
            Ordering.compound(order).min(transform(pools, toPoolCost(size)));

        if (_allowSameHostRetry == SameHost.NEVER &&
            previousHost != null && previousHost.equals(best.host)) {
            throw new CacheException(150 , "No cheap candidates available for stage");
        }

        if (isFallbackCostExceeded(best.performanceCost)) {
            throw new CostException("Cost limit exceeded",
                                    null, true, false);
        }
        return best.pool;
    }

    /**
     * Returns a hash of pnfsId and pool.
     */
    protected int minCostCutPosition(PnfsId pnfsId, PoolInfo pool)
    {
        return (pnfsId.toString() + pool.getName()).hashCode();
    }

    /**
     * Establish the costCut at the moment for the given set of parameters.
     * If the costCut was assigned a value from a string ending with a '%' then
     * that percentile cost is used.
     * @param cm current CostModule
     * @return the costCut, taking into account possible relative costCut.
     */
    protected double getCurrentCostCut(CostModule cm)
    {
        return _costCutIsPercentile
            ? cm.getPoolsPercentilePerformanceCost(_costCut)
            : _costCut;
    }

    protected boolean isPanicCostExceeded(double cost)
    {
        return (_panicCostCut > 0.0 && cost > _panicCostCut);
    }

    protected boolean isFallbackCostExceeded(double cost)
    {
        return (_fallbackCostCut > 0.0 && cost > _fallbackCostCut);
    }

    protected boolean isCostCutExceeded(CostModule cm, double cost)
    {
        return (_costCut > 0.0 && cost >= getCurrentCostCut(cm));
    }

    protected boolean isAlertCostExceeded(double cost)
    {
        return (_alertCostCut > 0.0 && cost > _alertCostCut);
    }

    /**
     * Internal immutable helper class to hold precomputed performance
     * and space cost.
     */
    protected static class PoolCost
    {
        final PoolInfo pool;
        final double performanceCost;
        final double spaceCost;
        final String host;

        public PoolCost(PoolInfo pool, CostCalculatable cost)
        {
            this(pool, cost.getPerformanceCost(), cost.getSpaceCost());
        }

        public PoolCost(PoolInfo pool, double performanceCost, double spaceCost)
        {
            this.pool = pool;
            this.performanceCost = performanceCost;
            this.spaceCost = spaceCost;
            this.host = pool.getHostName();
        }
    }

    protected double getWeightedFullCost(PoolCost cost)
    {
        return Math.abs(cost.spaceCost) * _spaceCostFactor +
            Math.abs(cost.performanceCost) * _performanceCostFactor;
    }

    protected double getWeightedPerformanceCost(PoolCost cost)
    {
        return Math.abs(cost.performanceCost) * _performanceCostFactor;
    }

    protected Function<PoolInfo,PoolCost> toPoolCost(final long filesize) {
        return new Function<PoolInfo,PoolCost>() {
            @Override
            public PoolCost apply(PoolInfo pool) {
                CostCalculatable calculatable =
                    new CostCalculationV5(pool.getCostInfo());
                calculatable.recalculate(filesize);
                return new PoolCost(pool, calculatable);
            }
        };
    }

    protected Predicate<PoolCost> performanceCostIsBelow(final double max)
    {
        return new Predicate<PoolCost>() {
            @Override
            public boolean apply(PoolCost cost) {
                return cost.performanceCost < max;
            }
        };
    }


    protected Ordering<PoolCost> thisPoolLast(final String name)
    {
        return new Ordering<PoolCost>() {
            @Override
            public int compare(PoolCost a, PoolCost b) {
                if (!a.pool.getName().equals(b.pool.getName())) {
                    if (a.pool.getName().equals(name)) {
                        return 1;
                    }
                    if (b.pool.getName().equals(name)) {
                        return -1;
                    }
                }
                return 0;
            }
        };
    }

    protected Ordering<PoolCost> thisHostLast(final String host)
    {
        return new Ordering<PoolCost>() {
            @Override
            public int compare(PoolCost a, PoolCost b) {
                if (!Objects.equal(a.host, b.host)) {
                    if (Objects.equal(a.host, host)) {
                        return 1;
                    }
                    if (Objects.equal(b.host, host)) {
                        return -1;
                    }
                }
                return 0;
            }
        };
    }

    private void initTransientFields()
    {
        _byPerformanceCost =
            new Ordering<PoolCost>()
            {
                @Override
                public int compare(PoolCost cost1, PoolCost cost2) {
                    return Double.compare(getWeightedPerformanceCost(cost1),
                                          getWeightedPerformanceCost(cost2));
                }
            };

        _byFullCost =
            new Ordering<PoolCost>()
            {
                @Override
                public int compare(PoolCost cost1, PoolCost cost2) {
                    return Double.compare(getWeightedFullCost(cost1),
                                          getWeightedFullCost(cost2));
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
