package org.dcache.poolmanager;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
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
import diskCacheV111.util.PnfsId;

import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkState;

/**
 * Legacy partition that provided the classic dCache pool selection semantics. Now
 * it only serves as a base class for WassPartition containing old code that is
 * expected to be replaced in the future.
 */
public abstract class ClassicPartition extends Partition
{
    private static final long serialVersionUID = 8239030345609342048L;

    public enum SameHost { NEVER, BESTEFFORT, NOTCHECKED }

    /**
     * COSTFACTORS
     *u
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
    private static final Map<String,String> DEFAULTS =
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
            calculatable.recalculate();

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
        calculatable.recalculate();
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
        final String host;

        public PoolCost(PoolInfo pool, CostCalculatable cost)
        {
            this(pool, cost.getPerformanceCost());
        }

        public PoolCost(PoolInfo pool, double performanceCost)
        {
            this.pool = pool;
            this.performanceCost = performanceCost;
            this.host = pool.getHostName();
        }
    }

    protected double getWeightedPerformanceCost(PoolCost cost)
    {
        return Math.abs(cost.performanceCost) * _performanceCostFactor;
    }

    protected static PoolCost toPoolCost(PoolInfo pool)
    {
        CostCalculatable calculatable = new CostCalculationV5(pool.getCostInfo());
        calculatable.recalculate();
        return new PoolCost(pool, calculatable);
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
    }

    private void readObject(ObjectInputStream stream)
        throws IOException, ClassNotFoundException
    {
        stream.defaultReadObject();
        initTransientFields();
    }
}
