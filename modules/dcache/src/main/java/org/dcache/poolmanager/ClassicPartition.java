package org.dcache.poolmanager;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CostException;
import diskCacheV111.util.PnfsId;

import org.dcache.pool.assumption.PerformanceCostAssumption;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.max;
import static java.util.stream.Collectors.*;

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
     *   max-copies       int
     *   fallback-onspace boolean
     *
     *    Options        |  Description
     *  ----------------------------------------------------------------------------
     *         idle      |  below 'idle' : 'reduce duplicate' mode
     *         p2p       |  above : start pool to pool mode
     *                   |  If p2p value is a percent then p2p is dynamically
     *                   |  assigned that percentile value of pool performance costs
     *        alert      |  stop pool 2 pool mode, start stage only mode
     *         halt      |  suspend system
     *       fallback    |  Allow fallback in Permission matrix on high load
     *  fallback-onspace |  Allow fallback on write if out of free space
     *        error      |  How much the performance cost of a pool may exceed
     *                   |  limits before it rejects a request
     */
    private static final Map<String,String> DEFAULTS =
        ImmutableMap.<String,String>builder()
        .put("max-copies", "3")
        .put("p2p", "0.0")
        .put("alert", "0.0")
        .put("halt", "0.0")
        .put("fallback", "0.0")
        .put("fallback-onspace", "no")
        .put("spacecostfactor", "1.0")
        .put("cpucostfactor", "1.0")
        .put("sameHostCopy", "besteffort")
        .put("sameHostRetry", "besteffort")
        .put("slope", "0.0")
        .put("idle", "0.0")
        .put("error", "0.2")
        .build();

    protected final SameHost _allowSameHostCopy;
    protected final SameHost _allowSameHostRetry;
    protected final long _maxPnfsFileCopies;

    protected final double  _costCut;
    protected final boolean _costCutIsPercentile;
    protected final double  _alertCostCut;
    protected final double  _panicCostCut;
    protected final double  _fallbackCostCut;
    protected final boolean _fallbackOnSpace;

    protected final double  _spaceCostFactor;
    protected final double  _performanceCostFactor;

    protected final double  _error;
    protected final double  _slope;
    protected final double  _minCostCut;

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
        _error = getDouble("error");
        _slope = getDouble("slope");
        _minCostCut = getDouble("idle");
        _fallbackOnSpace = getBoolean("fallback-onspace");

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
    public SelectedPool selectReadPool(CostModule cm,
                                       List<PoolInfo> pools,
                                       FileAttributes attributes)
        throws CacheException
    {
        checkState(!pools.isEmpty());

        /* Randomise order of pools with equal cost. In particular
         * important when performance cost factor is 0.
         */
        Collections.shuffle(pools);

        /* Find best pool, taking min cost cut into account.
         */
        PnfsId pnfsId = attributes.getPnfsId();
        double pcf = _performanceCostFactor;
        double mcc = _minCostCut;

        Comparator<PoolInfo> byPerformanceCost =
                (a, b) -> (Math.max(a.getPerformanceCost(), b.getPerformanceCost()) < mcc)
                          ? Integer.compare(minCostCutPosition(pnfsId, a), minCostCutPosition(pnfsId, b))
                          : Double.compare(a.getPerformanceCost() * pcf, b.getPerformanceCost() * pcf);

        List<PoolInfo> best = pools.stream()
                .sorted(byPerformanceCost)
                .limit(2)
                .collect(toList());

        PoolInfo bestPool = best.get(0);

        /* Check cost cuts.
         */
        double cost = bestPool.getCostInfo().getPerformanceCost();
        boolean isPanicCostExceeded = isPanicCostExceeded(cost);
        boolean isFallbackCostExceeded = isFallbackCostExceeded(cost);
        boolean isCostCutExceeded = isCostCutExceeded(cm, cost);
        if (isPanicCostExceeded) {
            throw new CostException("Cost limit exceeded", null,
                                    isFallbackCostExceeded, isCostCutExceeded);
        }
        if (isFallbackCostExceeded || isCostCutExceeded) {
            throw new CostException("Cost limit exceeded",
                                    new SelectedPool(bestPool, PerformanceCostAssumption.of(_error, _panicCostCut)),
                                    isFallbackCostExceeded, isCostCutExceeded);
        }

        /* Add an assumption of the load being lower than the second best pool while still
         * taking the min cost cut into account.
         */
        double nextBest =
                (best.size() > 1) ? Math.max(best.get(1).getPerformanceCost(), mcc) : Double.POSITIVE_INFINITY;

        return new SelectedPool(bestPool, PerformanceCostAssumption.of(_error, _panicCostCut, _fallbackCostCut, _costCut, nextBest));
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

        public PoolCost(PoolInfo pool)
        {
            this(pool, pool.getCostInfo().getPerformanceCost());
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
        return new PoolCost(pool);
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
