package org.dcache.poolmanager;

import java.util.Map;
import java.util.List;
import java.util.Collections;

import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.CostCalculatable;
import diskCacheV111.pools.CostCalculationV5;
import diskCacheV111.util.CacheException;

import com.google.common.collect.ImmutableMap;
import static com.google.common.base.Preconditions.checkState;

/**
 * Partition that provides classic dCache pool selection semantics.
 */
public class ClassicPartition extends Partition
{
    static final long serialVersionUID = 8239030345609342048L;

    public enum SameHost { NEVER, BESTEFFORT, NOTCHECKED };

    private static final Map<String,String> NO_PROPERTIES =
        ImmutableMap.<String,String>of();
    private static final double MAX_WRITE_COST = 1000000.0;

    private final static Map<String,String> DEFAULTS =
        ImmutableMap.<String,String>builder()
        .put("max-copies", "500")
        .put("p2p", "0.0")
        .put("alert", "0.0")
        .put("halt", "0.0")
        .put("fallback", "0.0")
        .put("spacecostfactor", "1.0")
        .put("cpucostfactor", "1.0")
        .put("sameHostCopy", "besteffort")
        .build();

    public final SameHost _allowSameHostCopy;
    public final int _maxPnfsFileCopies;

    public final double  _costCut;
    public final boolean _costCutIsPercentile;
    public final double  _alertCostCut;
    public final double  _panicCostCut;
    public final double  _fallbackCostCut;

    public final double  _spaceCostFactor;
    public final double  _performanceCostFactor;

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
        super(DEFAULTS, inherited, properties);

        _allowSameHostCopy =
            SameHost.valueOf(getProperty("sameHostCopy").toUpperCase());
        _maxPnfsFileCopies = getInteger("max-copies");
        _alertCostCut = getDouble("alert");
        _panicCostCut = getDouble("halt");
        _fallbackCostCut = getDouble("fallback");
        _spaceCostFactor = getDouble("spacecostfactor");
        _performanceCostFactor = getDouble("cpucostfactor");

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
}