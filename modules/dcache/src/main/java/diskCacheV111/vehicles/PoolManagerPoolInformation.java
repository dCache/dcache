package diskCacheV111.vehicles;

import java.io.Serializable;

import diskCacheV111.pools.CostCalculationV5;
import diskCacheV111.pools.PoolCostInfo;

import static com.google.common.base.Preconditions.checkNotNull;

public class PoolManagerPoolInformation
    implements Serializable
{
    private static final long serialVersionUID = -279163439475487756L;

    private final String _name;
    @Deprecated // Can be removed after Golden Release 4
    private double _spaceCost;
    private double _cpuCost;
    private final PoolCostInfo _poolCostInfo;

    public PoolManagerPoolInformation(String name, PoolCostInfo poolCostInfo)
    {
        _name = name;
        _poolCostInfo = checkNotNull(poolCostInfo);
        CostCalculationV5 calc = new CostCalculationV5(_poolCostInfo);
        calc.recalculate();
        _spaceCost = calc.getSpaceCost();
        _cpuCost = calc.getPerformanceCost();
    }

    public String getName()
    {
        return _name;
    }

    public double getCpuCost()
    {
        return _cpuCost;
    }

    public PoolCostInfo getPoolCostInfo()
    {
        return _poolCostInfo;
    }

    @Override
    public String toString()
    {
        return String.format("[name=%s;cpu=%f;cost=%s]",
                             _name, _cpuCost, _poolCostInfo);
    }
}
