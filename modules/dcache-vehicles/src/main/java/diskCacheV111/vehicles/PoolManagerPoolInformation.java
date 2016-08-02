package diskCacheV111.vehicles;

import java.io.Serializable;

import diskCacheV111.pools.PoolCostInfo;

import static com.google.common.base.Preconditions.checkNotNull;

public class PoolManagerPoolInformation
    implements Serializable
{
    private static final long serialVersionUID = -279163439475487756L;

    private final String _name;
    private final double _cpuCost;
    private final PoolCostInfo _poolCostInfo;

    public PoolManagerPoolInformation(String name, PoolCostInfo poolCostInfo, double cpuCost)
    {
        _name = name;
        _poolCostInfo = checkNotNull(poolCostInfo);
        _cpuCost = cpuCost;
    }

    public PoolManagerPoolInformation(String name, PoolCostInfo poolCostInfo) {
        this(name, poolCostInfo, 0.0);
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
