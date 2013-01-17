package diskCacheV111.vehicles;

import java.io.Serializable;

import diskCacheV111.pools.PoolCostInfo;

public class PoolManagerPoolInformation
    implements Serializable
{
    private static final long serialVersionUID = -279163439475487756L;

    private final String _name;
    private double _spaceCost;
    private double _cpuCost;
    private PoolCostInfo _poolCostInfo;

    public PoolManagerPoolInformation(String name)
    {
        this(name, 0.0, 0.0);
    }

    public PoolManagerPoolInformation(String name, double spaceCost, double cpuCost)
    {
        _name = name;
        _spaceCost = spaceCost;
        _cpuCost = cpuCost;
    }

    public String getName()
    {
        return _name;
    }

    public void setSpaceCost(double spaceCost)
    {
        _spaceCost = spaceCost;
    }

    public double getSpaceCost()
    {
        return _spaceCost;
    }

    public void setCpuCost(double cpuCost)
    {
        _cpuCost = cpuCost;
    }

    public double getCpuCost()
    {
        return _cpuCost;
    }

    public void setPoolCostInfo(PoolCostInfo poolCostInfo)
    {
        _poolCostInfo = poolCostInfo;
    }

    public PoolCostInfo getPoolCostInfo()
    {
        return _poolCostInfo;
    }

    @Override
    public String toString()
    {
        return String.format("[name=%s;space=%f;cpu=%f;cost=%s]",
                             _name, _spaceCost, _cpuCost, _poolCostInfo);
    }
}
