package diskCacheV111.vehicles;

import java.io.Serializable;

public class PoolManagerPoolInformation
    implements Serializable
{
    private final String _name;
    private double _spaceCost;
    private double _cpuCost;

    public PoolManagerPoolInformation(String name)
    {
        _name = name;
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
}