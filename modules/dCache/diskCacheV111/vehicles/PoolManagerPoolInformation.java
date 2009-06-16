package diskCacheV111.vehicles;

import java.io.Serializable;

public class PoolManagerPoolInformation
    implements Serializable
{
    static final long serialVersionUID = -279163439475487756L;

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

    @Override
    public String toString()
    {
        return String.format("[name=%s;space=%f;cpu=%f]",
                             _name, _spaceCost, _cpuCost);
    }
}