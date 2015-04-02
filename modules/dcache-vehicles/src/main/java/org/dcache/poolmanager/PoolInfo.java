package org.dcache.poolmanager;

import com.google.common.collect.ImmutableMap;

import java.io.Serializable;

import diskCacheV111.pools.PoolCostInfo;

import dmg.cells.nucleus.CellAddressCore;

import static com.google.common.base.Preconditions.checkNotNull;

public class PoolInfo implements Serializable
{
    private static final long serialVersionUID = -5370136105656529718L;
    private final PoolCostInfo _cost;
    private final ImmutableMap<String,String> _tags;
    private final CellAddressCore _address;

    public PoolInfo(CellAddressCore address, PoolCostInfo cost, ImmutableMap<String,String> tags)
    {
        checkNotNull(address);
        checkNotNull(cost);
        checkNotNull(tags);
        _address = address;
        _cost = cost;
        _tags = tags;
    }

    public CellAddressCore getAddress()
    {
        return _address;
    }

    public String getName()
    {
        return _cost.getPoolName();
    }

    public PoolCostInfo getCostInfo()
    {
        return _cost;
    }

    public ImmutableMap<String,String> getTags()
    {
        return _tags;
    }

    public String getHostName()
    {
        return _tags.get("hostname");
    }

    @Override
    public String toString()
    {
        return _cost.toString();
    }
}
