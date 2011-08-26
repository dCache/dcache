package org.dcache.poolmanager;

import diskCacheV111.pools.PoolCostInfo;
import com.google.common.collect.ImmutableMap;
import static com.google.common.base.Preconditions.checkNotNull;

public class PoolInfo
{
    private final PoolCostInfo _cost;
    private final ImmutableMap<String,String> _tags;

    public PoolInfo(PoolCostInfo cost, ImmutableMap<String,String> tags)
    {
        checkNotNull(cost);
        checkNotNull(tags);
        _cost = cost;
        _tags = tags;
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
