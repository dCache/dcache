package org.dcache.poolmanager;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import diskCacheV111.pools.PoolCostInfo;
import dmg.cells.nucleus.CellAddressCore;
import java.io.Serializable;

public class PoolInfo implements Serializable {

    private static final long serialVersionUID = -5370136105656529718L;
    private final PoolCostInfo _cost;
    private final ImmutableMap<String, String> _tags;
    private final CellAddressCore _address;

    public PoolInfo(CellAddressCore address, PoolCostInfo cost, ImmutableMap<String, String> tags) {
        requireNonNull(address);
        requireNonNull(cost);
        requireNonNull(tags);
        _address = address;
        _cost = cost;
        _tags = tags;
    }

    public CellAddressCore getAddress() {
        return _address;
    }

    public String getName() {
        return _cost.getPoolName();
    }

    public PoolCostInfo getCostInfo() {
        return _cost;
    }

    public double getPerformanceCost() {
        return _cost.getPerformanceCost();
    }

    public ImmutableMap<String, String> getTags() {
        return _tags;
    }

    public String getHostName() {
        return _tags.get("hostname");
    }

    @Override
    public String toString() {
        return _cost.toString();
    }
}
