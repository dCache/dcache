package diskCacheV111.poolManager;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;

import java.io.Serializable;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellInfo;

public class PoolManagerCellInfo
       extends CellInfo
       implements Serializable
{
    private static final long serialVersionUID = -5064922519895537712L;

    private ImmutableBiMap<String,CellAddressCore> _pools = ImmutableBiMap.of();

    PoolManagerCellInfo(CellInfo info)
    {
       super(info);
    }

    void setPools(BiMap<String,CellAddressCore> pools)
    {
       _pools = ImmutableBiMap.copyOf(pools);
    }

    public ImmutableBiMap<String,CellAddressCore> getPoolMap()
    {
        return _pools;
    }

    public ImmutableSet<String> getPoolNames()
    {
        return _pools.keySet();
    }

    public ImmutableSet<CellAddressCore> getPoolCells()
    {
        return _pools.values();
    }

    @Override
    public String toString()
    {
        return super.toString() + " " + _pools;
    }
}
