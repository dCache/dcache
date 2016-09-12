package org.dcache.poolmanager;

import java.util.List;
import java.util.Map;
import java.util.Random;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.pools.PoolCostInfo.PoolSpaceInfo;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CostException;

import org.dcache.pool.assumption.AvailableSpaceAssumption;
import org.dcache.vehicles.FileAttributes;

import static java.util.stream.Collectors.toList;

/**
 * Partition that selects pools randomly.
 */
public class RandomPartition extends Partition
{
    private static final long serialVersionUID = -2614882036844578650L;

    static final String TYPE = "random";

    public static final Random random = new Random();

    public RandomPartition(Map<String,String> inherited)
    {
        this(inherited, NO_PROPERTIES);
    }

    public RandomPartition(Map<String,String> inherited,
                           Map<String,String> properties)
    {
        super(NO_PROPERTIES, inherited, properties);
    }

    @Override
    protected Partition create(Map<String,String> inherited,
                               Map<String,String> properties)
    {
        return new RandomPartition(inherited, properties);
    }

    @Override
    public String getType()
    {
        return TYPE;
    }

    private boolean canHoldFile(PoolInfo pool, long size)
    {
        PoolSpaceInfo space = pool.getCostInfo().getSpaceInfo();
        long available =
            space.getFreeSpace() + space.getRemovableSpace();
        return available - size > space.getGap();
    }

    private PoolInfo select(List<PoolInfo> pools)
    {
        return pools.get(random.nextInt(pools.size()));
    }

    @Override
    public SelectedPool selectWritePool(CostModule cm,
                                        List<PoolInfo> pools,
                                        FileAttributes attributes,
                                        long preallocated)
        throws CacheException
    {
        List<PoolInfo> freePools =
                pools.stream().filter(pool -> canHoldFile(pool, preallocated)).collect(toList());
        if (freePools.isEmpty()) {
            throw new CostException("All pools are full", null, false, false);
        }
        return new SelectedPool(select(freePools), new AvailableSpaceAssumption(preallocated));
    }

    @Override
    public SelectedPool selectReadPool(CostModule cm,
                                       List<PoolInfo> pools,
                                       FileAttributes attributes)
        throws CacheException
    {
        return new SelectedPool(select(pools));
    }

    @Override
    public P2pPair
        selectPool2Pool(CostModule cm,
                        List<PoolInfo> src,
                        List<PoolInfo> dst,
                        FileAttributes attributes,
                        boolean force)
        throws CacheException
    {
        return new P2pPair(new SelectedPool(select(src)),
                           selectWritePool(cm, dst, attributes, attributes.getSize()));
    }

    @Override
    public SelectedPool selectStagePool(CostModule cm,
                                        List<PoolInfo> pools,
                                        String previousPool,
                                        String previousHost,
                                        FileAttributes attributes)
        throws CacheException
    {
        return selectWritePool(cm, pools, attributes, attributes.getSize());
    }
}
