package org.dcache.poolmanager;

import java.util.Map;
import java.util.List;
import java.util.Random;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolCostInfo.PoolSpaceInfo;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

import org.dcache.vehicles.FileAttributes;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import static com.google.common.collect.Iterables.filter;

/**
 * Partition that selects pools randomly.
 */
public class RandomPartition extends Partition
{
    private static final long serialVersionUID = -2614882036844578650L;

    static final String TYPE = "random";

    public final static Random random = new Random();

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

    private Predicate<PoolInfo> canHoldFile(FileAttributes attributes)
    {
        final long filesize = attributes.getSize();
        return new Predicate<PoolInfo>() {
            @Override
            public boolean apply(PoolInfo pool) {
                PoolSpaceInfo space = pool.getCostInfo().getSpaceInfo();
                long available =
                    space.getFreeSpace() + space.getRemovableSpace();
                return available - filesize > space.getGap();
            }
        };
    }

    private PoolInfo select(List<PoolInfo> pools)
    {
        return pools.get(random.nextInt(pools.size()));
    }

    @Override
    public PoolInfo selectWritePool(CostModule cm,
                                    List<PoolInfo> pools,
                                    FileAttributes attributes)
        throws CacheException
    {
        List<PoolInfo> freePools =
            Lists.newArrayList(filter(pools, canHoldFile(attributes)));
        if (freePools.isEmpty()) {
            throw new CacheException(21, "All pools are full");
        }
        return select(freePools);
    }

    @Override
    public PoolInfo selectReadPool(CostModule cm,
                                   List<PoolInfo> pools,
                                   FileAttributes attributes)
        throws CacheException
    {
        return select(pools);
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
        return new P2pPair(select(src), selectWritePool(cm, dst, attributes));
    }

    @Override
    public PoolInfo selectStagePool(CostModule cm,
                                    List<PoolInfo> pools,
                                    String previousPool,
                                    String previousHost,
                                    FileAttributes attributes)
        throws CacheException
    {
        return selectWritePool(cm, pools, attributes);
    }
}
