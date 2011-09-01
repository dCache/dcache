package org.dcache.poolmanager;

import java.util.Map;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;;
import java.util.concurrent.atomic.AtomicLong;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolCostInfo.PoolSpaceInfo;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.base.Preconditions.checkState;

/**
 * Partition that selects the least recently used pool.
 *
 * Corresponds more or less to a round robin selection.
 */
public class LruPartition extends Partition
{
    static final long serialVersionUID = 2982471048144479008L;

    public final static String TYPE = "lru";

    private final static AtomicLong _counter = new AtomicLong();

    private final static Random random = new Random();

    /**
     * Pool name to access order. Making this static will mean that
     * all instances of LruPartition will share this information. It
     * will also ensure that this information is not serialized,
     * meaning that multiple deserialized instances preserve this
     * information.
     */
    private final static ConcurrentMap<String,Long> _lastWrite =
        Maps.newConcurrentMap();
    private final static ConcurrentMap<String,Long> _lastRead =
        Maps.newConcurrentMap();

    public LruPartition(Map<String,String> inherited)
    {
        this(inherited, NO_PROPERTIES);
    }

    public LruPartition(Map<String,String> inherited,
                        Map<String,String> properties)
    {
        super(NO_PROPERTIES, inherited, properties);
    }

    @Override
    protected Partition create(Map<String,String> inherited,
                               Map<String,String> properties)
    {
        return new LruPartition(inherited, properties);
    }

    @Override
    public String getType()
    {
        return TYPE;
    }

    private Predicate<PoolInfo> canHoldFile(final long filesize)
    {
        return new Predicate<PoolInfo>() {
            public boolean apply(PoolInfo pool) {
                PoolSpaceInfo space = pool.getCostInfo().getSpaceInfo();
                long available =
                    space.getFreeSpace() + space.getRemovableSpace();
                return available - filesize > space.getGap();
            }
        };
    }

    private long next()
    {
        return _counter.getAndIncrement();
    }

    /* Thread safe lock-free algorithm for choosing the least recently
     * used pool and updating the timestamp of the chosen pool.
     */
    private PoolInfo select(List<PoolInfo> pools,
                            ConcurrentMap<String,Long> lastAccessed)
    {
        checkState(!pools.isEmpty());
        PoolInfo oldest;
        long current;
        do {
            oldest = null;
            current = Long.MAX_VALUE;
            for (PoolInfo pool: pools) {
                Long value = lastAccessed.get(pool.getName());

                /* A random negative value for unused pools ensures
                 * that the initial order in which we select pools is
                 * random.
                 */
                if (value == null) {
                    value = -random.nextLong();
                }
                if (value < current) {
                    current = value;
                    oldest = pool;
                }
            }
        } while (!lastAccessed.replace(oldest.getName(), current, next()));
        return oldest;
    }

    @Override
    public PoolInfo
        selectWritePool(CostModule cm, List<PoolInfo> pools,
                        long filesize)
        throws CacheException
    {
        List<PoolInfo> freePools =
            Lists.newArrayList(filter(pools, canHoldFile(filesize)));
        if (freePools.isEmpty()) {
            throw new CacheException(21, "All pools are full");
        }
        return select(freePools, _lastWrite);
    }

    @Override
    public PoolInfo
        selectReadPool(CostModule cm, List<PoolInfo> pools, PnfsId pnfsId)
        throws CacheException
    {
        return select(pools, _lastRead);
    }

    @Override
    public P2pPair
        selectPool2Pool(CostModule cm,
                        List<PoolInfo> src,
                        List<PoolInfo> dst,
                        long filesize,
                        boolean force)
        throws CacheException
    {
        return new P2pPair(select(src, _lastRead),
                           selectWritePool(cm, dst, filesize));
    }

    @Override
    public PoolInfo selectStagePool(CostModule cm,
                                    List<PoolInfo> pools,
                                    String previousPool,
                                    String previousHost,
                                    long size)
        throws CacheException
    {
        return selectWritePool(cm, pools, size);
    }
}
