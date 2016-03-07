package org.dcache.poolmanager;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.pools.PoolCostInfo.PoolSpaceInfo;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CostException;

import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkState;
import static java.util.stream.Collectors.toList;

/**
 * Partition that selects the least recently used pool.
 *
 * Corresponds more or less to a round robin selection.
 */
public class LruPartition extends Partition
{
    private static final long serialVersionUID = 2982471048144479008L;

    static final String TYPE = "lru";

    private static final AtomicLong _counter = new AtomicLong();

    private static final Random random = new Random();

    /**
     * Pool name to access order. Making this static will mean that
     * all instances of LruPartition will share this information. It
     * will also ensure that this information is not serialized,
     * meaning that multiple deserialized instances preserve this
     * information.
     */
    private static final ConcurrentMap<String,Long> _lastWrite =
        Maps.newConcurrentMap();
    private static final ConcurrentMap<String,Long> _lastRead =
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

    private static boolean canHoldFile(PoolInfo pool, long size)
    {
        PoolSpaceInfo space = pool.getCostInfo().getSpaceInfo();
        long available = space.getFreeSpace() + space.getRemovableSpace();
        return available - size > space.getGap();
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
            /* Repeat until we manage update the entry without concurrent
             * modification.
             */
        } while (current < 0 && lastAccessed.putIfAbsent(oldest.getName(), next()) != null ||
                current >= 0 && !lastAccessed.replace(oldest.getName(), current, next()));
        return oldest;
    }

    @Override
    public PoolInfo selectWritePool(CostModule cm,
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
        return select(freePools, _lastWrite);
    }

    @Override
    public PoolInfo selectReadPool(CostModule cm,
                                   List<PoolInfo> pools,
                                   FileAttributes attributes)
        throws CacheException
    {
        return select(pools, _lastRead);
    }

    @Override
    public P2pPair selectPool2Pool(CostModule cm,
                                   List<PoolInfo> src,
                                   List<PoolInfo> dst,
                                   FileAttributes attributes,
                                   boolean force)
        throws CacheException
    {
        return new P2pPair(select(src, _lastRead),
                           selectWritePool(cm, dst, attributes, attributes.getSize()));
    }

    @Override
    public PoolInfo selectStagePool(CostModule cm,
                                    List<PoolInfo> pools,
                                    String previousPool,
                                    String previousHost,
                                    FileAttributes attributes)
        throws CacheException
    {
        return selectWritePool(cm, pools, attributes, attributes.getSize());
    }
}
