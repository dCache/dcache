package org.dcache.poolmanager;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.util.CacheException;

import org.dcache.vehicles.FileAttributes;

import static com.google.common.collect.Iterables.filter;

/**
 * Yet another weighted random partition. Selects randomly source pool and
 * probabilistic weighted destination pool. P2p transfers never use the same host/tag
 * as source and destination. The weight of a pool calculated as:
 *
 * free space / total free space
 *
 * where 'free space' is a sum of free and removable space.
 *
 */
public class WRandomPartition extends Partition
{
    public final static String TYPE = "wrandom";
    private static final long serialVersionUID = 5005233401277944842L;
    private final Random _random = new SecureRandom();

    public WRandomPartition(Map<String, String> inherited) {
        super(NO_PROPERTIES, inherited, NO_PROPERTIES);
    }

    @Override
    protected Partition create(Map<String, String> inherited,
            Map<String, String> properties) {
        return new WRandomPartition(inherited);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public P2pPair selectPool2Pool(CostModule cm, List<PoolInfo> src, List<PoolInfo> dst, FileAttributes attributes, boolean force) throws CacheException {

        Collections.shuffle(src);

        /*
         * for randomly selected source pool and try to find correct destination.
         */
        for (PoolInfo srcPoolInfo : src) {
            List<PoolInfo> tryList = Lists.newArrayList(
                    filter(dst, new DifferentHost(srcPoolInfo.getHostName())));

            if (!tryList.isEmpty()) {
                PoolInfo destPoolInfo = selectWritePool(cm, tryList, attributes, attributes.getSize());
                return new P2pPair(srcPoolInfo, destPoolInfo);
            }
        }

        return null;
    }

    @Override
    public PoolInfo selectReadPool(CostModule cm, List<PoolInfo> pools, FileAttributes attributes) throws CacheException {
        return pools.get(_random.nextInt(pools.size()));
    }

    @Override
    public PoolInfo selectStagePool(CostModule cm, List<PoolInfo> pools, String previousPool, String previousHost, FileAttributes attributes) throws CacheException {
        return selectWritePool(cm, pools, attributes, attributes.getSize());
    }

    @Override
    public PoolInfo selectWritePool(CostModule cm, List<PoolInfo> pools, FileAttributes attributes, long preallocated) throws CacheException {
        WeightedPool weightedPools[] = toWeightedWritePoolsArray(pools);
        int index = selectWrandomIndex(weightedPools);
        return weightedPools[index].getCostInfo();
    }

    private WeightedPool[] toWeightedWritePoolsArray(Collection<PoolInfo> costInfos) {

        long totalFree = 0;
        for (PoolInfo costInfo : costInfos) {
            long spaceToUse = costInfo.getCostInfo().getSpaceInfo().getFreeSpace()
                    + costInfo.getCostInfo().getSpaceInfo().getRemovableSpace();
            totalFree += spaceToUse;
        }

        WeightedPool[] weghtedPools = new WeightedPool[costInfos.size()];
        int i = 0;
        for (PoolInfo costInfo : costInfos) {
            long spaceToUse = costInfo.getCostInfo().getSpaceInfo().getFreeSpace()
                    + costInfo.getCostInfo().getSpaceInfo().getRemovableSpace();

            weghtedPools[i] = new WeightedPool(costInfo, (double) spaceToUse / totalFree);
            i++;
        }

        Arrays.sort(weghtedPools, new CostComparator());
        return weghtedPools;
    }

    private static class CostComparator implements Comparator<WeightedPool>
    {
        @Override
        public int compare(WeightedPool o1, WeightedPool o2) {
            return Double.compare(o1.getWeight(), o2.getWeight());
        }
    }

    private static class WeightedPool
    {
        private final PoolInfo _costInfo;
        private final double _weight;

        public WeightedPool(PoolInfo costInfo, double weight) {
            _costInfo = costInfo;
            _weight = weight;
        }

        public PoolInfo getCostInfo() {
            return _costInfo;
        }

        public double getWeight() {
            return _weight;
        }
    }

    private int selectWrandomIndex(WeightedPool[] weightedPools) {
        double selection = _random.nextDouble();
        double total = 0;
        int i;
        for (i = 0; (i < weightedPools.length) && (total <= selection); i++) {
            total += weightedPools[i].getWeight();
        }
        return i - 1;
    }

    private class DifferentHost implements Predicate<PoolInfo>
    {
        private final String _host;

        DifferentHost(String host) {
            _host = host;
        }

        @Override
        public boolean apply(PoolInfo t) {
            String hostname = t.getHostName();
            return !_host.equals(hostname);
        }
    }
}
