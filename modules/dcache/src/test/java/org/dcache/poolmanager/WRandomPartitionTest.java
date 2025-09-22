package org.dcache.poolmanager;

import com.google.common.collect.ImmutableMap;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.util.CacheException;
import dmg.cells.nucleus.CellAddressCore;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.AdditionalMatchers;

import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class WRandomPartitionTest {


    @Test(expected = CostException.class)
    public void shouldFailIfAllocatesIntoGap() throws CacheException {

        var wrandom = new WRandomPartition(Map.of());
        var pools = IntStream.range(0, 10).mapToObj(i -> {
                    var cost = new PoolCostInfo("pool" + i, "default-queue");
                    cost.setSpaceUsage(10_000L, 3000L, 0L, 0L);
                    cost.getSpaceInfo().setParameter(0.0d, 2500L);

                    return new PoolInfo(new CellAddressCore("pool" + i), cost, ImmutableMap.of());
                }
        ).toList();

        long fileSize = 1000L;
        wrandom.selectWritePool(null, pools, null, fileSize);
    }

    @Test
    public void shouldSelectValidPool() throws CacheException {

        var wrandom = new WRandomPartition(Map.of());
        var pools = IntStream.range(0, 10).mapToObj(i -> {
                    var cost = new PoolCostInfo("pool" + i, "default-queue");
                    cost.setSpaceUsage(10_000L, 5000L, 0L, 0L);
                    cost.getSpaceInfo().setParameter(0.0d, 2500L);

                    return new PoolInfo(new CellAddressCore("pool" + i), cost, ImmutableMap.of());
                }
        ).toList();

        long fileSize = 1000L;
        var selectedPool = wrandom.selectWritePool(null, pools, null, fileSize);

        var spaceInfo = selectedPool.info().getCostInfo().getSpaceInfo();
        assertTrue("selected pool has no sufficient space", spaceInfo.getFreeSpace() + spaceInfo.getRemovableSpace() - fileSize >= spaceInfo.getGap());
    }


    @Test
    public void shouldSkipFullPool() throws CacheException {

        var wrandom = new WRandomPartition(Map.of());

        // starting the 5th pools have enough space
        var pools = IntStream.range(0, 10).mapToObj(i -> {
                    var cost = new PoolCostInfo("pool" + i, "default-queue");
                    cost.setSpaceUsage(10_000L, 3_000L + 100L*i, 0L, 0L);
                    cost.getSpaceInfo().setParameter(0.0d, 2500L);

                    return new PoolInfo(new CellAddressCore("pool" + i), cost, ImmutableMap.of());
                }
        ).toList();

        long fileSize = 1000L;
        var selectedPool = wrandom.selectWritePool(null, pools, null, fileSize);

        var spaceInfo = selectedPool.info().getCostInfo().getSpaceInfo();
        System.out.println(selectedPool.info().getCostInfo().getPoolName()+ " " + spaceInfo);
        assertTrue("selected pool has no sufficient space", spaceInfo.getFreeSpace() + spaceInfo.getRemovableSpace() - fileSize >= spaceInfo.getGap());
    }
}