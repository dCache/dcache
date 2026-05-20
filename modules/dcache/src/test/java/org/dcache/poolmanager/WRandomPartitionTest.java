package org.dcache.poolmanager;

import com.google.common.collect.ImmutableMap;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.util.CacheException;
import dmg.cells.nucleus.CellAddressCore;
import org.dcache.vehicles.FileAttributes;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.AdditionalMatchers;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
        assertTrue("selected pool has no sufficient space", spaceInfo.getFreeSpace() + spaceInfo.getRemovableSpace() - fileSize >= spaceInfo.getGap());
    }

    @Test
    public void testCorrectZoneDst() throws CacheException {
        var attributes = FileAttributes.ofSize(1000L);
        var zone = Optional.of("A");
        var wrandom = new WRandomPartition(Map.of());

        var src = IntStream.range(0, 2).mapToObj(i -> {
                    var cost = new PoolCostInfo("pool" + i, "default-queue");
                    cost.setSpaceUsage(10_000L, 5000L, 0L, 0L);
                    cost.getSpaceInfo().setParameter(0.0d, 2500L);
                    return new PoolInfo(new CellAddressCore("pool" + i), cost, ImmutableMap.of("zone", "Z", "hostname", "src"));
                }
        ).collect(Collectors.toList());

        var dstWrong = IntStream.range(3, 5).mapToObj(i -> {
                    var cost = new PoolCostInfo("pool" + i, "default-queue");
                    cost.setSpaceUsage(10_000L, 5000L, 0L, 0L);
                    cost.getSpaceInfo().setParameter(0.0d, 2500L);
                    return new PoolInfo(new CellAddressCore("pool" + i), cost, ImmutableMap.of("zone", "B", "hostname", "dst-wrong"));
                }
        ).toList();

        var dstCorrect = IntStream.range(6, 8).mapToObj(i -> {
                    var cost = new PoolCostInfo("pool" + i, "default-queue");
                    cost.setSpaceUsage(10_000L, 5000L, 0L, 0L);
                    cost.getSpaceInfo().setParameter(0.0d, 2500L);
                    return new PoolInfo(new CellAddressCore("pool" + i), cost, ImmutableMap.of("zone", "A", "hostname", "dst-correct"));
                }
        ).toList();

        var dst = Stream.concat(dstCorrect.stream(), dstWrong.stream()).toList();

        PoolInfo info = wrandom.selectPool2Pool(null, src, dst, attributes, zone, false).destination.info();
        assertEquals(zone.get(), info.getTags().get("zone"));
    }
}