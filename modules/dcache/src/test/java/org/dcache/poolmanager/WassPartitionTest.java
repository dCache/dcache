package org.dcache.poolmanager;

import com.google.common.collect.ImmutableMap;
import diskCacheV111.poolManager.CostModule;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.util.CacheException;
import dmg.cells.nucleus.CellAddressCore;
import org.dcache.vehicles.FileAttributes;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.RepeatedTest;

import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WassPartitionTest {

    @Test
    public void testCorrectZoneDst() throws CacheException {

        var wass = new WassPartition(Map.of());
        var attributes = FileAttributes.ofSize(1000L);
        var zone = Optional.of("A");

        var src = IntStream.range(0, 1).mapToObj(i -> {
                    var cost = new PoolCostInfo("pool" + i, "default-queue");
                    cost.setSpaceUsage(10_000L, 5000L, 0L, 0L);
                    cost.getSpaceInfo().setParameter(0.0d, 2500L);

                    return new PoolInfo(new CellAddressCore("pool" + i), cost, ImmutableMap.of("zone", "Z"));
                }
        ).toList();

        var dstWrong = IntStream.range(2, 4).mapToObj(i -> {
                    var cost = new PoolCostInfo("pool" + i, "default-queue");
                    cost.setSpaceUsage(10_000L, 5000L, 0L, 0L);
                    cost.getSpaceInfo().setParameter(0.0d, 2500L);

                    return new PoolInfo(new CellAddressCore("pool" + i), cost, ImmutableMap.of("zone", "B"));
                }
        ).toList();

        var dstCorrect = IntStream.range(5, 7).mapToObj(i -> {
                    var cost = new PoolCostInfo("pool" + i, "default-queue");
                    cost.setSpaceUsage(10_000L, 5000L, 0L, 0L);
                    cost.getSpaceInfo().setParameter(0.0d, 2500L);

                    return new PoolInfo(new CellAddressCore("pool" + i), cost, ImmutableMap.of("zone", "A"));
                }
        ).toList();

        var dst = Stream.concat(dstCorrect.stream(), dstWrong.stream()).toList();

        // run repeatedly; outcome is nondeterministic if filtering by zone is broken.
        // chances are pool with correct zone is selected when even when not filtered.
        for (int i = 0; i < 9; i++){
            PoolInfo info = wass.selectPool2Pool(null, src, dst, attributes, zone, false).destination.info();
            assertEquals(zone.get(), info.getTags().get("zone"));
        }
    }
}