package org.dcache.tests.pool.migration;

import org.junit.Test;
import static org.junit.Assert.*;

import org.dcache.pool.migration.PoolListFromPoolManager;
import org.dcache.pool.migration.PoolCostPair;
import diskCacheV111.vehicles.PoolManagerGetPoolsMessage;
import diskCacheV111.vehicles.PoolManagerPoolInformation;

import java.util.Collections;
import java.util.Collection;
import java.util.List;
import java.util.Arrays;
import java.util.regex.Pattern;

public class PoolListFromPoolManagerTest
{
    private final PoolManagerPoolInformation POOL1 =
        createPool("pool1", 0.5, 0.5);
    private final PoolManagerPoolInformation POOL2 =
        createPool("pool2", 0.5, 0.5);
    private final PoolManagerPoolInformation POOL3 =
        createPool("pool3", Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);

    private final Collection<Pattern> EMPTY_LIST =
        Collections.emptyList();

    @Test
    public void testInfinityWithZeroFactor()
    {
        PoolListFromPoolManager list = new PoolList(EMPTY_LIST, 0, 0);

        list.success(createMessage(POOL1, POOL3, POOL2));
        List<PoolCostPair> result = list.getPools();

        assertEquals(3, result.size());
        for (PoolCostPair pair: result) {
            assertEquals(0, pair.cost);
        }
    }

    @Test
    public void testInfinityWithNonZeroFactor()
    {
        PoolListFromPoolManager list = new PoolList(EMPTY_LIST, 0.5, 0.5);

        list.success(createMessage(POOL3));
        List<PoolCostPair> result = list.getPools();

        assertEquals(1, result.size());
        assertEquals(Double.POSITIVE_INFINITY, result.get(0).cost);
    }

    public static PoolManagerGetPoolsMessage
        createMessage(PoolManagerPoolInformation... pools)
    {
        PoolManagerGetPoolsMessage msg =
            new PoolManagerGetPoolsMessage();
        msg.setPools(Arrays.asList(pools));
        return msg;
    }

    public static PoolManagerPoolInformation
        createPool(String name, double spaceCost, double cpuCost)
    {
        PoolManagerPoolInformation info =
            new PoolManagerPoolInformation(name);
        info.setSpaceCost(spaceCost);
        info.setCpuCost(cpuCost);
        return info;
    }

    /**
     * The object we want to test is abstract, hence we need to
     * subclass to test it.
     */
    static class PoolList extends PoolListFromPoolManager
    {
        public PoolList(Collection<Pattern> exclude,
                        double spaceFactor,
                        double cpuFactor)
        {
            super(exclude, spaceFactor, cpuFactor);
        }

        public void refresh()
        {
        }
    }
}
