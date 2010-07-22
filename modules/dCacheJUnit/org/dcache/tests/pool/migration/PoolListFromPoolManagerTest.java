package org.dcache.tests.pool.migration;

import org.junit.Test;
import static org.junit.Assert.*;

import org.dcache.pool.migration.PoolListFromPoolManager;
import org.dcache.pool.migration.PoolCostPair;
import org.dcache.util.Glob;
import diskCacheV111.vehicles.PoolManagerGetPoolsMessage;
import diskCacheV111.vehicles.PoolManagerPoolInformation;

import java.util.Collections;
import java.util.Collection;
import java.util.List;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;
import java.util.regex.Pattern;

import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.Expression;

public class PoolListFromPoolManagerTest
{
    private final static JexlEngine jexl = new JexlEngine();

    private final PoolManagerPoolInformation POOL1 =
        createPool("pool1", 0.5, 0.5);
    private final PoolManagerPoolInformation POOL2 =
        createPool("pool2", 0.5, 0.5);
    private final PoolManagerPoolInformation POOL3 =
        createPool("pool3", Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);

    @Test
    public void testInfinityWithZeroFactor()
    {
        PoolListFromPoolManager list = new PoolList(0, 0);

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
        PoolListFromPoolManager list = new PoolList(0.5, 0.5);

        list.success(createMessage(POOL3));
        List<PoolCostPair> result = list.getPools();

        assertEquals(1, result.size());
        assertEquals(Double.POSITIVE_INFINITY, result.get(0).cost);
    }

    @Test
    public void testExclude()
    {
        PoolListFromPoolManager list =
            new PoolList("*1", "false",
                         null, "true",
                         0.5, 0.5);

        list.success(createMessage(POOL1, POOL2, POOL3));
        List<PoolCostPair> result = list.getPools();

        assertEquals(2, result.size());
        assertDoesNotContainPool(POOL1, result);
        assertContainsPool(POOL2, result);
        assertContainsPool(POOL3, result);
    }

    @Test
    public void testExcludeWhen()
    {
        PoolListFromPoolManager list =
            new PoolList(null, "target.name=~'pool[12]'",
                         null, "true",
                         0.5, 0.5);

        list.success(createMessage(POOL1, POOL2, POOL3));
        List<PoolCostPair> result = list.getPools();

        assertEquals(1, result.size());
        assertDoesNotContainPool(POOL1, result);
        assertDoesNotContainPool(POOL2, result);
        assertContainsPool(POOL3, result);
    }

    @Test
    public void testInclude()
    {
        PoolListFromPoolManager list =
            new PoolList(null, "false",
                         "*1", "true",
                         0.5, 0.5);

        list.success(createMessage(POOL1, POOL2, POOL3));
        List<PoolCostPair> result = list.getPools();

        assertEquals(1, result.size());
        assertContainsPool(POOL1, result);
        assertDoesNotContainPool(POOL2, result);
        assertDoesNotContainPool(POOL3, result);
    }

    @Test
    public void testIncludeWhen()
    {
        PoolListFromPoolManager list =
            new PoolList(null, "false",
                         null, "target.name=~'pool[12]'",
                         0.5, 0.5);

        list.success(createMessage(POOL1, POOL2, POOL3));
        List<PoolCostPair> result = list.getPools();

        assertEquals(2, result.size());
        assertContainsPool(POOL1, result);
        assertContainsPool(POOL2, result);
        assertDoesNotContainPool(POOL3, result);
    }

    @Test
    public void testBothIncludedAndExcluded()
    {
        PoolListFromPoolManager list =
            new PoolList("*1", "false",
                         "*1", "true",
                         0.5, 0.5);

        list.success(createMessage(POOL1, POOL2, POOL3));
        List<PoolCostPair> result = list.getPools();

        assertEquals(0, result.size());
    }

    @Test
    public void testBothIncludedWhenAndExcludedWhen()
    {
        PoolListFromPoolManager list =
            new PoolList(null, "target.name=='pool1'",
                         null, "target.name=='pool1'",
                         0.5, 0.5);

        list.success(createMessage(POOL1, POOL2, POOL3));
        List<PoolCostPair> result = list.getPools();

        assertEquals(0, result.size());
    }

    private static Expression createExpression(String s)
    {
        return (s == null) ? null : jexl.createExpression(s);
    }

    private static PoolManagerGetPoolsMessage
        createMessage(PoolManagerPoolInformation... pools)
    {
        PoolManagerGetPoolsMessage msg =
            new PoolManagerGetPoolsMessage();
        msg.setPools(Arrays.asList(pools));
        return msg;
    }

    private static PoolManagerPoolInformation
        createPool(String name, double spaceCost, double cpuCost)
    {
        PoolManagerPoolInformation info =
            new PoolManagerPoolInformation(name);
        info.setSpaceCost(spaceCost);
        info.setCpuCost(cpuCost);
        return info;
    }

    private static Set<Pattern> createPatterns(String glob)
    {
        Set<Pattern> patterns = new HashSet<Pattern>();
        if (glob != null) {
            patterns.add(Glob.parseGlobToPattern(glob));
        }
        return patterns;
    }

    private static boolean containsPool(String name, List<PoolCostPair> list)
    {
        for (PoolCostPair pool: list) {
            if (pool.path.getCellName().equals(name)) {
                return true;
            }
        }
        return false;
    }

    private static void assertContainsPool(PoolManagerPoolInformation pool,
                                           List<PoolCostPair> list)
    {
        String name = pool.getName();
        if (!containsPool(name, list)) {
            fail("Expected pool " + name + " is not in list " + list);
        }
    }

    private static void assertDoesNotContainPool(PoolManagerPoolInformation pool,
                                                 List<PoolCostPair> list)
    {
        String name = pool.getName();
        if (containsPool(name, list)) {
            fail("Unexpected pool " + name + " is in list " + list);
        }
    }


    /**
     * The object we want to test is abstract, hence we need to
     * subclass to test it.
     */
    static class PoolList extends PoolListFromPoolManager
    {
        public PoolList(double spaceFactor,
                        double cpuFactor)
        {
            this(null, "false", null, "true", spaceFactor, cpuFactor);
        }

        public PoolList(String exclude, String excludeWhen,
                        String include, String includeWhen,
                        double spaceFactor,
                        double cpuFactor)
        {
            super(createPatterns(exclude), createExpression(excludeWhen),
                  createPatterns(include), createExpression(includeWhen),
                  spaceFactor, cpuFactor);
        }

        public void refresh()
        {
        }
    }
}
