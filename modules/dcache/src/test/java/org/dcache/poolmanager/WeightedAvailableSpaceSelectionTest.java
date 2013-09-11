package org.dcache.poolmanager;

import org.junit.Test;

import diskCacheV111.pools.PoolCostInfo;

import static org.junit.Assert.assertEquals;

public class WeightedAvailableSpaceSelectionTest
{
    private final WeightedAvailableSpaceSelection wass =
        new WeightedAvailableSpaceSelection(1.0, 1.0);

    public void checkAvailable(long expected,
                               long free,
                               long removable,
                               double breakeven,
                               long lru,
                               long gap)
    {
        PoolCostInfo info = new PoolCostInfo("pool");
        info.setSpaceUsage(free + removable, free, 0, removable, lru);
        info.getSpaceInfo().setParameter(breakeven, gap);
        assertEquals(expected, (long) wass.getAvailable(info.getSpaceInfo(), 0));
    }

    @Test
    public void testNoRemovable()
    {
        checkAvailable(1000000, 1000000, 0, 0.5, 1000, 1000);
    }

    @Test
    public void testNoRemovableLessThanGap()
    {
        checkAvailable(0, 1000000, 0, 0.5, 1000, 1000000);
    }

    @Test
    public void testBreakEvenZero()
    {
        checkAvailable(1500000, 1000000, 500000, 0.0, 1000, 1000);
    }

    @Test
    public void testBreakLruZero()
    {
        checkAvailable(1000000, 1000000, 500000, 0.5, 0, 1000);
    }

    @Test
    public void testLruOneWeekHalflifeOneWeek()
    {
        checkAvailable((long) (1000000 + 500000 - 500000 * 0.5 / Math.log(2)),
                       1000000, 500000, 0.5, (7 * 24 * 3600), 1000);
    }

    @Test
    public void testLruOneWeekHalflifeOneWeekLessThanGap()
    {
        checkAvailable(0, 1000000, 500000, 0.5, (7 * 24 * 3600), 1500000);
    }
}
