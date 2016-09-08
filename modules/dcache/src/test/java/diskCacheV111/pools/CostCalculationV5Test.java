package diskCacheV111.pools;

import org.junit.Test;

import org.dcache.pool.classic.IoQueueManager;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class CostCalculationV5Test
{
    private static final String POOL_NAME = "aPool";

    @Test
    public void whenQueuesAreEmptyPerformanceCostIsZero()
    {
        PoolCostInfo cost = buildPoolCost(
                0, 100, 0,
                0, 0, 0,
                0, 0, 0);
        assertThat(cost.getPerformanceCost(), is(0.0));
    }

    @Test
    public void whenQueuesHaveNoCapacityTheyStillCount()
    {
        PoolCostInfo cost = buildPoolCost(
                50, 100, 0,
                0, 0, 0,
                0, 0, 0);
        assertThat(cost.getPerformanceCost(), is(0.5 / 2));
    }

    @Test
    public void whenTapeQueuesHaveQueuedItemsTheyAreConsideredFull()
    {
        PoolCostInfo cost = buildPoolCost(
                0, 100, 0,
                0, 0, 1,
                0, 0, 1);
        assertThat(cost.getPerformanceCost(), is(1.0 / 2));
    }

    @Test
    public void whenTapeQueuesHaveActiveRequestsTheCostIsComputedLikeThis()
    {
        PoolCostInfo cost = buildPoolCost(
                0, 100, 0,
                0, 0, 0,
                10, 0, 0);
        assertThat(cost.getPerformanceCost(), is((1 - Math.pow(0.75, 10)) / 2));
    }

    @Test
    public void whenSeveralQueuesHaveRequestsTheCostIsTheAverage()
    {
        PoolCostInfo cost = buildPoolCost(
                50, 100, 0,
                10, 0, 0,
                5, 0, 0);
        assertThat(cost.getPerformanceCost(), is((0.5 + (1 - Math.pow(0.75, 5))) / 2));
    }

    private static PoolCostInfo buildPoolCost(int moverActive, int moverMaxActive, int moverQueued,
                                              int restoreActive, int restoreMaxActive, int restoreQueued,
                                              int storeActive, int storeMaxActive, int storeQueued)
    {
        PoolCostInfo poolCost = new PoolCostInfo(POOL_NAME, IoQueueManager.DEFAULT_QUEUE);
        poolCost.setQueueSizes(
                restoreActive, restoreMaxActive, restoreQueued,
                storeActive, storeMaxActive, storeQueued);
        poolCost.addExtendedMoverQueueSizes(IoQueueManager.DEFAULT_QUEUE, moverActive, moverMaxActive, moverQueued, 0, 0);
        return poolCost;
    }
}
