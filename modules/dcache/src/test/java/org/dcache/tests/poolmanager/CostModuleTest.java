package org.dcache.tests.poolmanager;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import diskCacheV111.poolManager.CostModuleV1;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.vehicles.PoolManagerPoolUpMessage;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellMessage;

import org.dcache.pool.classic.IoQueueManager;

import static org.dcache.util.ByteUnit.GiB;
import static org.junit.Assert.*;

public class CostModuleTest {

    private static final String POOL_NAME = "aPool";
    private static final String POOL_NAME_2 = "anotherPool";
    private static final String POOL_NAME_3 = "yetAnotherPool";

    private static final CellAddressCore POOL_ADDRESS = new CellAddressCore("aPool", "poolDomain");
    private static final CellAddressCore POOL_ADDRESS_2 = new CellAddressCore("anotherPool", "poolDomain");
    private static final CellAddressCore POOL_ADDRESS_3 = new CellAddressCore("yetAnotherPool", "poolDomain");


    private static final long DEFAULT_FILE_SIZE = 100;
    private static final double DEFAULT_PERCENTILE = 0.95;

    /*
     * Various numbers used to test percentile cost threshold
     */
    public static final double FRACTION_JUST_ABOVE_ZERO = 0.001;
    public static final double FRACTION_JUST_BELOW_ONE = 0.999;
    public static final double FRACTION_HALF = 0.5;
    public static final double FRACTION_JUST_BELOW_HALF = 0.499;
    public static final double FRACTION_JUST_BELOW_ONE_THIRD = 0.333;
    public static final double FRACTION_JUST_ABOVE_ONE_THIRD = 0.334;
    public static final double FRACTION_JUST_BELOW_TWO_THIRDS = 0.666;
    public static final double FRACTION_JUST_ABOVE_TWO_THIRDS = 0.667;

    private static final double INFINITE_PERF_COST_VALUE = 1000000;

    private CostModuleV1 _costModule;

    @Before
    public void setUp() throws ClassNotFoundException, NoSuchMethodException {
        _costModule = new CostModuleV1();
    }

    @Test
    public void testPoolNotExist() {
        assertNull( "getPoolCostInfo() on non existing pool", _costModule.getPoolCostInfo( POOL_NAME));
        assertEquals( "getPoolsPercentilePerformanceCost on non-existing pool", 0, _costModule.getPoolsPercentilePerformanceCost( DEFAULT_PERCENTILE), 0);
    }


    @Test
    public void testPoolUpWithoutCost() {
        _costModule.messageArrived(buildEnvelope(POOL_ADDRESS), buildEmptyPoolUpMessage( POOL_NAME, PoolV2Mode.ENABLED));

        assertNull("getPoolCostInfo on a pool without costInfo", _costModule.getPoolCostInfo( POOL_NAME));
        assertEquals( "getPoolsPercentilePerformanceCost on a pool without costInfo", 0, _costModule.getPoolsPercentilePerformanceCost( DEFAULT_PERCENTILE), 0);
    }


    @Test
    public void testPoolUpWithCost() {
        long totalSpace = 100;
        long freeSpace = 30;
        long removableSpace = 20;
        long preciousSpace = 10;

        _costModule.messageArrived(
                buildEnvelope(POOL_ADDRESS),
                buildPoolUpMessageWithCost( POOL_NAME, totalSpace,
                        freeSpace, preciousSpace,
                        removableSpace));

        PoolCostInfo receivedCost = _costModule.getPoolCostInfo( POOL_NAME);

        assertNotNull("should return non null cost on a pool with costInfo",
                      receivedCost);
        assertPoolSpaceInfo( "pool", receivedCost.getSpaceInfo(),  totalSpace,
                             freeSpace, removableSpace, preciousSpace);

        assertEquals( "getPoolsPercentilePerformanceCost on a single pool",
                      INFINITE_PERF_COST_VALUE,
                      _costModule.getPoolsPercentilePerformanceCost( DEFAULT_PERCENTILE), 0);
    }

    @Test
    public void testPoolUpWithCostAndQueues() {
        long totalSpace = 100;
        long freeSpace = 30;
        long removableSpace = 20;
        long preciousSpace = 10;

        /* Add a pool with capacity and queue information: no current activity */
        _costModule.messageArrived(
                buildEnvelope(POOL_ADDRESS),
                buildPoolUpMessageWithCostAndQueue( POOL_NAME,
                        totalSpace, freeSpace,
                        preciousSpace, removableSpace,
                        0, 100, 0, 0, 0, 0, 0, 0, 0));

        PoolCostInfo receivedCost = _costModule.getPoolCostInfo( POOL_NAME);

        assertNotNull("should return non null cost on a pool with costInfo",
                      receivedCost);
        assertPoolSpaceInfo( "pool", receivedCost.getSpaceInfo(),  totalSpace,
                             freeSpace, removableSpace, preciousSpace);

        double perfCost = getPerformanceCostOfPercentileFile( POOL_NAME);

        assertTrue( "perf cost for pool >= 0", perfCost >= 0);
        assertTrue( "perf cost for pool != INF", perfCost != INFINITE_PERF_COST_VALUE);
        assertEquals( "getPoolsPercentilePerformanceCost on a single pool",
                      perfCost,
                      _costModule.getPoolsPercentilePerformanceCost( DEFAULT_PERCENTILE), 0);
    }

    @Test
    public void testPoolUpThenDisabled() {

        _costModule.messageArrived(
                buildEnvelope(POOL_ADDRESS), buildPoolUpMessageWithCost( POOL_NAME, 100, 20, 30, 50));

        // set pool DISABLED
        _costModule.messageArrived(
                buildEnvelope(POOL_ADDRESS), buildEmptyPoolUpMessage( POOL_NAME, PoolV2Mode.DISABLED));

        PoolCostInfo receivedCost = _costModule.getPoolCostInfo( POOL_NAME);
        assertNull("should return null cost on a DOWN pool", receivedCost);
    }


    @Test
    public void testPoolUpThenDead() {

        _costModule.messageArrived(
                buildEnvelope(POOL_ADDRESS), buildPoolUpMessageWithCost( POOL_NAME, 100, 20, 30, 50));

        // set pool DEAD
        _costModule.messageArrived(
                buildEnvelope(POOL_ADDRESS), buildEmptyPoolUpMessage( POOL_NAME, PoolV2Mode.DISABLED_DEAD));

        PoolCostInfo receivedCost = _costModule.getPoolCostInfo( POOL_NAME);
        assertNull("should return null cost on a DEAD pool", receivedCost);
    }


    @Test
    public void testTwoPoolsThenPercentile() {

        // Add pool with no activity
        _costModule.messageArrived(
                buildEnvelope(POOL_ADDRESS),
                buildPoolUpMessageWithCostAndQueue(
                        POOL_NAME,
                        100, 20, 30, 50,
                        0, 100, 0,
                        0, 0, 0,
                        0, 0, 0));

        // Add pool with some activity
        _costModule.messageArrived(
                buildEnvelope(POOL_ADDRESS_2),
                buildPoolUpMessageWithCostAndQueue(
                        POOL_NAME_2,
                        100, 20, 30, 50,
                        20, 100, 0,
                        0, 0, 0,
                        0, 0, 0));

        double pool1PerfCost = getPerformanceCostOfPercentileFile( POOL_NAME);
        double pool2PerfCost = getPerformanceCostOfPercentileFile( POOL_NAME_2);

        double minPerfCost = Math.min( pool1PerfCost, pool2PerfCost);
        double maxPerfCost = Math.max( pool1PerfCost, pool2PerfCost);

        assertPercentileCost( FRACTION_JUST_ABOVE_ZERO, minPerfCost);
        assertPercentileCost( FRACTION_JUST_BELOW_HALF, minPerfCost);
        assertPercentileCost( FRACTION_HALF, maxPerfCost);
        assertPercentileCost( FRACTION_JUST_BELOW_ONE, maxPerfCost);
    }


    @Test
    public void testThreePoolsThenPercentile() {

        // Add pool with no activity
        _costModule.messageArrived(
                buildEnvelope(POOL_ADDRESS),
                buildPoolUpMessageWithCostAndQueue(
                        POOL_NAME,
                        100, 20, 30, 50,
                        0, 100, 0,
                        0, 0, 0,
                        0, 0, 0));

        // Add pool with some activity
        _costModule.messageArrived(
                buildEnvelope(POOL_ADDRESS_2),
                buildPoolUpMessageWithCostAndQueue(
                        POOL_NAME_2,
                        100, 20, 30, 50,
                        20, 100, 0,
                        0, 0, 0,
                        0, 0, 0));

        // Add pool with more activity
        _costModule.messageArrived(
                buildEnvelope(POOL_ADDRESS_3),
                buildPoolUpMessageWithCostAndQueue(
                        POOL_NAME_3,
                        100, 20, 30, 50,
                        40, 100, 0,
                        0, 0, 0,
                        0, 0, 0));

        double perfCost[] = new double[3];

        perfCost [0] = getPerformanceCostOfPercentileFile( POOL_NAME);
        perfCost [1] = getPerformanceCostOfPercentileFile( POOL_NAME_2);
        perfCost [2] = getPerformanceCostOfPercentileFile( POOL_NAME_3);

        Arrays.sort( perfCost);

        assertPercentileCost( FRACTION_JUST_ABOVE_ZERO, perfCost [0]);
        assertPercentileCost( FRACTION_JUST_BELOW_ONE_THIRD, perfCost [0]);
        assertPercentileCost( FRACTION_JUST_ABOVE_ONE_THIRD, perfCost [1]);
        assertPercentileCost( FRACTION_JUST_BELOW_TWO_THIRDS, perfCost [1]);
        assertPercentileCost( FRACTION_JUST_ABOVE_TWO_THIRDS, perfCost [2]);
        assertPercentileCost( FRACTION_JUST_BELOW_ONE, perfCost [2]);
    }

    /*
     *  SUPPORT METHODS FOR BUILDING MESSAGES AND ASSERTING
     */


    private static CellMessage buildEnvelope(CellAddressCore source)
    {
        CellMessage envelope = new CellMessage(new CellAddressCore("irrelevant"), null);
        envelope.addSourceAddress(source);
        return envelope;
    }

    /**
     * Create an empty Pool-up message for a pool with a given state
     * @param poolName the name of the pool
     * @param poolV2Mode in what state to claim the pool is.
     */
    private static PoolManagerPoolUpMessage buildEmptyPoolUpMessage( String poolName,
                                                                     int poolV2Mode) {
        PoolV2Mode poolMode = new PoolV2Mode(poolV2Mode);

        long serialId = System.currentTimeMillis();

        return new PoolManagerPoolUpMessage( poolName, serialId, poolMode);
    }


    /**
     * Create a pool-up message for a pool with the given space parameters
     * @param poolName name of the pool
     * @param totalSpace total capacity of the pool in Gigabytes
     * @param freeSpace space not yet used in Gigabytes
     * @param preciousSpace space used by files marked precious in Gigabytes
     * @param removableSpace space used by files that are removable in Gigabytes
     * @return
     */
    private static PoolManagerPoolUpMessage buildPoolUpMessageWithCost( String poolName,
                                                                long totalSpace,
                                                                long freeSpace,
                                                                long preciousSpace,
                                                                long removableSpace) {

        PoolV2Mode poolMode = new PoolV2Mode(PoolV2Mode.ENABLED);
        PoolCostInfo poolCost = new PoolCostInfo( poolName, IoQueueManager.DEFAULT_QUEUE);

        poolCost.setSpaceUsage(GiB.toBytes(totalSpace),
                               GiB.toBytes(freeSpace),
                               GiB.toBytes(preciousSpace),
                               GiB.toBytes(removableSpace));

        long serialId = System.currentTimeMillis();

        return new PoolManagerPoolUpMessage( poolName, serialId, poolMode, poolCost);
    }

    /**
     * Create a pool-up message for a pool with the given space parameters and mover
     * values
     * @param poolName name of the pool
     * @param totalSpace total capacity of the pool in Gigabytes
     * @param freeSpace space not yet used in Gigabytes
     * @param preciousSpace space used by files marked precious in Gigabytes
     * @param removableSpace space used by files that are removable in Gigabytes
     *...
     * @return
     */
    private static PoolManagerPoolUpMessage buildPoolUpMessageWithCostAndQueue( String poolName,
                                                                long totalSpace, long freeSpace,
                                                                long preciousSpace, long removableSpace,
                                                                int moverActive, int moverMaxActive, int moverQueued,
                                                                int restoreActive, int restoreMaxActive, int restoreQueued,
                                                                int storeActive, int storeMaxActive, int storeQueued) {

        PoolV2Mode poolMode = new PoolV2Mode(PoolV2Mode.ENABLED);
        PoolCostInfo poolCost = new PoolCostInfo( poolName, IoQueueManager.DEFAULT_QUEUE);

        poolCost.setSpaceUsage(GiB.toBytes(totalSpace),
                               GiB.toBytes(freeSpace),
                               GiB.toBytes(preciousSpace),
                               GiB.toBytes(removableSpace));

        poolCost.setQueueSizes(
                restoreActive, restoreMaxActive, restoreQueued,
                storeActive, storeMaxActive, storeQueued);
        poolCost.addExtendedMoverQueueSizes(IoQueueManager.DEFAULT_QUEUE, moverActive, moverMaxActive, moverQueued, 0, 0);

        long serialId = System.currentTimeMillis();

        return new PoolManagerPoolUpMessage( poolName, serialId, poolMode, poolCost);
    }

    /**
     * Obtain the performance cost for the named pool for the filesize used by
     * the percentile calculations as "typical".
     * @param poolName
     * @return
     */
    private double getPerformanceCostOfPercentileFile( String poolName) {
        return _costModule.getPoolCostInfo(poolName).getPerformanceCost();
    }



    /**
     * Assert that the PoolCostInfo.PoolSpaceInfo object has total-, free-,
     * removable-, and precious space metrics that match those supplied.
     * All supplied metrics should be in gigabytes.
     * @param msg a prefix for the assert message.
     * @param info the PoolSpaceInfo to test
     * @param totalSpace the expected total Space, in gigabytes.
     * @param freeSpace the expected free Space, in gigabytes.
     * @param removableSpace the expected removable Space, in gigabytes.
     * @param preciousSpace the expected precious Space, in gigabytes.
     */
    private static void assertPoolSpaceInfo( String msg, PoolCostInfo.PoolSpaceInfo info,
                                             long totalSpace, long freeSpace,
                                             long removableSpace,
                                             long preciousSpace) {
        assertEquals(msg + ": total space", GiB.toBytes(totalSpace),
                      info.getTotalSpace());
        assertEquals(msg + ": free space", GiB.toBytes(freeSpace),
                      info.getFreeSpace());
        assertEquals(msg + ": removable space", GiB.toBytes(removableSpace),
                      info.getRemovableSpace());
        assertEquals(msg + ": precious space", GiB.toBytes(preciousSpace),
                      info.getPreciousSpace());
    }


    /**
     * Check that the percentile cost is the expected value.
     * @param fraction
     * @param expectedCost
     */
    private void assertPercentileCost( double fraction, double expectedCost) {
        assertEquals( "check " + Double.toString( expectedCost) + " percentile cost",
                      expectedCost,
                      _costModule.getPoolsPercentilePerformanceCost( fraction), 0);
    }
}
