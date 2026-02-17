package org.dcache.pool.migration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.SettableFuture;
import diskCacheV111.poolManager.PoolSelectionUnit.DirectionType;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PoolMgrQueryPoolsMsg;
import diskCacheV111.vehicles.PoolManagerGetPoolsByNameMessage;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.pool.classic.IoQueueManager;
import org.dcache.vehicles.FileAttributes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class PoolListByPoolMgrQueryTest {

    private CellStub poolManager;
    private FileAttributes fileAttributes;
    private PnfsId pnfsId;

    @Before
    public void setUp() {
        poolManager = mock(CellStub.class);
        fileAttributes = mock(FileAttributes.class);
        pnfsId = new PnfsId("0000A1B2C3D4E5F6");

        // Mock FileAttributes to have STORAGEINFO defined
        when(fileAttributes.isDefined(FileAttribute.STORAGEINFO)).thenReturn(true);
    }

    @Test
    public void testRefreshWithValidResponse() throws Exception {
        // Setup
        PoolListByPoolMgrQuery poolList = new PoolListByPoolMgrQuery(
              poolManager, pnfsId, fileAttributes, "DCap/3", "127.0.0.1");

        // Mock the send method to return SettableFuture
        SettableFuture<PoolMgrQueryPoolsMsg> future = SettableFuture.create();
        when(poolManager.send(any(PoolMgrQueryPoolsMsg.class))).thenReturn(future);

        // Setup the first message response (PoolMgrQueryPoolsMsg)
        @SuppressWarnings("unchecked")
        List<String>[] poolLists = new List[2];
        poolLists[0] = Arrays.asList("pool1", "pool2");
        poolLists[1] = Arrays.asList("pool3");

        // Mock the second send for PoolManagerGetPoolsByNameMessage
        SettableFuture<PoolManagerGetPoolsByNameMessage> poolInfoFuture = SettableFuture.create();

        // Setup mock to return different futures based on message type
        when(poolManager.send(any(PoolManagerGetPoolsByNameMessage.class)))
              .thenReturn(poolInfoFuture);

        // Call refresh
        poolList.refresh();

        // Verify that send was called and capture the message
        ArgumentCaptor<PoolMgrQueryPoolsMsg> queryMsgCaptor = ArgumentCaptor.forClass(
              PoolMgrQueryPoolsMsg.class);
        org.mockito.Mockito.verify(poolManager).send(queryMsgCaptor.capture());

        // Verify the query message parameters
        PoolMgrQueryPoolsMsg queryMsg = queryMsgCaptor.getValue();
        assertEquals(DirectionType.READ, queryMsg.getAccessType());
        assertEquals("DCap/3", queryMsg.getProtocolUnitName());
        assertEquals("127.0.0.1", queryMsg.getNetUnitName());
        assertEquals(fileAttributes, queryMsg.getFileAttributes());

        // Simulate the callback being invoked with a success response
        PoolMgrQueryPoolsMsg response = new PoolMgrQueryPoolsMsg(
              DirectionType.READ, "DCap/3", "127.0.0.1", fileAttributes);
        response.setPoolList(poolLists);
        response.setSucceeded();

        // Complete the future to trigger the callback
        future.set(response);

        // Wait a bit for async processing
        Thread.sleep(100);

        // Now simulate the pool info response
        List<PoolManagerPoolInformation> pools = new ArrayList<>();
        pools.add(new PoolManagerPoolInformation("pool1",
              new PoolCostInfo("pool1", IoQueueManager.DEFAULT_QUEUE), 0.5));
        pools.add(new PoolManagerPoolInformation("pool2",
              new PoolCostInfo("pool2", IoQueueManager.DEFAULT_QUEUE), 0.3));

        PoolManagerGetPoolsByNameMessage poolInfoResponse = new PoolManagerGetPoolsByNameMessage(
              Arrays.asList("pool1", "pool2"));
        poolInfoResponse.setPools(pools);
        List<String> offlinePools = new ArrayList<>();
        offlinePools.add("pool4");
        poolInfoResponse.setOfflinePools(offlinePools);
        poolInfoResponse.setSucceeded();

        poolInfoFuture.set(poolInfoResponse);

        // Wait for async processing
        Thread.sleep(100);

        // Verify the pool list is valid and has correct pools
        // Should only have pools from preference level 0 (pool1, pool2), not from level 1 (pool3)
        assertTrue(poolList.isValid());
        assertEquals(2, poolList.getPools().size());
        assertEquals("pool1", poolList.getPools().get(0).getName());
        assertEquals(1, poolList.getOfflinePools().size());
        assertEquals("pool4", poolList.getOfflinePools().get(0));
    }

    @Test
    public void testRefreshWithEmptyResponse() throws Exception {
        PoolListByPoolMgrQuery poolList = new PoolListByPoolMgrQuery(
              poolManager, pnfsId, fileAttributes, "DCap/3", "127.0.0.1");

        SettableFuture<PoolMgrQueryPoolsMsg> future = SettableFuture.create();
        when(poolManager.send(any(PoolMgrQueryPoolsMsg.class))).thenReturn(future);

        poolList.refresh();

        // Simulate empty response
        PoolMgrQueryPoolsMsg response = new PoolMgrQueryPoolsMsg(
              DirectionType.READ, "DCap/3", "127.0.0.1", fileAttributes);
        @SuppressWarnings("unchecked")
        List<String>[] emptyList = new List[0];
        response.setPoolList(emptyList);
        response.setSucceeded();

        future.set(response);
        Thread.sleep(100);

        assertTrue(poolList.isValid());
        assertEquals(0, poolList.getPools().size());
    }

    @Test
    public void testRefreshWithMissingStorageInfo() {
        when(fileAttributes.isDefined(FileAttribute.STORAGEINFO)).thenReturn(false);

        PoolListByPoolMgrQuery poolList = new PoolListByPoolMgrQuery(
              poolManager, pnfsId, fileAttributes, "DCap/3", "127.0.0.1");

        poolList.refresh();

        assertFalse(poolList.isValid());
        assertEquals(0, poolList.getPools().size());
    }

    @Test
    public void testRefreshWithNullNetUnitName() throws Exception {
        PoolListByPoolMgrQuery poolList = new PoolListByPoolMgrQuery(
              poolManager, pnfsId, fileAttributes, "DCap/3", null);

        SettableFuture<PoolMgrQueryPoolsMsg> future = SettableFuture.create();
        when(poolManager.send(any(PoolMgrQueryPoolsMsg.class))).thenReturn(future);

        poolList.refresh();

        ArgumentCaptor<PoolMgrQueryPoolsMsg> queryMsgCaptor = ArgumentCaptor.forClass(
              PoolMgrQueryPoolsMsg.class);
        org.mockito.Mockito.verify(poolManager).send(queryMsgCaptor.capture());

        PoolMgrQueryPoolsMsg queryMsg = queryMsgCaptor.getValue();
        assertNull(queryMsg.getNetUnitName());
    }

    /**
     * Test that only the highest preference level is selected when multiple pool groups have
     * different read preferences. This reproduces the test stand scenario where:
     * - flushPools (pool6-10) have readpref=5 (lower preference)
     * - readOnlyPools (pool2-5) have readpref=10 (higher preference)
     * - pool1 is in both groups
     * Expected: Only pools from the highest preference group (readOnlyPools) should be selected.
     */
    @Test
    public void testSelectsOnlyHighestPreferenceLevel() throws Exception {
        // Setup: Two pool groups with different read preferences
        // Pool1 is in both groups, pool2-5 only in high-pref group, pool6-10 only in low-pref group
        PoolListByPoolMgrQuery poolList = new PoolListByPoolMgrQuery(
              poolManager, pnfsId, fileAttributes, "DCap/3", null);

        SettableFuture<PoolMgrQueryPoolsMsg> queryFuture = SettableFuture.create();
        when(poolManager.send(any(PoolMgrQueryPoolsMsg.class))).thenReturn(queryFuture);

        // Mock the second send for PoolManagerGetPoolsByNameMessage
        SettableFuture<PoolManagerGetPoolsByNameMessage> poolInfoFuture = SettableFuture.create();
        when(poolManager.send(any(PoolManagerGetPoolsByNameMessage.class)))
              .thenReturn(poolInfoFuture);

        // Call refresh
        poolList.refresh();

        // Simulate PoolManager response with two preference levels
        // Level 0 (highest): readOnlyPools with readpref=10 (pool1, pool2, pool3, pool4, pool5)
        // Level 1 (lower): flushPools with readpref=5 (pool1, pool6, pool7, pool8, pool9, pool10)
        @SuppressWarnings("unchecked")
        List<String>[] poolLists = new List[2];
        poolLists[0] = Arrays.asList("pool1", "pool2", "pool3", "pool4", "pool5"); // High pref
        poolLists[1] = Arrays.asList("pool1", "pool6", "pool7", "pool8", "pool9", "pool10"); // Low pref

        PoolMgrQueryPoolsMsg response = new PoolMgrQueryPoolsMsg(
              DirectionType.READ, "DCap/3", null, fileAttributes);
        response.setPoolList(poolLists);
        response.setSucceeded();

        // Complete the future to trigger the callback
        queryFuture.set(response);
        Thread.sleep(100);

        // Verify that only the pools from preference level 0 were requested
        ArgumentCaptor<PoolManagerGetPoolsByNameMessage> poolInfoCaptor =
              ArgumentCaptor.forClass(PoolManagerGetPoolsByNameMessage.class);
        org.mockito.Mockito.verify(poolManager).send(poolInfoCaptor.capture());

        Collection<String> requestedPools = poolInfoCaptor.getValue().getPoolNames();
        assertEquals("Should only request pools from highest preference level",
              5, requestedPools.size());
        assertTrue("Should include pool1 from high-pref group", requestedPools.contains("pool1"));
        assertTrue("Should include pool2 from high-pref group", requestedPools.contains("pool2"));
        assertTrue("Should include pool3 from high-pref group", requestedPools.contains("pool3"));
        assertTrue("Should include pool4 from high-pref group", requestedPools.contains("pool4"));
        assertTrue("Should include pool5 from high-pref group", requestedPools.contains("pool5"));
        assertFalse("Should NOT include pool6 from low-pref group", requestedPools.contains("pool6"));
        assertFalse("Should NOT include pool7 from low-pref group", requestedPools.contains("pool7"));
        assertFalse("Should NOT include pool8 from low-pref group", requestedPools.contains("pool8"));
        assertFalse("Should NOT include pool9 from low-pref group", requestedPools.contains("pool9"));
        assertFalse("Should NOT include pool10 from low-pref group", requestedPools.contains("pool10"));

        // Complete the pool info request
        List<PoolManagerPoolInformation> pools = new ArrayList<>();
        for (String poolName : requestedPools) {
            pools.add(new PoolManagerPoolInformation(poolName,
                  new PoolCostInfo(poolName, IoQueueManager.DEFAULT_QUEUE), 0.5));
        }

        PoolManagerGetPoolsByNameMessage poolInfoResponse = new PoolManagerGetPoolsByNameMessage(
              requestedPools);
        poolInfoResponse.setPools(pools);
        poolInfoResponse.setOfflinePools(new ArrayList<>());
        poolInfoResponse.setSucceeded();

        poolInfoFuture.set(poolInfoResponse);
        Thread.sleep(100);

        // Verify the final pool list contains only high-preference pools
        assertTrue(poolList.isValid());
        assertEquals("Should have 5 pools from high-preference level",
              5, poolList.getPools().size());
    }

    /**
     * Test that the first non-empty preference level is selected when the highest level is empty.
     */
    @Test
    public void testSelectsFirstNonEmptyPreferenceLevel() throws Exception {
        PoolListByPoolMgrQuery poolList = new PoolListByPoolMgrQuery(
              poolManager, pnfsId, fileAttributes, "DCap/3", null);

        SettableFuture<PoolMgrQueryPoolsMsg> queryFuture = SettableFuture.create();
        when(poolManager.send(any(PoolMgrQueryPoolsMsg.class))).thenReturn(queryFuture);

        SettableFuture<PoolManagerGetPoolsByNameMessage> poolInfoFuture = SettableFuture.create();
        when(poolManager.send(any(PoolManagerGetPoolsByNameMessage.class)))
              .thenReturn(poolInfoFuture);

        poolList.refresh();

        // Simulate response with empty first level, pools in second level
        @SuppressWarnings("unchecked")
        List<String>[] poolLists = new List[3];
        poolLists[0] = new ArrayList<>(); // Empty highest preference
        poolLists[1] = Arrays.asList("pool3"); // First non-empty level
        poolLists[2] = Arrays.asList("pool1", "pool2"); // Lower preference (should be ignored)

        PoolMgrQueryPoolsMsg response = new PoolMgrQueryPoolsMsg(
              DirectionType.READ, "DCap/3", null, fileAttributes);
        response.setPoolList(poolLists);
        response.setSucceeded();

        queryFuture.set(response);
        Thread.sleep(100);

        // Verify that only pool3 was requested (from first non-empty level)
        ArgumentCaptor<PoolManagerGetPoolsByNameMessage> poolInfoCaptor =
              ArgumentCaptor.forClass(PoolManagerGetPoolsByNameMessage.class);
        org.mockito.Mockito.verify(poolManager).send(poolInfoCaptor.capture());

        Collection<String> requestedPools = poolInfoCaptor.getValue().getPoolNames();
        assertEquals(1, requestedPools.size());
        assertTrue("pool3 should be requested", requestedPools.contains("pool3"));

        // Complete the pool info request
        List<PoolManagerPoolInformation> pools = new ArrayList<>();
        pools.add(new PoolManagerPoolInformation("pool3",
              new PoolCostInfo("pool3", IoQueueManager.DEFAULT_QUEUE), 0.5));

        PoolManagerGetPoolsByNameMessage poolInfoResponse = new PoolManagerGetPoolsByNameMessage(
              requestedPools);
        poolInfoResponse.setPools(pools);
        poolInfoResponse.setOfflinePools(new ArrayList<>());
        poolInfoResponse.setSucceeded();

        poolInfoFuture.set(poolInfoResponse);
        Thread.sleep(100);

        assertTrue(poolList.isValid());
        assertEquals(1, poolList.getPools().size());
        assertEquals("pool3", poolList.getPools().get(0).getName());
    }

    @Test
    public void testToString() {
        PoolListByPoolMgrQuery poolList = new PoolListByPoolMgrQuery(
              poolManager, pnfsId, fileAttributes, null, null);

        String result = poolList.toString();
        assertTrue(result.contains("null")); // Protocol is null
        assertTrue(result.contains("0 pools"));
    }
}

