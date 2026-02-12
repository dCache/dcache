package org.dcache.pool.migration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import diskCacheV111.poolManager.PoolSelectionUnit.DirectionType;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PoolMgrQueryPoolsMsg;
import diskCacheV111.vehicles.PoolManagerGetPoolsByNameMessage;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import java.util.ArrayList;
import java.util.Arrays;
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
        pools.add(new PoolManagerPoolInformation("pool3",
              new PoolCostInfo("pool3", IoQueueManager.DEFAULT_QUEUE), 0.7));

        PoolManagerGetPoolsByNameMessage poolInfoResponse = new PoolManagerGetPoolsByNameMessage(
              Arrays.asList("pool1", "pool2", "pool3"));
        poolInfoResponse.setPools(pools);
        List<String> offlinePools = new ArrayList<>();
        offlinePools.add("pool4");
        poolInfoResponse.setOfflinePools(offlinePools);
        poolInfoResponse.setSucceeded();

        poolInfoFuture.set(poolInfoResponse);

        // Wait for async processing
        Thread.sleep(100);

        // Verify the pool list is valid and has correct pools
        assertTrue(poolList.isValid());
        assertEquals(3, poolList.getPools().size());
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

    @Test
    public void testToString() {
        PoolListByPoolMgrQuery poolList = new PoolListByPoolMgrQuery(
              poolManager, pnfsId, fileAttributes, "DCap/3", "127.0.0.1");

        String result = poolList.toString();
        assertTrue(result.contains("DCap/3"));
        assertTrue(result.contains("0 pools"));
    }
}

