package org.dcache.pool.migration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import diskCacheV111.poolManager.PoolSelectionUnit.DirectionType;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PoolMgrQueryPoolsMsg;
import diskCacheV111.vehicles.PoolManagerGetPoolsByNameMessage;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.pool.classic.IoQueueManager;
import org.dcache.vehicles.FileAttributes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

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

        // Mock the send method to capture the callback and invoke it
        CompletableFuture<PoolMgrQueryPoolsMsg> future = new CompletableFuture<>();
        when(poolManager.send(any(PoolMgrQueryPoolsMsg.class))).thenReturn(future);

        // Setup the first message response (PoolMgrQueryPoolsMsg)
        List<String>[] poolLists = new List[2];
        poolLists[0] = Arrays.asList("pool1", "pool2");
        poolLists[1] = Arrays.asList("pool3");

        // Mock the second send for PoolManagerGetPoolsByNameMessage
        CompletableFuture<PoolManagerGetPoolsByNameMessage> poolInfoFuture = new CompletableFuture<>();

        // Capture the callback argument to invoke it later
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
        response.setPools(poolLists);
        response.setSucceeded();

        // Complete the future to trigger the callback
        future.complete(response);

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
        poolInfoResponse.setOfflinePools(Arrays.asList("pool4"));
        poolInfoResponse.setSucceeded();

        poolInfoFuture.complete(poolInfoResponse);

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

        CompletableFuture<PoolMgrQueryPoolsMsg> future = new CompletableFuture<>();
        when(poolManager.send(any(PoolMgrQueryPoolsMsg.class))).thenReturn(future);

        poolList.refresh();

        // Simulate empty response
        PoolMgrQueryPoolsMsg response = new PoolMgrQueryPoolsMsg(
              DirectionType.READ, "DCap/3", "127.0.0.1", fileAttributes);
        response.setPools(new List[0]);
        response.setSucceeded();

        future.complete(response);
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

        CompletableFuture<PoolMgrQueryPoolsMsg> future = new CompletableFuture<>();
        when(poolManager.send(any(PoolMgrQueryPoolsMsg.class))).thenReturn(future);

        poolList.refresh();

        ArgumentCaptor<PoolMgrQueryPoolsMsg> queryMsgCaptor = ArgumentCaptor.forClass(
              PoolMgrQueryPoolsMsg.class);
        org.mockito.Mockito.verify(poolManager).send(queryMsgCaptor.capture());

        PoolMgrQueryPoolsMsg queryMsg = queryMsgCaptor.getValue();
        assertEquals(null, queryMsg.getNetUnitName());
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

