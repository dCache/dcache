package org.dcache.tests.poolmanager;

import com.google.common.util.concurrent.MoreExecutors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import diskCacheV111.poolManager.CostModuleV1;
import diskCacheV111.poolManager.PoolMonitorV5;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnitV2;
import diskCacheV111.poolManager.RequestContainerV5;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.OSMStorageInfo;
import diskCacheV111.vehicles.PoolFetchFileMessage;
import diskCacheV111.vehicles.PoolManagerPoolUpMessage;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;

import org.dcache.namespace.FileAttribute;
import org.dcache.pool.classic.IoQueueManager;
import org.dcache.poolmanager.PartitionManager;
import org.dcache.tests.cells.MockCellEndpoint;
import org.dcache.tests.cells.MockCellEndpoint.MessageAction;
import org.dcache.util.Args;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HsmRestoreTest {

    private static int _counter;
//    retry intervall of RequestContainer for test purposes lowered
    private static final int RETRY_INTERVAL = 5;

    private MockCellEndpoint _cell;
    private PoolMonitorV5 _poolMonitor;
    private CostModuleV1 _costModule ;
    private PoolSelectionUnit _selectionUnit;
    private PartitionManager _partitionManager;
    private PnfsHandler      _pnfsHandler;
    private RequestContainerV5 _rc;

    private List<CellMessage> __messages ;


    private ProtocolInfo _protocolInfo;
    private StorageInfo _storageInfo;


    @Before
    public void setUp() throws Exception {
        _counter = _counter + 1;
        _cell= new MockCellEndpoint("HsmRestoreTest" + _counter, "");

         _protocolInfo = new DCapProtocolInfo("DCap", 3, 0,
            new InetSocketAddress("127.0.0.1", 17));
        _storageInfo = new OSMStorageInfo("h1", "rawd");

        _partitionManager = new PartitionManager();
        _selectionUnit = new PoolSelectionUnitV2();
        _costModule = new CostModuleV1();

        _pnfsHandler = new PnfsHandler(new CellPath("PnfsManager"));
        _pnfsHandler.setCellEndpoint(_cell);
        _poolMonitor = new PoolMonitorV5();
        _poolMonitor.setPoolSelectionUnit(_selectionUnit);
        _poolMonitor.setCostModule(_costModule);
        _poolMonitor.setPartitionManager(_partitionManager);

        /*
         * allow stage
         */
        _partitionManager.ac_pm_set_$_0_1(new Args("-stage-allowed=yes"));
        _rc = new RequestContainerV5(RETRY_INTERVAL);
        _rc.setPoolSelectionUnit(_selectionUnit);
        _rc.setPnfsHandler(_pnfsHandler);
        _rc.setPoolMonitor(_poolMonitor);
        _rc.setPartitionManager(_partitionManager);
        _rc.setExecutor(MoreExecutors.sameThreadExecutor());
        _rc.setCellEndpoint(_cell);
        _rc.ac_rc_set_retry_$_1(new Args("0"));
        _rc.setStageConfigurationFile(null);
        _rc.setPnfsHandler(_pnfsHandler);
        __messages = new ArrayList<>();
    }

    @Test
    public void testRestoreNoLocations() throws Exception {

        PnfsId pnfsId = new PnfsId("000000000000000000000000000000000001");


        /*
         * pre-configure pool selection unit
         */
        List<String> pools = new ArrayList<>(3);
        pools.add("pool1");
        pools.add("pool2");
        PoolMonitorHelper.prepareSelectionUnit(_selectionUnit, pools);

        /*
         * prepare reply for GetStorageInfo
         */

        _storageInfo.addLocation(new URI("osm://osm?"));
        _storageInfo.setIsNew(false);

        PnfsGetFileAttributes fileAttributesMessage =
            new PnfsGetFileAttributes(pnfsId, EnumSet.noneOf(FileAttribute.class));
        FileAttributes attributes = new FileAttributes();
        attributes.setStorageInfo(_storageInfo);
        attributes.setPnfsId(pnfsId);
        attributes.setLocations(Collections.<String>emptyList());
        attributes.setSize(5);
        attributes.setAccessLatency(StorageInfo.DEFAULT_ACCESS_LATENCY);
        attributes.setRetentionPolicy(StorageInfo.DEFAULT_RETENTION_POLICY);
        fileAttributesMessage.setFileAttributes(attributes);
        _cell.prepareMessage(new CellPath("PnfsManager"), fileAttributesMessage, true);


        /*
         * make pools know to 'PoolManager'
         */

        long serialId = System.currentTimeMillis();
        PoolV2Mode poolMode = new PoolV2Mode(PoolV2Mode.ENABLED);
        Set<String> connectedHSM = new HashSet<>(1);
        connectedHSM.add("osm");

        for( String pool : pools) {

            PoolCostInfo poolCostInfo = new PoolCostInfo(pool);
            poolCostInfo.setSpaceUsage(100, 20, 30, 50);
            poolCostInfo.setQueueSizes(0, 10, 0, 0, 10, 0);
            poolCostInfo.addExtendedMoverQueueSizes(IoQueueManager.DEFAULT_QUEUE, 0, 10, 0, 0, 0);

            CellMessage envelope = new CellMessage(new CellPath("irrelevant"), null);
            envelope.addSourceAddress(new CellAddressCore(pool));
            PoolManagerPoolUpMessage poolUpMessage = new PoolManagerPoolUpMessage(pool, serialId, poolMode, poolCostInfo);

            prepareSelectionUnit(pool, poolMode, connectedHSM);
            _costModule.messageArrived(envelope, poolUpMessage);

        }


        final AtomicInteger stageRequests = new AtomicInteger(0);

        MessageAction messageAction = new StageMessageAction(stageRequests);

        _cell.registerAction("pool1", PoolFetchFileMessage.class, messageAction);
        _cell.registerAction("pool2", PoolFetchFileMessage.class, messageAction);

        PoolMgrSelectReadPoolMsg selectReadPool = new PoolMgrSelectReadPoolMsg(attributes, _protocolInfo, null);
        CellMessage cellMessage = new CellMessage( new CellPath("PoolManager"), selectReadPool);

        _rc.messageArrived(cellMessage, selectReadPool);


        assertEquals("No stage request sent to pools", 1, stageRequests.get());

    }


    @Test
    public void testRestoreNoLocationsOnePoolCantStage() throws Exception {

        PnfsId pnfsId = new PnfsId("000000000000000000000000000000000001");

        /*
         * pre-configure pool selection unit
         */
        List<String> pools = new ArrayList<>(3);
        pools.add("pool1");
        pools.add("pool2");
        PoolMonitorHelper.prepareSelectionUnit(_selectionUnit, pools);

        /*
         * prepare reply for GetStorageInfo
         */

        _storageInfo.addLocation(new URI("osm://osm?"));
        _storageInfo.setIsNew(false);

        PnfsGetFileAttributes fileAttributesMessage =
            new PnfsGetFileAttributes(pnfsId, EnumSet.noneOf(FileAttribute.class));
        FileAttributes attributes = new FileAttributes();
        attributes.setStorageInfo(_storageInfo);
        attributes.setPnfsId(pnfsId);
        attributes.setLocations(Collections.<String>emptyList());
        attributes.setSize(5);
        attributes.setAccessLatency(StorageInfo.DEFAULT_ACCESS_LATENCY);
        attributes.setRetentionPolicy(StorageInfo.DEFAULT_RETENTION_POLICY);
        fileAttributesMessage.setFileAttributes(attributes);
        _cell.prepareMessage(new CellPath("PnfsManager"), fileAttributesMessage, true);



        /*
         * make pools know to 'PoolManager'
         */

        long serialId = System.currentTimeMillis();
        PoolV2Mode poolMode = new PoolV2Mode(PoolV2Mode.ENABLED);
        Set<String> connectedHSM = new HashSet<>(1);
        connectedHSM.add("osm");

        for( String pool : pools) {

            PoolCostInfo poolCostInfo = new PoolCostInfo(pool);
            poolCostInfo.setSpaceUsage(100, 20, 30, 50);
            poolCostInfo.setQueueSizes(0, 10, 0, 0, 10, 0);
            poolCostInfo.addExtendedMoverQueueSizes(IoQueueManager.DEFAULT_QUEUE, 0, 10, 0, 0, 0);

            CellMessage envelope = new CellMessage(new CellPath(""), null);
            envelope.addSourceAddress(new CellAddressCore(pool));
            PoolManagerPoolUpMessage poolUpMessage = new PoolManagerPoolUpMessage(pool, serialId, poolMode, poolCostInfo);

            prepareSelectionUnit(pool, poolMode, connectedHSM);
            _costModule.messageArrived(envelope, poolUpMessage);

        }


        final AtomicInteger stageRequests1 = new AtomicInteger(0);
        final AtomicInteger stageRequests2 = new AtomicInteger(0);
        final AtomicInteger replyRequest = new AtomicInteger(0);

        MessageAction messageAction1 = new StageMessageAction(stageRequests1);
        MessageAction messageAction2 = new StageMessageAction(stageRequests2);
        MessageAction messageAction3 = new StageMessageAction(replyRequest);
        _cell.registerAction("pool1", PoolFetchFileMessage.class, messageAction1);
        _cell.registerAction("pool2", PoolFetchFileMessage.class, messageAction2);
        _cell.registerAction("door", PoolMgrSelectReadPoolMsg.class, messageAction3);

        PoolMgrSelectReadPoolMsg selectReadPool = new PoolMgrSelectReadPoolMsg(attributes, _protocolInfo, null);
        CellMessage cellMessage = new CellMessage( new CellPath("PoolManager"), selectReadPool);
        cellMessage.getSourcePath().add("door", "local");

        _rc.messageArrived(cellMessage, selectReadPool);

        // first pool replies with an error
        CellMessage m = __messages.remove(0);
        PoolFetchFileMessage ff = (PoolFetchFileMessage) m.getMessageObject();
        ff.setFailed(17, "pech");
        _rc.messageArrived(m, m.getMessageObject());

        // pool manager bounces request back to door
        m = __messages.remove(0);
        selectReadPool = (PoolMgrSelectReadPoolMsg) m.getMessageObject();
        assertEquals("Unexpected reply from pool manager",
                     17, selectReadPool.getReturnCode())
;

        // resubmit request
        PoolMgrSelectReadPoolMsg selectReadPool2 = new PoolMgrSelectReadPoolMsg(attributes, _protocolInfo, selectReadPool.getContext());
        CellMessage cellMessage2 = new CellMessage( new CellPath("PoolManager"), selectReadPool2);
        _rc.messageArrived(cellMessage2, selectReadPool2);

        assertEquals("No stage request sent to pools1", 1, stageRequests1.get());
        assertEquals("No stage request sent to pools2", 1, stageRequests2.get());
    }


    @Test
    public void testRestoreNoLocationsSinglePool() throws Exception {

        PnfsId pnfsId = new PnfsId("000000000000000000000000000000000001");


        /*
         * pre-configure pool selection unit
         */
        List<String> pools = new ArrayList<>(3);
        pools.add("pool1");
        PoolMonitorHelper.prepareSelectionUnit(_selectionUnit, pools);

        /*
         * prepare reply for GetStorageInfo
         */

        _storageInfo.addLocation(new URI("osm://osm?"));
        _storageInfo.setIsNew(false);

        PnfsGetFileAttributes fileAttributesMessage =
            new PnfsGetFileAttributes(pnfsId, EnumSet.noneOf(FileAttribute.class));
        FileAttributes attributes = new FileAttributes();
        attributes.setStorageInfo(_storageInfo);
        attributes.setPnfsId(pnfsId);
        attributes.setLocations(Collections.<String>emptyList());
        attributes.setSize(5);
        attributes.setAccessLatency(StorageInfo.DEFAULT_ACCESS_LATENCY);
        attributes.setRetentionPolicy(StorageInfo.DEFAULT_RETENTION_POLICY);
        fileAttributesMessage.setFileAttributes(attributes);
        _cell.prepareMessage(new CellPath("PnfsManager"), fileAttributesMessage, true);



        /*
         * make pools know to 'PoolManager'
         */

        long serialId = System.currentTimeMillis();
        PoolV2Mode poolMode = new PoolV2Mode(PoolV2Mode.ENABLED);
        Set<String> connectedHSM = new HashSet<>(1);
        connectedHSM.add("osm");

        for( String pool : pools) {

            PoolCostInfo poolCostInfo = new PoolCostInfo(pool);
            poolCostInfo.setSpaceUsage(100, 20, 30, 50);
            poolCostInfo.setQueueSizes(0, 10, 0, 0, 10, 0);
            poolCostInfo.addExtendedMoverQueueSizes(IoQueueManager.DEFAULT_QUEUE, 0, 10, 0, 0, 0);

            CellMessage envelope = new CellMessage(new CellPath(""), null);
            envelope.addSourceAddress(new CellAddressCore(pool));
            PoolManagerPoolUpMessage poolUpMessage = new PoolManagerPoolUpMessage(pool, serialId, poolMode, poolCostInfo);

            prepareSelectionUnit(pool, poolMode, connectedHSM);
            _costModule.messageArrived(envelope, poolUpMessage);

        }


        final AtomicInteger stageRequests1 = new AtomicInteger(0);
        final AtomicInteger replyRequest = new AtomicInteger(0);

        MessageAction messageAction1 = new StageMessageAction(stageRequests1);
        MessageAction messageAction2 = new StageMessageAction(replyRequest);
        _cell.registerAction("pool1", PoolFetchFileMessage.class, messageAction1);
        _cell.registerAction("door", PoolMgrSelectReadPoolMsg.class, messageAction2);

        PoolMgrSelectReadPoolMsg selectReadPool = new PoolMgrSelectReadPoolMsg(attributes, _protocolInfo, null);
        CellMessage cellMessage = new CellMessage( new CellPath("PoolManager"), selectReadPool);
        cellMessage.getSourcePath().add("door", "local");

        _rc.messageArrived(cellMessage, selectReadPool);

        // pool replies with an error
        CellMessage m = __messages.remove(0);
        PoolFetchFileMessage ff = (PoolFetchFileMessage)m.getMessageObject();
        ff.setFailed(17, "pech");
        _rc.messageArrived(m, m.getMessageObject());

        // pool manager bounces request back to door
        m = __messages.remove(0);
        selectReadPool = (PoolMgrSelectReadPoolMsg) m.getMessageObject();
        assertEquals("Unexpected reply from pool manager",
                     17, selectReadPool.getReturnCode())
;

        // resubmit request
        PoolMgrSelectReadPoolMsg selectReadPool2 = new PoolMgrSelectReadPoolMsg(attributes, _protocolInfo, selectReadPool.getContext());
        CellMessage cellMessage2 = new CellMessage( new CellPath("PoolManager"), selectReadPool2);
        _rc.messageArrived(cellMessage2, selectReadPool2);


        assertEquals("Single Pool excluded on second shot", 2, stageRequests1.get());


    }


    @Test
    public void testRestoreNoLocationsAllPoolsCantStage() throws Exception {

        PnfsId pnfsId = new PnfsId("000000000000000000000000000000000001");

        /*
         * pre-configure pool selection unit
         */
        List<String> pools = new ArrayList<>(3);
        pools.add("pool1");
        pools.add("pool2");
        PoolMonitorHelper.prepareSelectionUnit(_selectionUnit, pools);

        /*
         * prepare reply for GetStorageInfo
         */

        _storageInfo.addLocation(new URI("osm://osm?"));
        _storageInfo.setIsNew(false);

        PnfsGetFileAttributes fileAttributesMessage =
            new PnfsGetFileAttributes(pnfsId, EnumSet.noneOf(FileAttribute.class));
        FileAttributes attributes = new FileAttributes();
        attributes.setStorageInfo(_storageInfo);
        attributes.setPnfsId(pnfsId);
        attributes.setLocations(Collections.<String>emptyList());
        attributes.setSize(5);
        attributes.setAccessLatency(StorageInfo.DEFAULT_ACCESS_LATENCY);
        attributes.setRetentionPolicy(StorageInfo.DEFAULT_RETENTION_POLICY);
        fileAttributesMessage.setFileAttributes(attributes);
        _cell.prepareMessage(new CellPath("PnfsManager"), fileAttributesMessage, true);



        /*
         * make pools know to 'PoolManager'
         */

        long serialId = System.currentTimeMillis();
        PoolV2Mode poolMode = new PoolV2Mode(PoolV2Mode.ENABLED);
        Set<String> connectedHSM = new HashSet<>(1);
        connectedHSM.add("osm");

        for( String pool : pools) {

            PoolCostInfo poolCostInfo = new PoolCostInfo(pool);
            poolCostInfo.setSpaceUsage(100, 20, 30, 50);
            poolCostInfo.setQueueSizes(0, 10, 0, 0, 10, 0);
            poolCostInfo.addExtendedMoverQueueSizes(IoQueueManager.DEFAULT_QUEUE, 0, 10, 0, 0, 0);

            CellMessage envelope = new CellMessage(new CellPath(""), null);
            envelope.addSourceAddress(new CellAddressCore(pool));
            PoolManagerPoolUpMessage poolUpMessage = new PoolManagerPoolUpMessage(pool, serialId, poolMode, poolCostInfo);

            prepareSelectionUnit(pool, poolMode, connectedHSM);
            _costModule.messageArrived(envelope, poolUpMessage);

        }


        final AtomicInteger stageRequests1 = new AtomicInteger(0);
        final AtomicInteger stageRequests2 = new AtomicInteger(0);
        final AtomicInteger replyRequest = new AtomicInteger(0);

        MessageAction messageAction1 = new StageMessageAction(stageRequests1);
        MessageAction messageAction2 = new StageMessageAction(stageRequests2);
        MessageAction messageAction3 = new StageMessageAction(replyRequest);
        _cell.registerAction("pool1", PoolFetchFileMessage.class, messageAction1);
        _cell.registerAction("pool2", PoolFetchFileMessage.class, messageAction2);
        _cell.registerAction("door", PoolMgrSelectReadPoolMsg.class, messageAction3);

        PoolMgrSelectReadPoolMsg selectReadPool = new PoolMgrSelectReadPoolMsg(attributes, _protocolInfo, null);
        CellMessage cellMessage = new CellMessage( new CellPath("PoolManager"), selectReadPool);
        cellMessage.getSourcePath().add("door", "local");

        _rc.messageArrived(cellMessage, selectReadPool);

        // first pool replies with an error
        CellMessage m = __messages.remove(0);
        PoolFetchFileMessage ff = (PoolFetchFileMessage)m.getMessageObject();
        ff.setFailed(17, "pech");
        _rc.messageArrived(m, m.getMessageObject());

        // pool manager bounces request back to door
        m = __messages.remove(0);
        selectReadPool = (PoolMgrSelectReadPoolMsg) m.getMessageObject();
        assertEquals("Unexpected reply from pool manager",
                     17, selectReadPool.getReturnCode());

        // resubmit request
        PoolMgrSelectReadPoolMsg selectReadPool2 = new PoolMgrSelectReadPoolMsg(attributes, _protocolInfo, selectReadPool.getContext());
        CellMessage cellMessage2 = new CellMessage( new CellPath("PoolManager"), selectReadPool2);
        cellMessage2.getSourcePath().add("door", "local");
        _rc.messageArrived(cellMessage2, selectReadPool2);

        // second pool replies with an error
        m = __messages.remove(0);
        ff = (PoolFetchFileMessage)m.getMessageObject();
        ff.setFailed(17, "pech");
        _rc.messageArrived(m, m.getMessageObject());

        // pool manager bounces request back to door
        m = __messages.remove(0);
        selectReadPool2 = (PoolMgrSelectReadPoolMsg) m.getMessageObject();
        assertEquals("Unexpected reply from pool manager",
                     17, selectReadPool.getReturnCode());

        // resubmit request
        PoolMgrSelectReadPoolMsg selectReadPool3 = new PoolMgrSelectReadPoolMsg(attributes, _protocolInfo, selectReadPool2.getContext());
        CellMessage cellMessage3 = new CellMessage( new CellPath("PoolManager"), selectReadPool2);
        _rc.messageArrived(cellMessage3, selectReadPool3);

        assertEquals("Three stage requests where expected", 3,
                     stageRequests1.get() + stageRequests2.get());
        assertTrue("No stage requests sent to pool1",
                   stageRequests1.get() != 0);
        assertTrue("No stage requests sent to pool2",
                   stageRequests2.get() != 0);
    }

    private void prepareSelectionUnit(String pool,
            PoolV2Mode poolMode, Set<String> connectedHSM) {
        _selectionUnit.getPool(pool).setHsmInstances(connectedHSM);
        _selectionUnit.getPool(pool).setPoolMode(poolMode);
    }

    @After
    public void clear() {
        _rc.shutdown();
    }

    private class StageMessageAction implements MessageAction {

        private final AtomicInteger _count;

        StageMessageAction(AtomicInteger ai) {
            _count = ai;
        }

        @Override
        public void messageArrived(CellMessage message) {
            _count.incrementAndGet();
            __messages.add(message);
        }
    }

}
