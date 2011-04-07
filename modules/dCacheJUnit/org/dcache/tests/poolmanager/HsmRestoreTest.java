package org.dcache.tests.poolmanager;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.EnumSet;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import org.dcache.tests.cells.GenericMockCellHelper;
import org.dcache.tests.cells.GenericMockCellHelper.MessageAction;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.poolManager.CostModuleV1;
import diskCacheV111.poolManager.PartitionManager;
import diskCacheV111.poolManager.PoolMonitorV5;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnitV2;
import diskCacheV111.poolManager.RequestContainerV5;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.ThreadPoolNG;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.OSMStorageInfo;
import diskCacheV111.vehicles.PoolFetchFileMessage;
import diskCacheV111.vehicles.PoolManagerPoolUpMessage;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.util.Args;
import org.dcache.tests.util.CurrentThreadExceutorHelper;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;
import org.dcache.namespace.FileAttribute;

import static org.junit.Assert.*;

public class HsmRestoreTest {

    private static int _counter = 0;
//    retry intervall of RequestContainer for test purposes lowered
    private static final int RETRY_INTERVAL = 5;

    private GenericMockCellHelper _cell = null;
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
        _cell= new GenericMockCellHelper("HsmRestoreTest" + _counter, "-threadPool=org.dcache.tests.util.CurrentThreadExceutorHelper");

        _protocolInfo = new DCapProtocolInfo("DCap", 3, 0, "127.0.0.1", 17);
        _storageInfo = new OSMStorageInfo("h1", "rawd");

        _partitionManager = new PartitionManager();
        _selectionUnit = new PoolSelectionUnitV2();
        _costModule = new CostModuleV1();
        _costModule.setCostCalculationEngine(new diskCacheV111.pools.CostCalculationEngine("diskCacheV111.pools.CostCalculationV5"));

        _pnfsHandler = new PnfsHandler(new CellPath("PnfsManager"));
        _pnfsHandler.setCellEndpoint(_cell);
        _poolMonitor = new PoolMonitorV5();
        _poolMonitor.setPoolSelectionUnit(_selectionUnit);
        _poolMonitor.setCostModule(_costModule);
        _poolMonitor.setPartitionManager(_partitionManager);

        /*
         * allow stage
         */
        _partitionManager.ac_rc_set_stage_$_1_2(new Args("on"));
        _rc = new RequestContainerV5(RETRY_INTERVAL);
        _rc.setPoolSelectionUnit(_selectionUnit);
        _rc.setPnfsHandler(_pnfsHandler);
        _rc.setPoolMonitor(_poolMonitor);
        _rc.setPartitionManager(_partitionManager);
        _rc.setThreadPool(new CurrentThreadExceutorHelper(_cell));
        _rc.setCellEndpoint(_cell);
        _rc.ac_rc_set_retry_$_1(new Args("0"));
        _rc.setStageConfigurationFile(null);
        _rc.setPnfsHandler(_pnfsHandler);
        __messages = new ArrayList<CellMessage>();
    }

    @Test
    public void testRestoreNoLocations() throws Exception {

        PnfsId pnfsId = new PnfsId("000000000000000000000000000000000001");


        /*
         * pre-configure pool selection unit
         */
        List<String> pools = new ArrayList<String>(3);
        pools.add("pool1");
        pools.add("pool2");
        PoolMonitorHelper.prepareSelectionUnit(_selectionUnit, pools);

        /*
         * prepare reply for GetStorageInfo
         */

        _storageInfo.addLocation(new URI("osm://osm?"));
        _storageInfo.setFileSize(5);
        _storageInfo.setIsNew(false);

        PnfsGetFileAttributes fileAttributesMessage =
            new PnfsGetFileAttributes(pnfsId, EnumSet.noneOf(FileAttribute.class));
        FileAttributes attributes = new FileAttributes();
        attributes.setStorageInfo(_storageInfo);
        attributes.setPnfsId(pnfsId);
        attributes.setLocations(Collections.<String>emptyList());
        fileAttributesMessage.setFileAttributes(attributes);
        GenericMockCellHelper.prepareMessage(new CellPath("PnfsManager"),
                                             fileAttributesMessage, true);


        /*
         * make pools know to 'PoolManager'
         */

        long serialId = System.currentTimeMillis();
        PoolV2Mode poolMode = new PoolV2Mode(PoolV2Mode.ENABLED);
        Set<String> connectedHSM = new HashSet<String>(1);
        connectedHSM.add("osm");

        for( String pool : pools) {

            PoolCostInfo poolCostInfo = new PoolCostInfo(pool);
            poolCostInfo.setSpaceUsage(100, 20, 30, 50);
            poolCostInfo.setQueueSizes(0, 10, 0, 0, 10, 0, 0, 10, 0);

            PoolManagerPoolUpMessage poolUpMessage = new PoolManagerPoolUpMessage(pool, serialId, poolMode, poolCostInfo);

            _selectionUnit.getPool(pool).setHsmInstances(connectedHSM);

            _costModule.messageArrived(poolUpMessage);

        }


        final AtomicInteger stageRequests = new AtomicInteger(0);

        MessageAction messageAction = new StageMessageAction(stageRequests);

        GenericMockCellHelper.registerAction("pool1", PoolFetchFileMessage.class,messageAction );
        GenericMockCellHelper.registerAction("pool2", PoolFetchFileMessage.class,messageAction );

        PoolMgrSelectReadPoolMsg selectReadPool = new PoolMgrSelectReadPoolMsg(attributes, _protocolInfo, _storageInfo.getFileSize(), null);
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
        List<String> pools = new ArrayList<String>(3);
        pools.add("pool1");
        pools.add("pool2");
        PoolMonitorHelper.prepareSelectionUnit(_selectionUnit, pools);

        /*
         * prepare reply for GetStorageInfo
         */

        _storageInfo.addLocation(new URI("osm://osm?"));
        _storageInfo.setFileSize(5);
        _storageInfo.setIsNew(false);

        PnfsGetFileAttributes fileAttributesMessage =
            new PnfsGetFileAttributes(pnfsId, EnumSet.noneOf(FileAttribute.class));
        FileAttributes attributes = new FileAttributes();
        attributes.setStorageInfo(_storageInfo);
        attributes.setPnfsId(pnfsId);
        attributes.setLocations(Collections.<String>emptyList());
        fileAttributesMessage.setFileAttributes(attributes);
        GenericMockCellHelper.prepareMessage(new CellPath("PnfsManager"), fileAttributesMessage, true);



        /*
         * make pools know to 'PoolManager'
         */

        long serialId = System.currentTimeMillis();
        PoolV2Mode poolMode = new PoolV2Mode(PoolV2Mode.ENABLED);
        Set<String> connectedHSM = new HashSet<String>(1);
        connectedHSM.add("osm");

        for( String pool : pools) {

            PoolCostInfo poolCostInfo = new PoolCostInfo(pool);
            poolCostInfo.setSpaceUsage(100, 20, 30, 50);
            poolCostInfo.setQueueSizes(0, 10, 0, 0, 10, 0, 0, 10, 0);

            PoolManagerPoolUpMessage poolUpMessage = new PoolManagerPoolUpMessage(pool, serialId, poolMode, poolCostInfo);

            _selectionUnit.getPool(pool).setHsmInstances(connectedHSM);

            _costModule.messageArrived(poolUpMessage);

        }


        final AtomicInteger stageRequests1 = new AtomicInteger(0);
        final AtomicInteger stageRequests2 = new AtomicInteger(0);

        MessageAction messageAction1 = new StageMessageAction(stageRequests1);
        MessageAction messageAction2 = new StageMessageAction(stageRequests2);
        GenericMockCellHelper.registerAction("pool1", PoolFetchFileMessage.class,messageAction1 );
        GenericMockCellHelper.registerAction("pool2", PoolFetchFileMessage.class,messageAction2 );

        PoolMgrSelectReadPoolMsg selectReadPool = new PoolMgrSelectReadPoolMsg(attributes, _protocolInfo, _storageInfo.getFileSize(), null);
        CellMessage cellMessage = new CellMessage( new CellPath("PoolManager"), selectReadPool);

        _rc.messageArrived(cellMessage, selectReadPool);

        // first pool replays an  error
        CellMessage m = __messages.get(0);
        PoolFetchFileMessage ff = (PoolFetchFileMessage)m.getMessageObject();
        ff.setFailed(17, "pech");
        _rc.messageArrived(m, m.getMessageObject());


        assertEquals("No stage request sent to pools1", 1, stageRequests1.get());
        assertEquals("No stage request sent to pools2", 1, stageRequests2.get());

    }


    @Test
    public void testRestoreNoLocationsSinglePool() throws Exception {

        PnfsId pnfsId = new PnfsId("000000000000000000000000000000000001");


        /*
         * pre-configure pool selection unit
         */
        List<String> pools = new ArrayList<String>(3);
        pools.add("pool1");
        PoolMonitorHelper.prepareSelectionUnit(_selectionUnit, pools);

        /*
         * prepare reply for GetStorageInfo
         */

        _storageInfo.addLocation(new URI("osm://osm?"));
        _storageInfo.setFileSize(5);
        _storageInfo.setIsNew(false);

        PnfsGetFileAttributes fileAttributesMessage =
            new PnfsGetFileAttributes(pnfsId, EnumSet.noneOf(FileAttribute.class));
        FileAttributes attributes = new FileAttributes();
        attributes.setStorageInfo(_storageInfo);
        attributes.setPnfsId(pnfsId);
        attributes.setLocations(Collections.<String>emptyList());
        fileAttributesMessage.setFileAttributes(attributes);
        GenericMockCellHelper.prepareMessage(new CellPath("PnfsManager"),
                                             fileAttributesMessage, true);



        /*
         * make pools know to 'PoolManager'
         */

        long serialId = System.currentTimeMillis();
        PoolV2Mode poolMode = new PoolV2Mode(PoolV2Mode.ENABLED);
        Set<String> connectedHSM = new HashSet<String>(1);
        connectedHSM.add("osm");

        for( String pool : pools) {

            PoolCostInfo poolCostInfo = new PoolCostInfo(pool);
            poolCostInfo.setSpaceUsage(100, 20, 30, 50);
            poolCostInfo.setQueueSizes(0, 10, 0, 0, 10, 0, 0, 10, 0);

            PoolManagerPoolUpMessage poolUpMessage = new PoolManagerPoolUpMessage(pool, serialId, poolMode, poolCostInfo);

            _selectionUnit.getPool(pool).setHsmInstances(connectedHSM);

            _costModule.messageArrived(poolUpMessage);

        }


        final AtomicInteger stageRequests1 = new AtomicInteger(0);

        MessageAction messageAction1 = new StageMessageAction(stageRequests1);
        GenericMockCellHelper.registerAction("pool1", PoolFetchFileMessage.class,messageAction1 );

        PoolMgrSelectReadPoolMsg selectReadPool = new PoolMgrSelectReadPoolMsg(attributes, _protocolInfo, _storageInfo.getFileSize(), null);
        CellMessage cellMessage = new CellMessage( new CellPath("PoolManager"), selectReadPool);

        _rc.messageArrived(cellMessage, selectReadPool);

        // first pool replays an  error
        CellMessage m = __messages.get(0);
        PoolFetchFileMessage ff = (PoolFetchFileMessage)m.getMessageObject();
        ff.setFailed(17, "pech");
        _rc.messageArrived(m, m.getMessageObject());


        assertEquals("Single Pool excluded on second shot", 2, stageRequests1.get());


    }


    @Test
    public void testRestoreNoLocationsAllPoolsCantStage() throws Exception {

        PnfsId pnfsId = new PnfsId("000000000000000000000000000000000001");

        /*
         * pre-configure pool selection unit
         */
        List<String> pools = new ArrayList<String>(3);
        pools.add("pool1");
        pools.add("pool2");
        PoolMonitorHelper.prepareSelectionUnit(_selectionUnit, pools);

        /*
         * prepare reply for GetStorageInfo
         */

        _storageInfo.addLocation(new URI("osm://osm?"));
        _storageInfo.setFileSize(5);
        _storageInfo.setIsNew(false);

        PnfsGetFileAttributes fileAttributesMessage =
            new PnfsGetFileAttributes(pnfsId, EnumSet.noneOf(FileAttribute.class));
        FileAttributes attributes = new FileAttributes();
        attributes.setStorageInfo(_storageInfo);
        attributes.setPnfsId(pnfsId);
        attributes.setLocations(Collections.<String>emptyList());
        fileAttributesMessage.setFileAttributes(attributes);
        GenericMockCellHelper.prepareMessage(new CellPath("PnfsManager"),
                                             fileAttributesMessage, true);



        /*
         * make pools know to 'PoolManager'
         */

        long serialId = System.currentTimeMillis();
        PoolV2Mode poolMode = new PoolV2Mode(PoolV2Mode.ENABLED);
        Set<String> connectedHSM = new HashSet<String>(1);
        connectedHSM.add("osm");

        for( String pool : pools) {

            PoolCostInfo poolCostInfo = new PoolCostInfo(pool);
            poolCostInfo.setSpaceUsage(100, 20, 30, 50);
            poolCostInfo.setQueueSizes(0, 10, 0, 0, 10, 0, 0, 10, 0);

            PoolManagerPoolUpMessage poolUpMessage = new PoolManagerPoolUpMessage(pool, serialId, poolMode, poolCostInfo);

            _selectionUnit.getPool(pool).setHsmInstances(connectedHSM);

            _costModule.messageArrived(poolUpMessage);

        }


        final AtomicInteger stageRequests1 = new AtomicInteger(0);
        final AtomicInteger stageRequests2 = new AtomicInteger(0);

        MessageAction messageAction1 = new StageMessageAction(stageRequests1);
        MessageAction messageAction2 = new StageMessageAction(stageRequests2);
        GenericMockCellHelper.registerAction("pool1", PoolFetchFileMessage.class,messageAction1 );
        GenericMockCellHelper.registerAction("pool2", PoolFetchFileMessage.class,messageAction2 );

        PoolMgrSelectReadPoolMsg selectReadPool = new PoolMgrSelectReadPoolMsg(attributes, _protocolInfo, _storageInfo.getFileSize(), null);
        CellMessage cellMessage = new CellMessage( new CellPath("PoolManager"), selectReadPool);

        _rc.messageArrived(cellMessage, selectReadPool);

        // first pool replays an error
        CellMessage m = __messages.remove(0);
        PoolFetchFileMessage ff = (PoolFetchFileMessage)m.getMessageObject();

        ff.setFailed(17, "pech");
        _rc.messageArrived(m, m.getMessageObject());

        // second pool replays an error
        m = __messages.remove(0);
        ff = (PoolFetchFileMessage)m.getMessageObject();
        ff.setFailed(17, "pech");
        _rc.messageArrived(m, m.getMessageObject());

        /*
         * request container retry timeout
         */
        Thread.sleep(50);

        assertEquals("Three stage requests where expected", 3,
                     stageRequests1.get() + stageRequests2.get());
        assertTrue("No stage requests sent to pool1",
                   stageRequests1.get() != 0);
        assertTrue("No stage requests sent to pool2",
                   stageRequests2.get() != 0);
    }


    @After
    public void clear() {
        GenericMockCellHelper.clean();
    }

    private class StageMessageAction implements MessageAction {

        private final AtomicInteger _count;

        StageMessageAction(AtomicInteger ai) {
            _count = ai;
        }

        public void messageArraved(CellMessage message) {
            _count.incrementAndGet();
            __messages.add(message);
        }
    }

}
