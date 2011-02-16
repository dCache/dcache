package org.dcache.tests.poolmanager;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;

import org.dcache.tests.cells.GenericMockCellHelper;

import diskCacheV111.poolManager.CostModuleV1;
import diskCacheV111.poolManager.PartitionManager;
import diskCacheV111.poolManager.PoolMonitorV5;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnitV2;
import diskCacheV111.poolManager.PoolMonitorV5.PnfsFileLocation;
import diskCacheV111.pools.CostCalculationEngine;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.OSMStorageInfo;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import diskCacheV111.vehicles.PoolCheckFileMessage;
import diskCacheV111.vehicles.PoolCostCheckable;
import diskCacheV111.vehicles.PoolManagerPoolUpMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;

public class PoolMonitorTest {


    private static GenericMockCellHelper _cell = new GenericMockCellHelper("PoolMonitorTestCell", "");

    private PoolMonitorV5 _poolMonitor;
    private CostModuleV1 _costModule ;
    private PoolSelectionUnit _selectionUnit;
    private PartitionManager _partitionManager = new PartitionManager();
    private PnfsHandler      _pnfsHandler;

    private final ProtocolInfo _protocolInfo = new DCapProtocolInfo("DCap", 3, 0, "127.0.0.1", 17);
    private final StorageInfo _storageInfo = new OSMStorageInfo("h1", "rawd");
    @Before
    public void setUp() throws Exception {


        _selectionUnit = new PoolSelectionUnitV2();
        _costModule = new CostModuleV1();
        _pnfsHandler = new PnfsHandler(new CellPath("PnfsManager"));
        _pnfsHandler.setCellEndpoint(_cell);
        _poolMonitor = new PoolMonitorV5();
        _poolMonitor.setPoolSelectionUnit(_selectionUnit);
        _poolMonitor.setPnfsHandler(_pnfsHandler);
        _poolMonitor.setCostModule(_costModule);
        _poolMonitor.setPartitionManager(_partitionManager);
        _costModule.setCostCalculationEngine(new CostCalculationEngine("diskCacheV111.pools.CostCalculationV5"));
    }


    @Test
    public void testFromStore() throws Exception {

        PnfsId pnfsId = new PnfsId("000000000000000000000000000000000001");

        List<String> pools = new ArrayList<String>();

        pools.add("pool1");
        pools.add("pool2");

        /*
         * pre-configure pool selection unit
         */
        PoolMonitorHelper.prepareSelectionUnit(_selectionUnit, pools);

        /*
         * prepare reply for getCacheLocation request
         */
        PnfsGetCacheLocationsMessage message = PoolMonitorHelper.prepareGetCacheLocation(pnfsId, pools);

        GenericMockCellHelper.prepareMessage(new CellPath("PnfsManager"), message);


        long serialId = System.currentTimeMillis();

        /*
         * make pools know to 'PoolManager'
         */
        PoolV2Mode poolMode = new PoolV2Mode(PoolV2Mode.ENABLED);

        PoolCostInfo poolCost1 = new PoolCostInfo("pool1");
        PoolCostInfo poolCost2 = new PoolCostInfo("pool2");

        poolCost1.setSpaceUsage(100, 20, 30, 50);
        poolCost2.setSpaceUsage(100, 20, 30, 50);

        PoolManagerPoolUpMessage pool1UpMessage = new PoolManagerPoolUpMessage("pool1",
                serialId, poolMode, poolCost1);

        PoolManagerPoolUpMessage pool2UpMessage = new PoolManagerPoolUpMessage("pool2",
                serialId, poolMode, poolCost2);

        _costModule.messageArrived(pool1UpMessage);
        _costModule.messageArrived(pool2UpMessage);

        /*
         * exercise
         */
        PnfsFileLocation availableLocations = _poolMonitor.getPnfsFileLocation(pnfsId, _storageInfo, _protocolInfo, null);

        List<PoolCostCheckable> onlinePools =
            availableLocations.getOnlinePools();

        assertFalse("No pools found (test setup error)",
                    onlinePools.isEmpty());
        assertFalse("No pools acknowledged (test setup error)",
                    availableLocations.getFileAvailableMatrix().isEmpty());

        boolean found1 = false;
        boolean found2 = false;
        for( PoolCostCheckable pool : onlinePools) {
            if( pool.getPoolName().equals("pool1") ) {
                found1 = true;
            }
            if( pool.getPoolName().equals("pool2") ) {
                found2 = true;
            }
        }

        assertTrue("Pool with the file not used by selection", found1 );
        assertTrue("Pool with the file not used by selection", found2 );
    }

}
