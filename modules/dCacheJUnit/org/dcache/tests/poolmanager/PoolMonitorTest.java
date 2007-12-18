package org.dcache.tests.poolmanager;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;

import org.dcache.tests.cells.GenericMocCellHelper;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.poolManager.CostModuleV1;
import diskCacheV111.poolManager.PartitionManager;
import diskCacheV111.poolManager.PoolMonitorV5;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnitV2;
import diskCacheV111.poolManager.PoolMonitorV5.PnfsFileLocation;
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


    private static GenericMocCellHelper _cell = new GenericMocCellHelper("PoolMonitorTestCell", "");

    private PoolMonitorV5 _poolMonitor;
    private CostModule _costModule ;
    private PoolSelectionUnit _selectionUnit;
    private PartitionManager _partitionManager = new PartitionManager(_cell);
    private PnfsHandler      _pnfsHandler;

    private final ProtocolInfo _protocolInfo = new DCapProtocolInfo("DCap", 3, 0, "127.0.0.1", 17);
    private final StorageInfo _storageInfo = new OSMStorageInfo("h1", "rawd");
    @Before
    public void setUp() throws Exception {


        _selectionUnit = new PoolSelectionUnitV2();
        _costModule = new CostModuleV1(_cell);
        _pnfsHandler = new PnfsHandler(_cell, new CellPath("PnfsManager"));
        _poolMonitor = new PoolMonitorV5( _cell ,
                _selectionUnit ,
                _pnfsHandler ,
                _costModule ,
                _partitionManager );


    }


    @Test
    public void testPartialFromStore() throws Exception {

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

        GenericMocCellHelper.prepareMessage(new CellPath("PnfsManager"), message);


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

        CellMessage cellMessage1 = new CellMessage( new CellPath("CostModule"), pool1UpMessage);
        CellMessage cellMessage2 = new CellMessage( new CellPath("CostModule"), pool2UpMessage);
        _costModule.messageArrived(cellMessage1);
        _costModule.messageArrived(cellMessage2);

        /*
         * one pool have the file
         */
        PoolCheckFileMessage pool1CeckMessage = new PoolCheckFileMessage("pool1",pnfsId );
        pool1CeckMessage.setHave(true);

        /*
         * the other, receiving it
         */
        PoolCheckFileMessage pool2CeckMessage = new PoolCheckFileMessage("pool2",pnfsId );
        pool2CeckMessage.setHave(false);
        pool2CeckMessage.setWaiting(true);


        /*
         * prepare pools reply
         */
        GenericMocCellHelper.prepareMessage(new CellPath("pool1"), pool1CeckMessage);
        GenericMocCellHelper.prepareMessage(new CellPath("pool2"), pool2CeckMessage);



        /*
         * exercise
         */
        PnfsFileLocation availableLocations = _poolMonitor.getPnfsFileLocation(pnfsId, _storageInfo, _protocolInfo, null);

        List<PoolCostCheckable> acknowledgedPools =  availableLocations.getAcknowledgedPnfsPools();

        assertFalse("No pools found (test setup error)", acknowledgedPools.isEmpty());
        assertTrue("No pools acknowledged (test setup error)", availableLocations.getAvailablePoolCount() > 0 );

        for( PoolCostCheckable pool : acknowledgedPools) {
            assertFalse("Pool with the file in the 'waiting' state should not be used in selection", pool.getPoolName().equals("pool2") );
        }

        boolean found = false;
        for( PoolCostCheckable pool : acknowledgedPools) {
            if( pool.getPoolName().equals("pool1") ) {
                found = true;
                break;
            }
        }

        assertFalse("Pool with the file not be used by selection", found );
    }

}
