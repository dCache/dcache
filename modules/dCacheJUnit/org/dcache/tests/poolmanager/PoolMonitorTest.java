package org.dcache.tests.poolmanager;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;

import org.dcache.poolmanager.PartitionManager;
import diskCacheV111.poolManager.CostModuleV1;
import diskCacheV111.poolManager.PoolMonitorV5;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnitV2;
import diskCacheV111.poolManager.PoolMonitorV5.PnfsFileLocation;
import diskCacheV111.pools.CostCalculationEngine;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.OSMStorageInfo;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import diskCacheV111.vehicles.PoolCheckFileMessage;
import diskCacheV111.vehicles.PoolCostCheckable;
import diskCacheV111.vehicles.PoolManagerPoolUpMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import org.dcache.vehicles.FileAttributes;

public class PoolMonitorTest
{
    private PoolMonitorV5 _poolMonitor;
    private CostModuleV1 _costModule ;
    private PoolSelectionUnit _selectionUnit;
    private PartitionManager _partitionManager = new PartitionManager();

    private final ProtocolInfo _protocolInfo = new DCapProtocolInfo("DCap", 3, 0, "127.0.0.1", 17);
    private final StorageInfo _storageInfo = new OSMStorageInfo("h1", "rawd");
    @Before
    public void setUp() throws Exception {


        _selectionUnit = new PoolSelectionUnitV2();
        _costModule = new CostModuleV1();
        _poolMonitor = new PoolMonitorV5();
        _poolMonitor.setPoolSelectionUnit(_selectionUnit);
        _poolMonitor.setCostModule(_costModule);
        _poolMonitor.setPartitionManager(_partitionManager);
        _costModule.setCostCalculationEngine(new CostCalculationEngine("diskCacheV111.pools.CostCalculationV5"));
    }


    @Test
    public void testFromStore() throws Exception {

        PnfsId pnfsId = new PnfsId("000000000000000000000000000000000001");

        List<String> pools = Arrays.asList("pool1", "pool2");

        /*
         * pre-configure pool selection unit
         */
        PoolMonitorHelper.prepareSelectionUnit(_selectionUnit, pools);


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
        FileAttributes attributes = new FileAttributes();
        attributes.setStorageInfo(_storageInfo);
        attributes.setPnfsId(pnfsId);
        attributes.setLocations(pools);
        PnfsFileLocation availableLocations =
            _poolMonitor.getPnfsFileLocation(attributes, _protocolInfo, null);

        /* The following isn't testing much as both pools are valid
         * replies.
         */
        assertTrue(pools.contains(availableLocations.selectReadPool().getName()));
        assertTrue(pools.contains(availableLocations.selectPinPool().getName()));
    }
}
