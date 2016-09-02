package org.dcache.tests.poolmanager;

import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

import diskCacheV111.poolManager.CostModuleV1;
import diskCacheV111.poolManager.PoolMonitorV5;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnitAccess;
import diskCacheV111.poolManager.PoolSelectionUnitV2;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.OSMStorageInfo;
import diskCacheV111.vehicles.PoolManagerPoolUpMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.StorageInfos;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;

import org.dcache.pool.classic.IoQueueManager;
import org.dcache.poolmanager.PartitionManager;
import org.dcache.poolmanager.PoolSelector;
import org.dcache.vehicles.FileAttributes;

import static org.junit.Assert.assertTrue;

public class PoolMonitorTest
{
    private PoolMonitorV5 _poolMonitor;
    private CostModuleV1 _costModule ;
    private PoolSelectionUnit _selectionUnit;
    private PoolSelectionUnitAccess _access;
    private PartitionManager _partitionManager = new PartitionManager();

    private final ProtocolInfo _protocolInfo = new DCapProtocolInfo("DCap", 3, 0,
            new InetSocketAddress("127.0.0.1", 17));
    private final StorageInfo _storageInfo = new OSMStorageInfo("h1", "rawd");
    @Before
    public void setUp() throws Exception {
        PoolSelectionUnitV2 psu = new PoolSelectionUnitV2();
        _access = psu;
        _selectionUnit = psu;
        _costModule = new CostModuleV1();
        _poolMonitor = new PoolMonitorV5();
        _poolMonitor.setPoolSelectionUnit(_selectionUnit);
        _poolMonitor.setCostModule(_costModule);
        _poolMonitor.setPartitionManager(_partitionManager);
    }


    @Test
    public void testFromStore() throws Exception {

        PnfsId pnfsId = new PnfsId("000000000000000000000000000000000001");

        List<String> pools = Arrays.asList("pool1", "pool2");

        /*
         * pre-configure pool selection unit
         */
        PoolMonitorHelper.prepareSelectionUnit(_selectionUnit, _access, pools);


        long serialId = System.currentTimeMillis();

        /*
         * make pools know to 'PoolManager'
         */
        PoolV2Mode poolMode = new PoolV2Mode(PoolV2Mode.ENABLED);

        PoolCostInfo poolCost1 = new PoolCostInfo("pool1", IoQueueManager.DEFAULT_QUEUE);
        PoolCostInfo poolCost2 = new PoolCostInfo("pool2", IoQueueManager.DEFAULT_QUEUE);

        poolCost1.setSpaceUsage(100, 20, 30, 50);
        poolCost2.setSpaceUsage(100, 20, 30, 50);

        PoolManagerPoolUpMessage pool1UpMessage = new PoolManagerPoolUpMessage("pool1",
                serialId, poolMode, poolCost1);

        PoolManagerPoolUpMessage pool2UpMessage = new PoolManagerPoolUpMessage("pool2",
                serialId, poolMode, poolCost2);

        CellMessage envelope1 = new CellMessage(new CellAddressCore("PoolManager"), null);
        envelope1.addSourceAddress(new CellAddressCore("pool1"));
        CellMessage envelope2 = new CellMessage(new CellAddressCore("PoolManager"), null);
        envelope2.addSourceAddress(new CellAddressCore("pool2"));

        _costModule.messageArrived(envelope1, pool1UpMessage);
        _costModule.messageArrived(envelope2, pool2UpMessage);

        /*
         * exercise
         */
        FileAttributes attributes = FileAttributes.of().pnfsId(pnfsId).locations(pools).build();
        StorageInfos.injectInto(_storageInfo, attributes);
        PoolSelector availableLocations =
            _poolMonitor.getPoolSelector(attributes, _protocolInfo, null);

        /* The following isn't testing much as both pools are valid
         * replies.
         */
        assertTrue(pools.contains(availableLocations.selectReadPool().getName()));
        assertTrue(pools.contains(availableLocations.selectPinPool().getName()));
    }
}
