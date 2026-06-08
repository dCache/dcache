package org.dcache.tests.poolmanager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.gson.GsonBuilder;
import diskCacheV111.poolManager.CostModuleV1;
import diskCacheV111.poolManager.PoolMonitorV5;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnitAccess;
import diskCacheV111.poolManager.PoolSelectionUnitV2;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.OSMStorageInfo;
import diskCacheV111.vehicles.PoolManagerPoolUpMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.StorageInfos;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellMessage;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.dcache.cells.UniversalSpringCell;
import org.dcache.pool.classic.IoQueueManager;
import org.dcache.poolmanager.PartitionManager;
import org.dcache.poolmanager.PoolSelector;
import org.dcache.vehicles.FileAttributes;
import org.junit.Before;
import org.junit.Test;

public class PoolMonitorTest {

    private PoolMonitorV5 _poolMonitor;
    private CostModuleV1 _costModule;
    private PoolSelectionUnit _selectionUnit;
    private PoolSelectionUnitAccess _access;
    private PartitionManager _partitionManager = new PartitionManager();

    private final ProtocolInfo _protocolInfo = new DCapProtocolInfo("DCap", 3, 0,
          new InetSocketAddress("127.0.0.1", 17));
    private final StorageInfo _storageInfo = new OSMStorageInfo("h1", "rawd");
    private PnfsId _pnfsId;
    private List<String> _pools;
    private String _localhost;

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
        _pnfsId = new PnfsId("000000000000000000000000000000000001");
        _pools = Arrays.asList("pool1", "pool2");
        _localhost = InetAddress.getLocalHost().getCanonicalHostName();
    }

    @Test
    public void testFromStore() throws Exception {
        prepareCostModule(false);

        /*
         * exercise
         */
        FileAttributes attributes = FileAttributes.of().pnfsId(_pnfsId).locations(_pools).build();
        StorageInfos.injectInto(_storageInfo, attributes);
        PoolSelector availableLocations =
              _poolMonitor.getPoolSelector(attributes,
                    _protocolInfo,
                    null,
                    Collections.EMPTY_SET);

        /* The following isn't testing much as both pools are valid
         * replies.
         */
        assertTrue(_pools.contains(availableLocations.selectReadPool().name()));
        assertTrue(_pools.contains(availableLocations.selectPinPool().name()));
    }

    @Test
    public void testLinkFallbackEnable() throws Exception {
        _poolMonitor.setEnableLinkFallback(true);
        prepareCostModule(true);

        /*
         * exercise
         */
        Collection<String> location = Collections.singleton("pool1");
        FileAttributes attributes = FileAttributes.of().pnfsId(_pnfsId).locations(location).build();
        StorageInfos.injectInto(_storageInfo, attributes);
        PoolSelector availableLocations
              = _poolMonitor.getPoolSelector(attributes,
              _protocolInfo,
              null,
              Collections.EMPTY_SET);

        assertEquals("pool1", availableLocations.selectReadPool().name());
    }

    @Test(expected = PermissionDeniedCacheException.class)
    public void testLinkFallbackDisabled() throws Exception {

        _poolMonitor.setEnableLinkFallback(false);
        prepareCostModule(true);

        /*
         * exercise
         */
        Collection<String> location = Collections.singleton("pool1");
        FileAttributes attributes = FileAttributes.of().pnfsId(_pnfsId).locations(location).build();
        StorageInfos.injectInto(_storageInfo, attributes);
        PoolSelector availableLocations
              = _poolMonitor.getPoolSelector(attributes,
              _protocolInfo,
              null,
              Collections.EMPTY_SET);

        availableLocations.selectReadPool();
    }

    @Test(expected = CacheException.class)
    public void testHostFilterForRead() throws Exception {
        prepareCostModule(false);
        prepareHostExclusion().selectReadPool();
    }

    @Test(expected = CacheException.class)
    public void testHostFilterForWrite() throws Exception {
        prepareCostModule(false);
        prepareHostExclusion().selectWritePool(0);
    }

    @Test
    public void testGsonDeserialization() throws Exception {
        prepareCostModule(false);
        Object obj = UniversalSpringCell.serialize(_poolMonitor);
        new GsonBuilder().serializeSpecialFloatingPointValues().setPrettyPrinting()
              .disableHtmlEscaping().create().toJson(obj);
    }

    @Test
    public void testWritePoolZonePreference() throws Exception {
        prepareCostModule(false, true);

        FileAttributes attributes = FileAttributes.of().pnfsId(_pnfsId).build();
        StorageInfos.injectInto(_storageInfo, attributes);

        PoolSelector selector = _poolMonitor.getPoolSelector(attributes, _protocolInfo,
                null, Optional.of("1"), Collections.EMPTY_SET);

        assertEquals("pool1", selector.selectWritePool(0).name());
    }

    @Test
    public void testReadPoolZonePreference() throws Exception {
        prepareCostModule(false, true);

        FileAttributes attributes = FileAttributes.of().pnfsId(_pnfsId).locations(_pools).build();
        StorageInfos.injectInto(_storageInfo, attributes);

        PoolSelector selector = _poolMonitor.getPoolSelector(attributes, _protocolInfo,
                null, Optional.of("1"), Collections.EMPTY_SET);

        assertEquals("pool1", selector.selectReadPool().name());
    }

    @Test
    public void testReadPoolZoneFallback() throws Exception {
        prepareCostModule(false, true);

        // Only pool2 in zone 2 has the file
        FileAttributes attributes = FileAttributes.of()
                .pnfsId(_pnfsId)
                .locations(Collections.singleton("pool2"))
                .build();
        StorageInfos.injectInto(_storageInfo, attributes);

        PoolSelector selector = _poolMonitor.getPoolSelector(attributes, _protocolInfo,
                null, Optional.of("1"), Collections.EMPTY_SET);

        assertEquals("pool2", selector.selectReadPool().name());
    }

    @Test
    public void testWritePoolZoneFallback() throws Exception {
        // pool2 is offline zone 2 requested so falls back to pool1
        prepareCostModule(false, true, true, false);

        FileAttributes attributes = FileAttributes.of().pnfsId(_pnfsId).build();
        StorageInfos.injectInto(_storageInfo, attributes);

        PoolSelector selector = _poolMonitor.getPoolSelector(attributes, _protocolInfo,
                null, Optional.of("2"), Collections.EMPTY_SET);

        assertEquals("pool1", selector.selectWritePool(0).name());
    }

    @Test
    public void testWritePoolFallBackAllPoolsInZoneFull() throws Exception {
        _partitionManager.setProperties(null, Map.of("fallback-onspace", "yes"));
        prepareCostModule(false, true, false, true);

        FileAttributes attributes = FileAttributes.of().pnfsId(_pnfsId).build();
        StorageInfos.injectInto(_storageInfo, attributes);

        PoolSelector selector = _poolMonitor.getPoolSelector(attributes, _protocolInfo,
                null, Optional.of("1"), Collections.EMPTY_SET);

        assertEquals("pool2", selector.selectWritePool(0).name());
    }
    @Test
    public void testStagePoolZonePreference() throws Exception {
        _partitionManager.setProperties(null, Map.of("fallback-onspace", "yes"));
        prepareCostModule(false, true, false, false);

        FileAttributes attributes = FileAttributes.of()
                .pnfsId(_pnfsId)
                .locations(Collections.emptyList())
                .build();
        StorageInfos.injectInto(_storageInfo, attributes);

        PoolSelector selector = _poolMonitor.getPoolSelector(attributes, _protocolInfo,
                null, Optional.of("1"), Collections.EMPTY_SET);

        assertEquals("pool1", selector.selectStagePool(Optional.empty()).name());
    }

    @Test
    public void testStagePoolZoneFallback() throws Exception {
        _partitionManager.setProperties(null, Map.of("fallback-onspace", "yes"));
        prepareCostModule(false, true, false, true);

        FileAttributes attributes = FileAttributes.of()
                .pnfsId(_pnfsId)
                .locations(Collections.emptyList())
                .build();
        StorageInfos.injectInto(_storageInfo, attributes);

        PoolSelector selector = _poolMonitor.getPoolSelector(attributes, _protocolInfo,
                null, Optional.of("1"), Collections.EMPTY_SET);

        assertEquals("pool2", selector.selectStagePool(Optional.empty()).name());
    }

    private void prepareCostModule(boolean linkPerPool) throws Exception {
        prepareCostModule(linkPerPool, false);
    }

    private void prepareCostModule(boolean linkPerPool, boolean withZones) throws Exception {
        prepareCostModule(linkPerPool, withZones, false, false);
    }

    private void prepareCostModule(boolean linkPerPool, boolean withZones, boolean pool2Offline, boolean pool1Full)
            throws Exception {
        if (linkPerPool) {
            PoolMonitorHelper.prepareLinkPerPool(_selectionUnit, _access, _pools);
        } else {
            PoolMonitorHelper.prepareSelectionUnit(_selectionUnit, _access, _pools);
        }

        long serialId = System.currentTimeMillis();

        /*
         * make pools know to 'PoolManager'
         */
        PoolV2Mode poolMode = new PoolV2Mode(PoolV2Mode.ENABLED);

        PoolCostInfo poolCost1 = new PoolCostInfo("pool1", IoQueueManager.DEFAULT_QUEUE);
        if(pool1Full){
            poolCost1.setSpaceUsage(100, 0, 100, 0);
        } else {
            poolCost1.setSpaceUsage(100, 20, 30, 50);
        }

        PoolManagerPoolUpMessage pool1UpMessage = new PoolManagerPoolUpMessage("pool1",
                serialId, poolMode, poolCost1);
        pool1UpMessage.setHostName(_localhost);

        if (withZones) {
            pool1UpMessage.setTagMap(Map.of("zone", "1"));
        }

        CellMessage envelope1 = new CellMessage(new CellAddressCore("PoolManager"), null);
        envelope1.addSourceAddress(new CellAddressCore("pool1"));
        _costModule.messageArrived(envelope1, pool1UpMessage);

        _selectionUnit.getPool("pool1").setHsmInstances(Set.of("osm"));

        if (!pool2Offline) {
            PoolCostInfo poolCost2 = new PoolCostInfo("pool2", IoQueueManager.DEFAULT_QUEUE);
            poolCost2.setSpaceUsage(100, 20, 30, 50);
            PoolManagerPoolUpMessage pool2UpMessage = new PoolManagerPoolUpMessage("pool2",
                    serialId, poolMode, poolCost2);
            pool2UpMessage.setHostName(_localhost);
            if (withZones) {
                pool2UpMessage.setTagMap(Map.of("zone", "2"));
            }
            CellMessage envelope2 = new CellMessage(new CellAddressCore("PoolManager"), null);
            envelope2.addSourceAddress(new CellAddressCore("pool2"));
            _costModule.messageArrived(envelope2, pool2UpMessage);
            _selectionUnit.getPool("pool2").setHsmInstances(Set.of("osm"));
        }
    }


    private PoolSelector prepareHostExclusion() throws Exception {
        /*
         * Emulate the poolup message arrival on the PoolManager
         * which updates the psu.
         */
        _selectionUnit.getPool("pool1").setCanonicalHostName(_localhost);
        _selectionUnit.getPool("pool2").setCanonicalHostName(_localhost);

        /*
         * Exclude _localhost pools.  Should throw exception.
         */
        FileAttributes attributes = FileAttributes.of().pnfsId(_pnfsId).locations(_pools).build();
        StorageInfos.injectInto(_storageInfo, attributes);

        Set<String> excluded = new HashSet<>();
        excluded.add(InetAddress.getLocalHost().getCanonicalHostName());

        return _poolMonitor.getPoolSelector(attributes, _protocolInfo, null, excluded);
    }
}
