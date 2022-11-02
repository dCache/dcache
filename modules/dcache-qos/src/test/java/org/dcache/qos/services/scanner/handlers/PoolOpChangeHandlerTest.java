package org.dcache.qos.services.scanner.handlers;

import static diskCacheV111.pools.PoolV2Mode.DISABLED_STRICT;
import static diskCacheV111.pools.PoolV2Mode.ENABLED;
import static org.dcache.mock.MockVerifyBuilder.using;
import static org.dcache.mock.verifiers.PoolOperationMapVerifier.aPoolOperationMapVerifier;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.poolManager.PoolSelectionUnitV2;
import diskCacheV111.pools.PoolV2Mode;
import java.util.Collection;
import java.util.stream.Stream;
import org.dcache.mock.PoolMonitorFactory;
import org.dcache.poolmanager.SerializablePoolMonitor;
import org.dcache.qos.data.PoolQoSStatus;
import org.dcache.qos.services.scanner.data.PoolOperationMap;
import org.dcache.qos.services.scanner.util.ScannerMapInitializer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.verification.AtMost;

public class PoolOpChangeHandlerTest {
    static final String DEFAULT_POOL_INFO = "org/dcache/mock/poolinfo.json";
    static final String POOL_INFO_WITH_CHANGED_TAGS = "org/dcache/mock/poolinfo-with-tags-changed.json";
    static final String POOL_ON_WHICH_TO_CHANGE_TAGS = "testpool03-3";

    SerializablePoolMonitor currentPoolMonitor;
    SerializablePoolMonitor nextPoolMonitor;
    PoolOpChangeHandler poolOpChangeHandler;
    ScannerMapInitializer initializer;
    PoolOperationMap poolOperationMap;

    String testPool;
    String testUnit;
    String testGroup;

    @Before
    public void setup() throws Exception {
        poolOperationMap = mock(PoolOperationMap.class);
        when(poolOperationMap.isInitialized(any(PoolV2Mode.class))).thenCallRealMethod();
        currentPoolMonitor = PoolMonitorFactory.create(DEFAULT_POOL_INFO);
        initializer = new ScannerMapInitializer();
        initializer.updatePoolMonitor(currentPoolMonitor);
        poolOpChangeHandler = new PoolOpChangeHandler();
        poolOpChangeHandler.setMapInitializer(initializer);
        poolOpChangeHandler.setPoolOperationMap(poolOperationMap);
        poolOpChangeHandler.setPoolMonitor(currentPoolMonitor);
    }

    @Test
    public void shouldNotSetPsuOnMapWhenNothingHasChanged() throws Exception {
        givenAllPoolsWith(poolMode(ENABLED));
        givenNoChangeInStatus();
        whenReloadAndScanIsCalled();
        assertNoUpdatesToPoolOperationMap();
    }

    /* Adding new pools. */

    @Test
    public void shouldTriggerScanWhenPoolsAddedToOperationMap() throws Exception {
        givenTestPool("testpool03-0").isAdded();
        whenReloadAndScanIsCalled();
        using(aPoolOperationMapVerifier()).verifyThat("add").isCalled(1).on(poolOperationMap)
              .with("testpool03-0");
        using(aPoolOperationMapVerifier()).verifyThat("scan").isCalled(1).on(poolOperationMap)
              .with("testpool03-0", noAddedTo(), noRemovedFrom(), noStorageUnit(), anyMode(),
                    ignoringStateCheck());
    }

    @Test
    public void shouldTriggerScanWhenPoolsAddedToPoolGroup() throws Exception {
        givenTestPool("testpool05-1").isAddedTo("tape-group");
        whenReloadAndScanIsCalled();
        using(aPoolOperationMapVerifier()).verifyThat("updateStatus").isCalled(1)
              .on(poolOperationMap).with("testpool05-1", anyMode());
        using(aPoolOperationMapVerifier()).verifyThat("scan").isCalled(1).on(poolOperationMap)
              .with("testpool05-1", "tape-group", noRemovedFrom(), noStorageUnit(), anyMode(),
                    withStateCheck());
    }

    /* Removing pools. */

    @Test
    public void shouldCancelOperationsForRemovedPools() throws Exception {
        givenTestPool("testpool03-1").isRemoved();
        whenReloadAndScanIsCalled();
        using(aPoolOperationMapVerifier()).verifyThat("cancel").isCalled(1).on(poolOperationMap)
              .with(anyArgs());
        using(aPoolOperationMapVerifier()).verifyThat("remove").isCalled(1).on(poolOperationMap)
              .with("testpool03-1");
    }

    @Test
    public void shouldCancelOperationsForPoolsRemovedFromGroup() throws Exception {
        givenTestPool("testpool03-1").isRemovedFrom("highavail-group");
        whenReloadAndScanIsCalled();
        using(aPoolOperationMapVerifier()).verifyThat("cancel").isCalled(1).on(poolOperationMap)
              .with(anyArgs());
    }

    @Test
    public void shouldTriggerScanForPoolsRemovedFromGroup() throws Exception {
        givenTestPool("testpool03-1").isRemovedFrom("highavail-group");
        whenReloadAndScanIsCalled();
        using(aPoolOperationMapVerifier()).verifyThat("scan").isCalled(1).on(poolOperationMap)
              .with("testpool03-1", noAddedTo(), "highavail-group", noStorageUnit(), anyMode(),
                    withStateCheck());
    }

    /* Storage group / link modification. */

    @Test
    public void shouldTriggerScanWhenPoolGroupAcquiresNewStorageUnit() throws Exception {
        givenTestUnit("tape.dcache-devel-test@enstore").isAddedToUnitGroup("internal");
        whenReloadAndScanIsCalled();
        forPoolsIn("internal-group").forEach(pool -> {
            try {
                using(aPoolOperationMapVerifier()).verifyThat("scan").isCalled(1)
                      .on(poolOperationMap)
                      .with(pool.getName(), noAddedTo(), noRemovedFrom(),
                            "tape.dcache-devel-test@enstore", anyMode(),
                            ignoringStateCheck());
            } catch (Exception e) {
                assertNull(e);
            }
        });
        forPoolsIn("stage-group").forEach(pool -> {
            try {
                using(aPoolOperationMapVerifier()).verifyThat("scan").isCalled(1)
                      .on(poolOperationMap)
                      .with(pool.getName(), noAddedTo(), noRemovedFrom(),
                            "tape.dcache-devel-test@enstore", anyMode(),
                            ignoringStateCheck());
            } catch (Exception e) {
                assertNull(e);
            }
        });
        forPoolsIn("tape-group").forEach(pool -> {
            try {
                using(aPoolOperationMapVerifier()).verifyThat("scan").isCalled(1)
                      .on(poolOperationMap)
                      .with(pool.getName(), noAddedTo(), noRemovedFrom(),
                            "tape.dcache-devel-test@enstore", anyMode(),
                            ignoringStateCheck());
            } catch (Exception e) {
                assertNull(e);
            }
        });
    }

    @Test
    public void shouldTriggerScanWhenPoolGroupLosesStorageUnit() throws Exception {
        givenTestUnit("bnltest.dcache-devel-test@enstore").isRemovedFromUnitGroup("internal");
        whenReloadAndScanIsCalled();
        forPoolsIn("dmz-group").forEach(pool -> {
            try {
                using(aPoolOperationMapVerifier()).verifyThat("scan").isCalled(1)
                      .on(poolOperationMap)
                      .with(pool.getName(), noAddedTo(), noRemovedFrom(),
                            "bnltest.dcache-devel-test@enstore", anyMode(),
                            ignoringStateCheck());
            } catch (Exception e) {
                assertNull(e);
            }
        });
    }

    /* Storage unit modification */

    @Test
    public void shouldTriggerScanWhenStorageUnitRequiredModified() throws Exception {
        givenTestUnit("persistent-tape.dcache-devel-test@enstore").isChangedToRequire(4);
        whenReloadAndScanIsCalled();
        using(aPoolOperationMapVerifier()).verifyThat("scan").isCalled(1).on(poolOperationMap)
              .with("testpool03-8", noAddedTo(), noRemovedFrom(),
                    "persistent-tape.dcache-devel-test@enstore", anyMode(),
                    ignoringStateCheck());
        using(aPoolOperationMapVerifier()).verifyThat("scan").isCalled(1).on(poolOperationMap)
              .with("testpool04-8", noAddedTo(), noRemovedFrom(),
                    "persistent-tape.dcache-devel-test@enstore", anyMode(),
                    ignoringStateCheck());
        using(aPoolOperationMapVerifier()).verifyThat("scan").isCalled(1).on(poolOperationMap)
              .with("testpool08-8", noAddedTo(), noRemovedFrom(),
                    "persistent-tape.dcache-devel-test@enstore", anyMode(),
                    ignoringStateCheck());
        using(aPoolOperationMapVerifier()).verifyThat("scan").isCalled(1).on(poolOperationMap)
              .with("testpool09-8", noAddedTo(), noRemovedFrom(),
                    "persistent-tape.dcache-devel-test@enstore", anyMode(),
                    ignoringStateCheck());
    }

    @Test
    public void shouldTriggerScanWhenStorageUnitOnlyOneOfModified() throws Exception {
        givenTestUnit("persistent-tape.dcache-devel-test@enstore").isChangedToPartitionOn(
              "hostname",
              "rack");
        whenReloadAndScanIsCalled();
        using(aPoolOperationMapVerifier()).verifyThat("scan").isCalled(1).on(poolOperationMap)
              .with("testpool03-8", noAddedTo(), noRemovedFrom(),
                    "persistent-tape.dcache-devel-test@enstore", anyMode(),
                    ignoringStateCheck());
        using(aPoolOperationMapVerifier()).verifyThat("scan").isCalled(1).on(poolOperationMap)
              .with("testpool04-8", noAddedTo(), noRemovedFrom(),
                    "persistent-tape.dcache-devel-test@enstore", anyMode(),
                    ignoringStateCheck());
        using(aPoolOperationMapVerifier()).verifyThat("scan").isCalled(1).on(poolOperationMap)
              .with("testpool08-8", noAddedTo(), noRemovedFrom(),
                    "persistent-tape.dcache-devel-test@enstore", anyMode(),
                    ignoringStateCheck());
        using(aPoolOperationMapVerifier()).verifyThat("scan").isCalled(1).on(poolOperationMap)
              .with("testpool09-8", noAddedTo(), noRemovedFrom(),
                    "persistent-tape.dcache-devel-test@enstore", anyMode(),
                    ignoringStateCheck());
    }

    /* Change of pool status. */

    @Test
    public void shouldTriggerUpdateWhenPoolIsUpThenDown() throws Exception {
        givenTestPool("testpool03-4").goesFrom(poolMode(ENABLED)).to(poolMode(DISABLED_STRICT));
        whenReloadAndScanIsCalled();
        using(aPoolOperationMapVerifier()).verifyThat("handlePoolStatusChange").isCalled(1)
              .on(poolOperationMap)
              .with("testpool03-4", statusCorrespondingTo(poolMode(DISABLED_STRICT)));
    }

    @Test
    public void shouldTriggerUpdateScanWhenPoolIsDownThenUp() throws Exception {
        givenTestPool("testpool03-4").goesFrom(poolMode(DISABLED_STRICT)).to(poolMode(ENABLED));
        whenReloadAndScanIsCalled();
        using(aPoolOperationMapVerifier()).verifyThat("handlePoolStatusChange").isCalled(1)
              .on(poolOperationMap).with("testpool03-4", statusCorrespondingTo(poolMode(ENABLED)));
    }

    @Test
    public void shouldTriggerUpdateScanTwiceWhenPoolGoesFromUpToDownToUp() throws Exception {
        givenTestPool("testpool03-4").goesFrom(poolMode(ENABLED)).to(poolMode(DISABLED_STRICT));
        whenReloadAndScanIsCalled();
        givenTestPool("testpool03-4").goesFrom(poolMode(DISABLED_STRICT)).to(poolMode(ENABLED));
        whenReloadAndScanIsCalled();
        using(aPoolOperationMapVerifier()).verifyThat("handlePoolStatusChange").isCalled(1)
              .on(poolOperationMap)
              .with("testpool03-4", statusCorrespondingTo(poolMode(DISABLED_STRICT)));
        using(aPoolOperationMapVerifier()).verifyThat("handlePoolStatusChange").isCalled(1)
              .on(poolOperationMap).with("testpool03-4", statusCorrespondingTo(poolMode(ENABLED)));
    }

    /* Pool groups whose marker changed. */

    @Test
    public void shouldTriggerScanWhenPoolGroupGoesPrimary() throws Exception {
        givenTestGroup("dmz-group").isChangedToPrimary();
        whenReloadAndScanIsCalled();
        using(aPoolOperationMapVerifier()).verifyThat("scan").isCalled(1).on(poolOperationMap)
              .with("testpool04-8", noAddedTo(), noRemovedFrom(), noStorageUnit(), anyMode(),
                    withStateCheck());
    }

    @Test
    public void shouldTriggerScanWhenPoolGroupGoesNonPrimary() throws Exception {
        givenTestGroup("persistent-tape-group").isChangedToNonPrimary();
        whenReloadAndScanIsCalled();
        using(aPoolOperationMapVerifier()).verifyThat("scan").isCalled(1).on(poolOperationMap)
              .with("testpool03-8", noAddedTo(), noRemovedFrom(), noStorageUnit(), anyMode(),
                    withStateCheck());
        using(aPoolOperationMapVerifier()).verifyThat("scan").isCalled(1).on(poolOperationMap)
              .with("testpool04-8", noAddedTo(), noRemovedFrom(), noStorageUnit(), anyMode(),
                    withStateCheck());
        using(aPoolOperationMapVerifier()).verifyThat("scan").isCalled(1).on(poolOperationMap)
              .with("testpool08-8", noAddedTo(), noRemovedFrom(), noStorageUnit(), anyMode(),
                    withStateCheck());
        using(aPoolOperationMapVerifier()).verifyThat("scan").isCalled(1).on(poolOperationMap)
              .with("testpool09-8", noAddedTo(), noRemovedFrom(), noStorageUnit(), anyMode(),
                    withStateCheck());
    }

    /*
     * Pools with changed tags.
     */
    @Test
    public void shouldTriggerScanWhenPoolTagsChanged() throws Exception {
        givenTestPool(POOL_ON_WHICH_TO_CHANGE_TAGS).withTagsChanged();
        whenReloadAndScanIsCalled();
        using(aPoolOperationMapVerifier()).verifyThat("scan").isCalled(1).on(poolOperationMap)
              .with(POOL_ON_WHICH_TO_CHANGE_TAGS, noAddedTo(), noRemovedFrom(), noStorageUnit(),
                    anyMode(), ignoringStateCheck());
    }

    private void assertNoUpdatesToPoolOperationMap() {
        verify(poolOperationMap, new AtMost(0)).add(any());
        verify(poolOperationMap, new AtMost(0)).remove(any());
        verify(poolOperationMap, new AtMost(0)).handlePoolStatusChange(anyString(), any());
        verify(poolOperationMap, new AtMost(0)).scan(anyString(), anyString(), anyString(),
              anyString(), any(), anyBoolean());
        verify(poolOperationMap, new AtMost(0)).setCurrentPsu(any());
    }

    private Stream<SelectionPool> forPoolsIn(String group) {
        return currentPoolMonitor.getPoolSelectionUnit().getPoolsByPoolGroup(group).stream();
    }

    private void givenAllPoolsWith(PoolV2Mode mode) {
        for (SelectionPool p : currentPoolMonitor.getPoolSelectionUnit()
              .getAllDefinedPools(false)) {
            givenTestPool(p.getName()).goesFrom(mode);
        }
    }

    private void givenNoChangeInStatus() throws Exception {
        nextPoolMonitor = currentPoolMonitor;
    }

    private PoolOpChangeHandlerTest givenTestPool(String pool) {
        this.testPool = pool;
        return this;
    }

    private PoolOpChangeHandlerTest givenTestUnit(String unit) {
        this.testUnit = unit;
        return this;
    }

    private PoolOpChangeHandlerTest givenTestGroup(String group) {
        this.testGroup = group;
        return this;
    }

    private PoolOpChangeHandlerTest goesFrom(PoolV2Mode mode) {
        currentPoolMonitor.getPoolSelectionUnit().getPool(testPool).setPoolMode(mode);
        when(poolOperationMap.getCurrentStatus(eq(testPool))).thenReturn(
              statusCorrespondingTo(mode));
        return this;
    }

    private void isAdded() throws Exception {
        nextPoolMonitor = PoolMonitorFactory.create(DEFAULT_POOL_INFO);
        PoolSelectionUnitV2 psu = (PoolSelectionUnitV2)nextPoolMonitor.getPoolSelectionUnit();
        psu.createPool(testPool, false, false, false);
    }

    private void isAddedTo(String group) throws Exception {
        testGroup = group;
        nextPoolMonitor = PoolMonitorFactory.create(DEFAULT_POOL_INFO);
        PoolSelectionUnitV2 psu = (PoolSelectionUnitV2)nextPoolMonitor.getPoolSelectionUnit();
        psu.addToPoolGroup(testGroup, testPool);
    }

    private void isAddedToUnitGroup(String unitGroup) throws Exception {
        nextPoolMonitor = PoolMonitorFactory.create(DEFAULT_POOL_INFO);
        PoolSelectionUnitV2 psu = (PoolSelectionUnitV2)nextPoolMonitor.getPoolSelectionUnit();
        psu.addToUnitGroup(unitGroup, testUnit, false);
    }

    private void isChangedToNonPrimary() throws Exception {
        nextPoolMonitor = PoolMonitorFactory.create(DEFAULT_POOL_INFO);
        PoolSelectionUnitV2 psu = (PoolSelectionUnitV2)nextPoolMonitor.getPoolSelectionUnit();
        Collection<SelectionPool> pools = psu.getPoolsByPoolGroup(testGroup);
        pools.forEach(p -> psu.removeFromPoolGroup(testGroup, p.getName()));
        psu.removePoolGroup(testGroup);
        psu.createPoolGroup(testGroup, false);
        pools.forEach(p -> psu.addToPoolGroup(testGroup, p.getName()));
    }

    private void isChangedToPartitionOn(String... tags) throws Exception {
        nextPoolMonitor = PoolMonitorFactory.create(DEFAULT_POOL_INFO);
        nextPoolMonitor.getPoolSelectionUnit().getStorageUnit(testUnit).setOnlyOneCopyPer(tags);
    }

    private void isChangedToPrimary() throws Exception {
        nextPoolMonitor = PoolMonitorFactory.create(DEFAULT_POOL_INFO);
        PoolSelectionUnitV2 psu = (PoolSelectionUnitV2)nextPoolMonitor.getPoolSelectionUnit();
        Collection<SelectionPool> pools = psu.getPoolsByPoolGroup(testGroup);
        pools.forEach(p -> psu.removeFromPoolGroup(testGroup, p.getName()));
        psu.removePoolGroup(testGroup);
        psu.createPoolGroup(testGroup, true);
        pools.forEach(p -> psu.addToPoolGroup(testGroup, p.getName()));
    }

    private void isChangedToRequire(int required) throws Exception {
        nextPoolMonitor = PoolMonitorFactory.create(DEFAULT_POOL_INFO);
        nextPoolMonitor.getPoolSelectionUnit().getStorageUnit(testUnit).setRequiredCopies(required);
    }

    private void isRemovedFrom(String group) throws Exception {
        testGroup = group;
        nextPoolMonitor = PoolMonitorFactory.create(DEFAULT_POOL_INFO);
        PoolSelectionUnitV2 psu = (PoolSelectionUnitV2) nextPoolMonitor.getPoolSelectionUnit();
        psu.removeFromPoolGroup(testGroup, testPool);
    }

    private void isRemovedFromUnitGroup(String unitGroup) throws Exception {
        nextPoolMonitor = PoolMonitorFactory.create(DEFAULT_POOL_INFO);
        PoolSelectionUnitV2 psu = (PoolSelectionUnitV2)nextPoolMonitor.getPoolSelectionUnit();
        psu.removeFromUnitGroup(unitGroup, testUnit, false);
    }

    private void isRemoved() throws Exception {
        nextPoolMonitor = PoolMonitorFactory.create(DEFAULT_POOL_INFO);
        PoolSelectionUnitV2 psu = (PoolSelectionUnitV2) nextPoolMonitor.getPoolSelectionUnit();
        psu.removePool(testPool);
    }

    private PoolQoSStatus statusCorrespondingTo(PoolV2Mode mode) {
        return PoolQoSStatus.valueOf(mode);
    }

    private void to(PoolV2Mode mode) throws Exception {
        nextPoolMonitor = PoolMonitorFactory.create(DEFAULT_POOL_INFO);
        nextPoolMonitor.getPoolSelectionUnit().getPool(testPool).setPoolMode(mode);
    }

    private void whenReloadAndScanIsCalled() {
        poolOpChangeHandler.reloadAndScan(nextPoolMonitor);
    }

    private Object[] anyArgs() {
        return null;
    }

    private String noAddedTo() {
        return null;
    }

    private String noRemovedFrom() {
        return null;
    }

    private String noStorageUnit() {
        return null;
    }

    private PoolV2Mode anyMode() {
        return null;
    }

    private PoolV2Mode poolMode(int mode) {
        return new PoolV2Mode(mode);
    }

    private boolean ignoringStateCheck() {
        return true;
    }

    private boolean withStateCheck() {
        return false;
    }

    private void withTagsChanged() throws Exception {
        nextPoolMonitor = PoolMonitorFactory.create(POOL_INFO_WITH_CHANGED_TAGS);
    }
}
