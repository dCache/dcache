/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.qos.services.verifier.handlers;

import static junit.framework.TestCase.assertTrue;
import static org.dcache.mock.MockVerifyBuilder.using;
import static org.dcache.mock.verifiers.VerifyAndUpdateHandlerVerifier.aVerifyAndUpdateHandlerVerifier;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.poolManager.PoolSelectionUnitV2;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolV2Mode;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.dcache.mock.PoolMonitorFactory;
import org.dcache.poolmanager.SerializablePoolMonitor;
import org.dcache.qos.services.verifier.data.PoolInfoMap;
import org.dcache.qos.services.verifier.util.VerifierMapInitializer;
import org.dcache.util.Args;
import org.junit.Before;
import org.junit.Test;

public class PoolInfoChangeHandlerTest {

    static final String DEFAULT_POOL_INFO = "org/dcache/mock/poolinfo.json";
    static final String MODIFIED_POOL_INFO = "org/dcache/mock/poolinfo-with-tags-changed.json";
    static final String POOL_ON_WHICH_TAGS_CHANGED = "testpool03-3";
    static final String POOL_ON_WHICH_COST_CHANGED = "testpool06-2";
    static final PoolV2Mode ENABLED = new PoolV2Mode(PoolV2Mode.ENABLED);

    SerializablePoolMonitor currentPoolMonitor;
    SerializablePoolMonitor nextPoolMonitor;
    PoolInfoChangeHandler poolInfoChangeHandler;
    VerifierMapInitializer initializer;
    VerifyAndUpdateHandler handler;
    PoolInfoMap poolInfoMap;

    String testPool;
    String testUnit;
    String testGroup;
    String testHsm;

    Set<String> poolsForHsm;

    PoolSelectionUnitV2 nextPsu;

    @Before
    public void setup() throws Exception {
        poolInfoMap = new PoolInfoMap();
        handler = mock(VerifyAndUpdateHandler.class);
        currentPoolMonitor = PoolMonitorFactory.create(DEFAULT_POOL_INFO);
        nextPoolMonitor = PoolMonitorFactory.create(DEFAULT_POOL_INFO);
        nextPsu = (PoolSelectionUnitV2) nextPoolMonitor.getPoolSelectionUnit();
        initializer = new VerifierMapInitializer();
        initializer.updatePoolMonitor(currentPoolMonitor);
        poolInfoChangeHandler = new PoolInfoChangeHandler();
        poolInfoChangeHandler.setMapInitializer(initializer);
        poolInfoChangeHandler.setPoolInfoMap(poolInfoMap);
        poolInfoChangeHandler.setPoolMonitor(currentPoolMonitor);
        poolInfoChangeHandler.setUpdateHandler(handler);
        poolInfoMap.apply(poolInfoMap.compare(currentPoolMonitor));
    }

    @Test
    public void shouldRemovePoolFromMapWhenRemovedFromPsu() throws Exception {
        givenAdminHasRemovedPool("testpool03-4");
        whenReloadAndScanIsCalled();
        verifyThat().testPoolIsRemovedFromMap();
        using(aVerifyAndUpdateHandlerVerifier()).verifyThat("cancelCurrentFileOpForPool")
              .isCalled(1).on(handler).with("testpool03-4");
    }

    @Test
    public void shouldRemoveGroupFromMapWhenRemovedFromPsu() throws Exception {
        givenAdminHasRemovedGroup("dmz-group");
        whenReloadAndScanIsCalled();
        verifyThat().testPoolGroupIsRemovedFromMap();
        using(aVerifyAndUpdateHandlerVerifier()).verifyThat("cancelCurrentFileOpForPool")
              .isCalled(1).on(handler).with("testpool04-8");
    }

    @Test
    public void shouldRemoveUnitFromMapWhenRemovedFromPsu() {
        givenAdminHasRemovedStorageUnit("bnltest.dcache-devel-test@enstore");
        whenReloadAndScanIsCalled();
        verifyThat().testUnitIsRemovedFromMap();
    }

    @Test
    public void shouldRemovePoolFromGroupWhenRemovedFromPsuGroup() {
        givenPool("testpool05-1").isRemovedFromPoolGroup("highavail-group");
        whenReloadAndScanIsCalled();
        verifyThat().testPoolIsRemovedFromGroupMapping();
    }

    @Test
    public void shouldRemoveUnitFromGroupMappingWhenRemovedFromPsuGroup() {
        givenUnit("bnltest.dcache-devel-test@enstore").isRemovedFromUnitGroup("internal");
        whenReloadAndScanIsCalled();
        verifyThat().testUnitDoesNotLinkToGroup("internal-group");
    }

    @Test
    public void shouldAddPoolToMapWhenAddedToPsu() {
        givenAdminHasAddedPool("testpool02-1");
        whenReloadAndScanIsCalled();
        verifyThat().testPoolIsAddedToMap();
    }

    @Test
    public void shouldAddGroupToMapWhenAddedToPsu() {
        givenAdminHasAddedGroup("test-group");
        whenReloadAndScanIsCalled();
        verifyThat().testPoolGroupIsAddedToMap();
    }

    @Test
    public void shouldAddUnitToMapWhenAddedToPsu() {
        givenAdminHasAddedStorageUnit("test.dcache-devel-test@enstore");
        whenReloadAndScanIsCalled();
        verifyThat().testUnitIsAddedToMap();
    }

    @Test
    public void shouldAddExistingUnitToGroupWhenAddedToNonPrimaryPsuGroup() {
        givenUnit("bnltest.dcache-devel-test@enstore").isAddedToUnitGroup("tape");
        whenReloadAndScanIsCalled();
        verifyThat().testUnitLinksToGroup("tape-group");
    }

    @Test
    public void shouldAddExistingUnitToGroupWhenAddedToPrimaryPsuGroup() {
        givenUnit("bnltest.dcache-devel-test@enstore").isAddedToUnitGroup("persistent");
        whenReloadAndScanIsCalled();
        verifyThat().testUnitLinksToGroup("persistent-group");
    }

    @Test
    public void shouldAddNewUnitToGroupWhenNewAddedToNonPrimaryPsuGroup() {
        givenAdminHasAddedStorageUnit("test.dcache-devel-test@enstore");
        givenUnit("test.dcache-devel-test@enstore").isAddedToUnitGroup("tape");
        whenReloadAndScanIsCalled();
        verifyThat().testUnitLinksToGroup("tape-group");
    }

    @Test
    public void shouldAddNewUnitToGroupWhenNewAddedToPrimaryPsuGroup() {
        givenAdminHasAddedStorageUnit("test.dcache-devel-test@enstore");
        givenUnit("test.dcache-devel-test@enstore").isAddedToUnitGroup("persistent");
        whenReloadAndScanIsCalled();
        verifyThat().testUnitLinksToGroup("persistent-group");
    }

    @Test
    public void shouldAddExistingPoolToGroupWhenAddedToPsuGroup() {
        givenPool("testpool07-3").isAddedToPoolGroup("dmz-group");
        whenReloadAndScanIsCalled();
        verifyThat().testPoolIsAddedTo("dmz-group");
    }

    @Test
    public void shouldAddNewPoolToGroupWhenAddedToPsuGroup() {
        givenAdminHasAddedPool("testpool02-1");
        givenPool("testpool02-1").isAddedToPoolGroup("dmz-group");
        whenReloadAndScanIsCalled();
        verifyThat().testPoolIsAddedTo("dmz-group");
    }

    @Test
    public void shouldModifyStorageUnitRequiredWhenChangedInPsu() {
        givenUnit("persistent-tape.dcache-devel-test@enstore").isChangedToRequire(4);
        whenReloadAndScanIsCalled();
        verifyThat().testUnitRequirementsForReplicasEquals(4);
    }

    @Test
    public void shouldModifyStorageUnitOnlyOnePerWhenChangedInPsu() {
        givenUnit("persistent-tape.dcache-devel-test@enstore").isChangedToRequire("hostname",
              "rack");
        whenReloadAndScanIsCalled();
        verifyThat().testUnitRequirementsForReplicasEquals("hostname", "rack");
    }

    @Test
    public void shouldReflectChangedTagsWhenPoolTagsModified() throws Exception {
        givenPool(POOL_ON_WHICH_TAGS_CHANGED).withPoolInfoChanged();
        whenReloadAndScanIsCalled();
        verifyThat().testPoolTagsWereChanged();
    }

    @Test
    public void shouldReflectChangedCostWhenPoolCostModified() throws Exception {
        givenPool(POOL_ON_WHICH_COST_CHANGED).withPoolInfoChanged();
        whenReloadAndScanIsCalled();
        verifyThat().testPoolCostChanged();
    }

    @Test
    public void shouldAddHsmToPoolWhenAddedToPsu() {
        givenPool("testpool03-8").acquiresBackend("osm");
        givenNextPoolsAllEnabled();
        whenReloadAndScanIsCalled();
        verifyThat().thePoolsForAnyStorageUnit().containsTestPool();
    }

    @Test
    public void shouldFindHsmPoolForUnitWhenAddedToPsu() throws Exception {
        givenPool("testpool03-8").acquiresBackend("osm");
        givenNextPoolsAllEnabled();
        whenReloadAndScanIsCalled();
        verifyThat().thePoolsForStorageUnit("bnltest.dcache-devel-test@enstore").containsTestPool();
    }

    @Test
    public void shouldReturnReadPref0WhenPoolIsInGroupThatHasNoLinksPointingToIt() throws Exception {
        whenReloadAndScanIsCalled();
        forPoolsIn("stage-group").map(SelectionPool::getName).forEach(p-> {
            givenPool(p);
            verifyThat().testPoolHasReadPref0();
        });
    }

    @Test
    public void shouldSendWarningWhenPrimaryGroupDoesNotSupportUnitRequirements() throws Exception {
        givenAdminHasAddedPrimaryGroup("primary-group");
        givenPoolGroup("primary-group").isAddedToLink("dmz-link");
        givenPool("testpool03-4").isAddedToPoolGroup("primary-group");
        givenUnit("bnltest.dcache-devel-test@enstore").isChangedToRequire(2);
        whenReloadAndScanIsCalled();
        verifyThat().theNumberOfVerifyWarningsSentIs(1);
    }

    private void acquiresBackend(String hsm) {
        testHsm = hsm;
        nextPsu.getPool(testPool).setHsmInstances(Set.of(hsm));
    }

    private void containsTestPool() {
        assertTrue("HSM pools do not include " + testPool + "!", poolsForHsm.contains(testPool));
    }

    private boolean equals(PoolCostInfo expected, PoolCostInfo actual) {
        return actual != null &&
              expected.getPerformanceCost() == actual.getPerformanceCost() &&
              expected.getMoverCostFactor() == actual.getMoverCostFactor() &&
              expected.getSpaceInfo().getFreeSpace() == actual.getSpaceInfo().getFreeSpace() &&
              expected.getSpaceInfo().getPreciousSpace() == actual.getSpaceInfo().getPreciousSpace() &&
              expected.getSpaceInfo().getRemovableSpace() == actual.getSpaceInfo().getRemovableSpace() &&
              expected.getSpaceInfo().getUsedSpace() == actual.getSpaceInfo().getUsedSpace() &&
              expected.getSpaceInfo().getTotalSpace() == actual.getSpaceInfo().getTotalSpace() &&
              expected.getSpaceInfo().getBreakEven() == actual.getSpaceInfo().getBreakEven() &&
              expected.getSpaceInfo().getGap() == actual.getSpaceInfo().getGap() &&
              expected.getSpaceInfo().getLRUSeconds() == actual.getSpaceInfo().getLRUSeconds();
    }

    private Stream<SelectionPool> forPoolsIn(String group) {
        return currentPoolMonitor.getPoolSelectionUnit().getPoolsByPoolGroup(group).stream();
    }

    private void givenAdminHasAddedGroup(String group) {
        testGroup = group;
        nextPsu.createPoolGroup(group, false);
    }

    private void givenAdminHasAddedPrimaryGroup(String group) {
        testGroup = group;
        nextPsu.createPoolGroup(group, true);
    }

    private void givenAdminHasAddedPool(String pool) {
        testPool = pool;
        nextPsu.createPool(pool, false, false, false);
    }

    private void givenAdminHasAddedStorageUnit(String unit) {
        testUnit = unit;
        nextPsu.createUnit(unit, false, true, false, false);
    }

    private void givenAdminHasRemovedPool(String pool) {
        testPool = pool;
        nextPsu.removePool(pool);
    }

    private void givenAdminHasRemovedGroup(String group) {
        testGroup = group;
        nextPsu.getPoolsByPoolGroup(group)
              .forEach(pool -> nextPsu.removeFromPoolGroup(group, pool.getName()));
        nextPsu.removePoolGroup(group);
    }

    private void givenAdminHasRemovedStorageUnit(String unit) {
        testUnit = unit;
        nextPsu.removeUnit(unit, false);
    }

    private void givenNextPoolsAllEnabled() {
        nextPsu.getAllDefinedPools(false).forEach(p-> {
           p.setPoolMode(ENABLED);
           p.setActive(true);
        });
    }

    private PoolInfoChangeHandlerTest givenPool(String pool) {
        testPool = pool;
        return this;
    }

    private PoolInfoChangeHandlerTest givenPoolGroup(String group) {
        testGroup = group;
        return this;
    }

    private PoolInfoChangeHandlerTest givenUnit(String unit) {
        testUnit = unit;
        return this;
    }

    private void isAddedToPoolGroup(String group) {
        testGroup = group;
        nextPsu.addToPoolGroup(group, testPool);
    }

    private void isAddedToUnitGroup(String group) {
        nextPsu.addToUnitGroup(group, testUnit, false);
    }

    private void isAddedToLink(String link) {
        Args args = new Args(link + " " + testGroup);
        nextPsu.ac_psu_addto_link_$_2(args);
    }

    private void isChangedToRequire(int required) {
        nextPoolMonitor.getPoolSelectionUnit().getStorageUnit(testUnit).setRequiredCopies(required);
    }

    private void isChangedToRequire(String... onlyOneCopyPer) {
        nextPoolMonitor.getPoolSelectionUnit().getStorageUnit(testUnit)
              .setOnlyOneCopyPer(onlyOneCopyPer);
    }

    private void isRemovedFromPoolGroup(String group) {
        testGroup = group;
        nextPsu.removeFromPoolGroup(group, testPool);
    }

    private void isRemovedFromUnitGroup(String group) {
        nextPsu.removeFromUnitGroup(group, testUnit, false);
    }

    private void testPoolHasReadPref0() {
        assertTrue(testPool + " does not have readPref = 0!",
              poolInfoMap.isReadPref0(testPool));
    }

    private void testPoolIsAddedTo(String group) {
        assertTrue(testPool + " was not added to " + group + "!.",
              poolInfoMap.getPoolsOfGroup(testGroup).contains(testPool));
    }

    private void testPoolIsAddedToMap() {
        assertTrue(testPool + " was not added to the map!.",
              poolInfoMap.hasPool(testPool));
    }

    private void testPoolIsRemovedFromMap() {
        assertFalse(testGroup + " was not removed from the map!.",
              poolInfoMap.hasGroup(testGroup));
    }

    private void testPoolIsRemovedFromGroupMapping() {
        assertFalse(testPool + " was not removed from the mapping for " + testGroup + "!.",
              poolInfoMap.getPoolsOfGroup(testGroup).contains(testPool));
    }

    private void testPoolCostChanged() {
        assertTrue("Pool cost change was not registered for " + testPool,
             equals(nextPoolMonitor.getCostModule().getPoolCostInfo(testPool),
              poolInfoMap.getPoolManagerInfo(testPool).getPoolCostInfo()));
    }

    private void testPoolTagsWereChanged() {
        assertEquals("Pool info change was not registered for " + testPool,
              nextPoolMonitor.getCostModule().getPoolInfo(testPool).getTags(),
              poolInfoMap.getTags(testPool));
    }

    private void testPoolGroupIsAddedToMap() {
        assertTrue(testGroup + " was not added to the map!.",
              poolInfoMap.hasGroup(testGroup));
    }

    private void testPoolGroupIsRemovedFromMap() {
        assertFalse(testPool + " was not removed from the map!.",
              poolInfoMap.hasPool(testPool));
    }

    private void testUnitIsAddedToMap() {
        assertTrue(testUnit + " was not added to the map!.",
              poolInfoMap.hasUnit(testUnit));
    }

    private void testUnitIsRemovedFromMap() {
        assertFalse(testUnit + " was not removed from the map!.",
              poolInfoMap.hasUnit(testUnit));
    }

    private void testUnitRequirementsForReplicasEquals(int replicas) {
        assertEquals(testUnit + " has incorrect -requires!.", replicas,
              poolInfoMap.getConstraints(testUnit).getRequired());
    }

    private void testUnitRequirementsForReplicasEquals(String... onlyOnePer) {
        assertEquals(testUnit + " has incorrect -requires!.",
              Arrays.stream(onlyOnePer).collect(Collectors.toSet()),
              poolInfoMap.getConstraints(testUnit).getOneCopyPer());
    }

    private void testUnitDoesNotLinkToGroup(String group) {
        assertFalse(testUnit + " still links to " + group + "!.",
              poolInfoMap.getStorageUnitsForGroup(group).contains(testUnit));
    }

    private void testUnitLinksToGroup(String group) {
        assertTrue(testUnit + " does not link to " + group + "!.",
              poolInfoMap.getStorageUnitsForGroup(group).contains(testUnit));
    }

    private void theNumberOfVerifyWarningsSentIs(int warnings) {
        assertEquals("The number of verify warnings was not " + warnings + "!.", warnings,
              poolInfoMap.verifyWarnings());
    }

    private PoolInfoChangeHandlerTest thePoolsForAnyStorageUnit() {
        poolsForHsm = poolInfoMap.getHsmPoolsForStorageUnit(null, Set.of(testHsm));
        return this;
    }

    private PoolInfoChangeHandlerTest thePoolsForStorageUnit(String storageUnit) {
        poolsForHsm = poolInfoMap.getHsmPoolsForStorageUnit(storageUnit, Set.of(testHsm));
        return this;
    }

    private void whenReloadAndScanIsCalled() {
        poolInfoChangeHandler.reloadAndScan(nextPoolMonitor);
    }

    private PoolInfoChangeHandlerTest verifyThat() {
        return this;
    }

    private void withPoolInfoChanged() throws Exception {
        nextPoolMonitor = PoolMonitorFactory.create(MODIFIED_POOL_INFO);
    }
}
