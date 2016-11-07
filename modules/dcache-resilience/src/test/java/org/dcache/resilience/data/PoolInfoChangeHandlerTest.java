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
package org.dcache.resilience.data;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.CacheException;
import org.dcache.resilience.TestBase;
import org.dcache.resilience.TestSynchronousExecutor;
import org.dcache.resilience.TestSynchronousExecutor.Mode;
import org.dcache.resilience.data.PoolOperation.State;
import org.dcache.resilience.handlers.FileOperationHandler;
import org.dcache.resilience.handlers.PoolInfoChangeHandler;
import org.dcache.resilience.handlers.ResilienceMessageHandler;
import org.dcache.resilience.util.BackloggedMessageHandler;
import org.dcache.resilience.util.MessageGuard;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * <p>Tests the application of the pool monitor diffs.  Note that
 *      for the purposes of these tests, the transition from uninitialized pool
 *      to initialized is ignored.</p>
 */
public class PoolInfoChangeHandlerTest extends TestBase {
    PoolInfoChangeHandler poolMonitorChangeHandler;
    PoolInfoDiff          diff;
    Integer               removedPoolIndex;

    @Before
    public void setUp() throws CacheException {
        setUpBase();
        setShortExecutionMode(Mode.RUN);
        setLongExecutionMode(Mode.NOP);
        createCounters();
        createPoolOperationHandler();
        createPoolOperationMap();
        fileOperationHandler = mock(FileOperationHandler.class);
        fileOperationMap = mock(FileOperationMap.class);
        initializeCounters();
        wirePoolOperationMap();
        wirePoolOperationHandler();
        poolOperationMap.loadPools();

        poolInfoMap.getResilientPools().stream()
                   .forEach((p) -> {
                            PoolV2Mode mode = new PoolV2Mode(PoolV2Mode.ENABLED);
                            PoolStateUpdate update = new PoolStateUpdate(p, mode);
                            poolInfoMap.updatePoolStatus(update);
                            poolOperationMap.update(update);
                   });

        ResilienceMessageHandler handler = new ResilienceMessageHandler();
        handler.setCounters(counters);
        handler.setFileOperationHandler(fileOperationHandler);
        handler.setPoolInfoMap(poolInfoMap);
        handler.setPoolOperationHandler(poolOperationHandler);
        handler.setUpdateService(shortJobExecutor);
        MessageGuard guard = new MessageGuard();
        guard.setBacklogHandler(mock(BackloggedMessageHandler.class));
        guard.enable();
        handler.setMessageGuard(guard);

        poolMonitorChangeHandler = new PoolInfoChangeHandler();
        poolMonitorChangeHandler.setPoolInfoMap(poolInfoMap);
        poolMonitorChangeHandler.setFileOperationMap(fileOperationMap);
        poolMonitorChangeHandler.setPoolOperationMap(poolOperationMap);
        poolMonitorChangeHandler.setResilienceMessageHandler(handler);
        poolMonitorChangeHandler.setUpdateService(new TestSynchronousExecutor(Mode.RUN));
    }

    @After
    public void tearDown() {
        clearInMemory();
    }

    @Test
    public void shouldAddNonResilientGroupToMap() {
        givenPsuUpdate();
        whenPsuUpdateContainsNewNonResilientPoolGroup("new-standard-group");
        whenPsuChangeHelperIsCalled();
        assertThatPoolInfoContainsPoolGroup("new-standard-group");
        assertThatScanIsNotCalled();
    }

    @Test
    public void shouldAddOrphanedPoolToOperationMapWhenAddedToResilientGroup() {
        shouldAddPoolToInfoButNotOperationMap();
        whenPsuUpdateContainsNewPoolForPoolGroup("new-pool", "resilient-group");
        whenPsuChangeHelperIsCalled();
        assertThatPoolInfoContainsPoolForPoolGroup("new-pool", "resilient-group");
        assertThatPoolIsBeingScanned("new-pool");
    }

    @Test
    public void shouldAddPoolToInfoButNotOperationMap() {
        givenPsuUpdate();
        whenPsuUpdateContainsNewPool("new-pool");
        whenPsuChangeHelperIsCalled();
        assertThatPoolInfoContainsPool("new-pool");
        assertThatPoolOperationHasNotBeenAddedFor("new-pool");
    }

    @Test
    public void shouldAddPoolToPoolGroupAndScan() {
        givenPsuUpdate();
        whenPsuUpdateContainsNewPool("new-pool");
        whenPsuUpdateContainsNewPoolForPoolGroup("new-pool", "resilient-group");
        whenPsuChangeHelperIsCalled();
        assertThatPoolInfoContainsPoolForPoolGroup("new-pool",
                        "resilient-group");
        assertThatPoolIsBeingScanned("new-pool");
    }

    @Test
    public void shouldAddPoolToPoolGroupButNotScan() {
        givenPsuUpdate();
        whenPsuUpdateContainsNewPool("new-pool");
        whenPsuUpdateContainsNewPoolForPoolGroup("new-pool", "standard-group");
        whenPsuChangeHelperIsCalled();
        assertThatPoolInfoContainsPoolForPoolGroup("new-pool",
                        "standard-group");
        assertThatNoScanCalledFor("new-pool");
    }

    @Test
    public void shouldAddResilientGroupToMap() {
        givenPsuUpdate();
        whenPsuUpdateContainsNewResilientPoolGroup("new-resilient-group");
        whenPsuChangeHelperIsCalled();
        assertThatPoolInfoContainsPoolGroup("new-resilient-group");
        assertThatScanIsNotCalled();
    }

    @Test
    public void shouldAddStorageUnitToGroupOnLinkChange() {
        givenPsuUpdate();
        whenPsuUpdateContainsNewLinkToResilientUnitGroup("new-link");
        whenPsuUpdateContainsPoolGroupAddedToNewLink("standard-group",
                        "new-link");
        whenPsuChangeHelperIsCalled();
        assertThatPoolInfoContainsResilientUnitsForPoolGroup("standard-group");
    }

    @Test
    public void shouldAddStorageUnitToMap() {
        givenPsuUpdate();
        whenPsuUpdateContainsNewStorageUnit("new-default-unit");
        whenPsuChangeHelperIsCalled();
        assertThatPoolInfoContainsStorageUnit("new-default-unit");
        assertThatScanIsNotCalled();
    }

    @Test
    public void shouldCancelAndRemovePoolFromPoolGroupAndScan() {
        givenPsuUpdate();
        whenPoolIsWaitingToBeScanned("resilient_pool-1");
        whenPsuUpdateNoLongerContainsPoolForPoolGroup("resilient_pool-1",
                        "resilient-group");
        whenPsuChangeHelperIsCalled();
        assertThatCancelScanHasBeenCalled();
        assertThatPoolIsBeingScanned("resilient_pool-1");
    }

    @Test
    public void shouldRemovePoolFromMap() {
        givenPsuUpdate();
        whenPsuUpdateNoLongerContainsPool("resilient_pool-2");
        whenPsuChangeHelperIsCalled();
        assertThatPoolInfoDoesNotContainPool("resilient_pool-2");
        assertThatPoolInfoDoesNotContainPoolForPoolGroup(removedPoolIndex,
                        "resilient-group");
        assertThatNoScanCalledFor("resilient_pool-2");
    }

    @Test
    public void shouldRemovePoolGroupFromMap() {
        givenPsuUpdate();
        whenPsuUpdateNoLongerContainsPoolGroup("resilient-group");
        whenPsuChangeHelperIsCalled();
        /*
         * This essentially 'orphans' the pools.  Would not be
         * done without removing the pools first.
         */
        assertThatPoolInfoDoesNotContainPoolGroup("resilient-group");
        assertThatScanIsNotCalled();
    }

    @Test
    public void shouldRemoveStorageUnitFromMap() {
        givenPsuUpdate();
        whenPsuUpdateNoLongerContainsStorageUnit(
                        "resilient-0.dcache-devel-test@enstore");
        whenPsuChangeHelperIsCalled();
        /*
         * This essentially 'orphans' files belonging to it.  Would not be
         * done without removing the files first.
         */
        assertThatPoolInfoDoesNotContainStorageUnit(
                        "resilient-0.dcache-devel-test@enstore");
        assertThatScanIsNotCalled();
    }

    @Test
    public void shouldRemoveStorageUnitsFromGroupOnLinkChange() {
        givenPsuUpdate();
        whenPsuUpdateNoLongerContainsLink("resilient-link");
        whenPsuChangeHelperIsCalled();
        assertThatPoolInfoDoesNotContainResilientUnitsForPoolGroup(
                        "resilient-group");
    }

    @Test
    public void shouldScanGroupWhenStorageConstraintsAreModified() {
        givenPsuUpdate();
        whenPsuUpdateContainsStorageUnitWithNewConstraints(
                        "resilient-0.dcache-devel-test@enstore");
        whenPsuChangeHelperIsCalled();
        assertThatPoolsInPoolGroupAreBeingScanned("resilient-group");
    }

    private void assertThatCancelScanHasBeenCalled() {
        verify(fileOperationMap, times(3)).cancel(any(FileFilter.class));
    }

    private void assertThatNoScanCalledFor(String pool) {
        if (poolOperationMap.idle.containsKey(pool)) {
            assertEquals("IDLE", poolOperationMap.getState(pool));
        } else {
            assertFalse(poolOperationMap.waiting.containsKey(pool));
        }
    }

    private void assertThatPoolInfoContainsPool(String pool) {
        assertNotNull(poolInfoMap.getPoolIndex(pool));
    }

    private void assertThatPoolInfoContainsPoolForPoolGroup(String pool,
                    String group) {
        assertTrue(poolInfoMap.getPoolsOfGroup(
                        poolInfoMap.getGroupIndex(group)).contains(
                        poolInfoMap.getPoolIndex(pool)));
    }

    private void assertThatPoolInfoContainsPoolGroup(String group) {
        assertNotNull(poolInfoMap.getGroupIndex(group));
    }

    private void assertThatPoolInfoContainsResilientUnitsForPoolGroup(
                    String group) {
        Set<String> units = poolInfoMap.getStorageUnitsFor(group).stream()
                                       .map(poolInfoMap::getUnit)
                                       .collect(Collectors.toSet());
        assertTrue(units.contains("resilient-0.dcache-devel-test@enstore"));
        assertTrue(units.contains("resilient-1.dcache-devel-test@enstore"));
        assertTrue(units.contains("resilient-2.dcache-devel-test@enstore"));
        assertTrue(units.contains("resilient-3.dcache-devel-test@enstore"));
        assertTrue(units.contains("resilient-4.dcache-devel-test@enstore"));
    }

    private void assertThatPoolInfoContainsStorageUnit(String unit) {
        assertNotNull(poolInfoMap.getUnitIndex(unit));
    }

    private void assertThatPoolInfoDoesNotContainPool(String pool) {
        Integer index = null;
        try {
            index = poolInfoMap.getPoolIndex(pool);
        } catch (NoSuchElementException e) {
            assertNull(index);
        }
    }

    private void assertThatPoolInfoDoesNotContainPoolForPoolGroup(Integer pool,
                    String group) {
        assertFalse(poolInfoMap.getPoolsOfGroup(
                        poolInfoMap.getGroupIndex(group)).contains(pool));
    }

    private void assertThatPoolInfoDoesNotContainPoolGroup(String group) {
        Integer index = null;
        try {
            index = poolInfoMap.getGroupIndex(group);
        } catch (NoSuchElementException e) {
            assertNull(index);
        }
    }

    private void assertThatPoolInfoDoesNotContainResilientUnitsForPoolGroup(
                    String group) {
        Set<String> units = poolInfoMap.getStorageUnitsFor(group).stream().map(
                        poolInfoMap::getGroup).collect(Collectors.toSet());
        assertFalse(units.contains("resilient-0.dcache-devel-test@enstore"));
        assertFalse(units.contains("resilient-1.dcache-devel-test@enstore"));
        assertFalse(units.contains("resilient-2.dcache-devel-test@enstore"));
        assertFalse(units.contains("resilient-3.dcache-devel-test@enstore"));
        assertFalse(units.contains("resilient-4.dcache-devel-test@enstore"));
    }

    private void assertThatPoolInfoDoesNotContainStorageUnit(String unit) {
        Integer index = null;
        try {
            index = poolInfoMap.getUnitIndex(unit);
        } catch (NoSuchElementException e) {
            assertNull(index);
        }
    }

    private void assertThatPoolIsBeingScanned(String pool) {
        assertTrue(poolOperationMap.waiting.containsKey(pool));
        assertEquals("WAITING", poolOperationMap.getState(pool));
    }

    private void assertThatPoolOperationHasNotBeenAddedFor(String pool) {
        assertFalse(poolOperationMap.idle.containsKey(pool));
        assertFalse(poolOperationMap.waiting.containsKey(pool));
    }

    private void assertThatPoolsInPoolGroupAreBeingScanned(String group) {
        poolInfoMap.getPoolsOfGroup(
                        poolInfoMap.getGroupIndex(group)).stream().forEach(
                        (p) -> assertThatPoolIsBeingScanned(
                                        poolInfoMap.getPool(p)));
    }

    private void assertThatScanIsNotCalled() {
        assertEquals(0, poolOperationMap.waiting.size());
    }

    private void givenPsuUpdate() {
        createNewPoolMonitor();
    }

    private void whenPoolIsWaitingToBeScanned(String pool) {
        PoolOperation operation = new PoolOperation();
        operation.state = State.WAITING;
        operation.currStatus = PoolStatusForResilience.ENABLED;
        poolOperationMap.waiting.put(pool, operation);
    }

    private void whenPsuChangeHelperIsCalled() {
        diff = poolMonitorChangeHandler.reloadAndScan(newPoolMonitor);
    }

    private void whenPsuUpdateContainsNewLinkToResilientUnitGroup(String link) {
        getUpdatedPsu().createLink(link, ImmutableList.of("resilient-storage"));
    }

    private void whenPsuUpdateContainsNewNonResilientPoolGroup(String group) {
        getUpdatedPsu().createPoolGroup(group, false);
    }

    private void whenPsuUpdateContainsNewPool(String pool) {
        createNewPool(pool);
    }

    private void whenPsuUpdateContainsNewPoolForPoolGroup(String pool,
                    String group) {
        getUpdatedPsu().addToPoolGroup(group, pool);
    }

    private void whenPsuUpdateContainsNewResilientPoolGroup(String group) {
        getUpdatedPsu().createPoolGroup(group, true);
    }

    private void whenPsuUpdateContainsNewStorageUnit(String unit) {
        getUpdatedPsu().createUnit(unit, false, true, false, false);
    }

    private void whenPsuUpdateContainsPoolGroupAddedToNewLink(String group,
                    String link) {
        getUpdatedPsu().addLink(link, group);
    }

    private void whenPsuUpdateContainsStorageUnitWithNewConstraints(
                    String unit) {
        getUpdatedPsu().setStorageUnit(unit, null, new String[] { "subnet" });
    }

    private void whenPsuUpdateNoLongerContainsLink(String link) {
        getUpdatedPsu().removeLink(link);
    }

    private void whenPsuUpdateNoLongerContainsPool(String pool) {
        removedPoolIndex = poolInfoMap.getPoolIndex(pool);
        getUpdatedPsu().removePool(pool);
    }

    private void whenPsuUpdateNoLongerContainsPoolForPoolGroup(String pool,
                    String group) {
        getUpdatedPsu().removeFromPoolGroup(group, pool);
    }

    private void whenPsuUpdateNoLongerContainsPoolGroup(String group) {
        getUpdatedPsu().getPoolsByPoolGroup(group).stream().forEach(
                        (p) -> getUpdatedPsu().removeFromPoolGroup(group,
                                        p.getName()));
        getUpdatedPsu().removePoolGroup(group);
    }

    private void whenPsuUpdateNoLongerContainsStorageUnit(String unit) {
        getUpdatedPsu().removeUnit(unit, false);
    }
}
