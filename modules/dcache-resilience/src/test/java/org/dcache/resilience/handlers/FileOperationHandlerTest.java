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
package org.dcache.resilience.handlers;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;
import org.dcache.pool.migration.Task;
import org.dcache.resilience.TestBase;
import org.dcache.resilience.TestMessageProcessor;
import org.dcache.resilience.TestSynchronousExecutor.Mode;
import org.dcache.resilience.data.FileOperation;
import org.dcache.resilience.data.FileUpdate;
import org.dcache.resilience.data.MessageType;
import org.dcache.resilience.data.PoolOperation;
import org.dcache.resilience.data.PoolStateUpdate;
import org.dcache.resilience.data.StorageUnitConstraints;
import org.dcache.resilience.data.PoolOperation.SelectionAction;
import org.dcache.resilience.util.ResilientFileTask;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.resilience.RemoveReplicaMessage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public final class FileOperationHandlerTest extends TestBase
                implements TestMessageProcessor {
    FileUpdate           update;
    FileAttributes       attributes;
    RemoveReplicaMessage repRmMessage;
    ResilientFileTask    task;
    String               verifyType;
    String               storageUnit;
    Integer              originalTarget;
    Integer              originalSource;
    boolean rmMessageFailure = false;

    @Override
    public void processMessage(Message message) throws Exception {
        if (message instanceof RemoveReplicaMessage) {
            if (rmMessageFailure) {
                throw new Exception(FORCED_FAILURE);
            }

            repRmMessage = (RemoveReplicaMessage) message;
        }
    }

    @Test
    public void shouldCreateMigrationTaskWhenVerifyResultIsCopy()
                    throws CacheException, IOException, InterruptedException {
        setUpTest(false);
        givenAFileUpdateForANewFileOnAPoolWithHostAndRackTags();
        whenHandleUpdateIsCalled();
        whenSourceAndTargetAreSelected();
        whenTaskIsCreatedAndCalled();
        assertThatCorrectMigrationTaskWasCreated();
    }

    @Test
    public void shouldFailOnFatalFailure()
                    throws CacheException, IOException, InterruptedException {
        setUpTest(false);
        givenAFileUpdateForANewFileOnAPoolWithNoTags();
        whenHandleUpdateIsCalled();
        whenVerifyIsRun();
        whenOperationFailsFatally();
        assertNull(fileOperationMap.getOperation(update.pnfsId));
    }

    @Test
    public void shouldFailOnNewLocFailureWhereThereIsNoOtherTarget()
                    throws CacheException, InterruptedException, IOException {
        setUpTest(false);
        givenAFileUpdateForANewFileOnAPoolWithNoTags();
        givenUpdateHasBeenAddedToMapWithCountOf(1);
        whenVerifyIsRun();
        afterInspectingSourceAndTarget();
        givenAllPoolsOfflineExceptSourceAndTarget();
        whenOperationFailsWithNewTargetError();
        whenVerifyIsRun();
        whenScanIsRun();
        assertNull(fileOperationMap.getOperation(update.pnfsId));
    }

    @Test
    public void shouldNotProcessUpdateWhenClearCacheLocationWithNoLocations()
                    throws CacheException, IOException, InterruptedException {
        setUpTest(false);
        givenAFileUpdateClearCacheLocationForAFileWithNoLocationsInNamespace();
        whenHandleUpdateIsCalled();
        assertTrue(noOperationHasBeenAdded());
    }

    @Test
    public void shouldNotProcessUpdateWhenFileDeletedFromNamespace()
                    throws CacheException, IOException, InterruptedException {
        setUpTest(false);
        givenAFileUpdateForAFileDeletedFromNamespace();
        whenHandleUpdateIsCalled();
        assertTrue(noOperationHasBeenAdded());
    }

    @Test
    public void shouldNotProcessUpdateWhenPoolNotResilient()
                    throws CacheException, IOException, InterruptedException {
        setUpTest(false);
        givenAFileUpdateForANewFileOnNonResilientPool();
        whenHandleUpdateIsCalled();
        assertTrue(noOperationHasBeenAdded());
    }

    @Test
    public void shouldNotProcessUpdateWhenStorageGroupNotResilient()
                    throws CacheException, IOException, InterruptedException {
        setUpTest(false);
        givenAFileUpdateForANewFileResilientPoolButRequiringASingleCopy();
        whenHandleUpdateIsCalled();
        assertTrue(noOperationHasBeenAdded());
    }

    @Test
    public void shouldNotSendSetStickyMessageOnScan()
                    throws CacheException, IOException, InterruptedException {
        setUpTest(false);
        givenAFileUpdateFromAPoolScan();
        whenHandleUpdateIsCalled();
        assertFalse(update.shouldVerifySticky());
    }

    @Test
    public void shouldReturnCopyWhenRequiredCopyMissing()
                    throws CacheException, IOException, InterruptedException {
        setUpTest(false);
        givenAFileUpdateForANewFileOnAPoolWithNoTags();
        whenHandleUpdateIsCalled();
        whenVerifyIsRun();
        assertEquals("COPY", verifyType);
    }

    @Test
    public void shouldReturnRemoveWhenExcessCopiesExist()
                    throws CacheException, IOException, InterruptedException {
        setUpTest(false);
        givenAFileUpdateForAFileWithExcessLocations();
        whenHandleUpdateIsCalled();
        whenVerifyIsRun();
        assertEquals("REMOVE", verifyType);
    }

    @Test
    public void shouldReturnRemoveWhenTagChangeRequiresRedistribution()
                    throws CacheException, IOException, InterruptedException {
        setUpTest(false);
        givenAFileUpdateForAFileWithExcessLocations();
        givenANewTagRequirementForFileStorageUnit();
        givenUpdateHasBeenAddedToMapWithCountOf(1);
        whenVerifyIsRun();
        assertEquals("REMOVE", verifyType);
        assertEquals(2, fileOperationMap.getOperation(update.pnfsId).getOpCount());
    }

    @Test
    public void shouldReturnVoidWhenNothingToDo()
                    throws CacheException, IOException, InterruptedException {
        setUpTest(false);
        givenAFileUpdateForAFileWithRequiredLocations();
        givenUpdateHasBeenAddedToMapWithCountOf(1);
        whenVerifyIsRun();
        assertEquals("VOID", verifyType);
    }

    @Test
    public void shouldSendRemoveRequestOnExcessLocation()
                    throws CacheException, IOException, InterruptedException {
        setUpTest(true);
        givenAFileUpdateForAFileWithExcessLocations();
        whenHandleUpdateIsCalled();
        whenTaskIsCreatedAndCalled();
        assertNotNull(repRmMessage);
    }

    @Test
    public void shouldSendSetStickyMessageOnNewLocation()
                    throws CacheException, IOException, InterruptedException {
        setUpTest(false);
        givenAFileUpdateForANewFileOnAPoolWithHostAndRackTags();
        whenHandleUpdateIsCalled();
        assertTrue(update.shouldVerifySticky());
    }

    @Test
    public void shouldSendSetStickyMessageOnPoolAddedToGroup()
                    throws CacheException, IOException, InterruptedException {
        setUpTest(false);
        givenAFileUpdateFromAPoolScanForPoolAddedToGroup();
        whenHandleUpdateIsCalled();
        assertTrue(update.shouldVerifySticky());
    }

    @Test
    public void shouldTryAnotherSourceOnBrokenFile()
                    throws CacheException, IOException, InterruptedException {
        setUpTest(false);
        givenAFileUpdateForAFileWithOneLocationOffline();
        whenHandleUpdateIsCalled();
        whenVerifyIsRun();
        afterInspectingSourceAndTarget();
        whenOperationFailsWithBrokenFileError();
        whenVerifyIsRun();
        assertTrue(theNewSourceIsDifferent());
    }

    @Test
    public void shouldTryAnotherSourceOnSourceError()
                    throws CacheException, IOException, InterruptedException {
        setUpTest(false);
        givenAFileUpdateForAFileWithOneLocationOffline();
        whenHandleUpdateIsCalled();
        whenVerifyIsRun();
        afterInspectingSourceAndTarget();
        whenOperationFailsWithSourceError();
        whenVerifyIsRun();
        assertTrue(theNewSourceIsDifferent());
    }

    @Test
    public void shouldTryAnotherTargetOnNewTargetFailure()
                    throws CacheException, IOException, InterruptedException {
        setUpTest(false);
        givenAFileUpdateForAFileWithOneLocationOffline();
        whenHandleUpdateIsCalled();
        whenVerifyIsRun();
        afterInspectingSourceAndTarget();
        whenOperationFailsWithNewTargetError();
        whenVerifyIsRun();
        assertTrue(theNewTargetIsDifferent());
    }

    @Test
    public void shouldTryAnotherTargetOnRetryMaxReached()
                    throws CacheException, IOException, InterruptedException {
        setUpTest(false);
        givenAFileUpdateForAFileWithOneLocationOffline();
        whenHandleUpdateIsCalled();
        whenVerifyIsRun();
        afterInspectingSourceAndTarget();
        whenOperationFailsWithRetriableError();
        whenVerifyIsRun();
        whenOperationFailsWithRetriableError();
        whenVerifyIsRun();
        assertTrue(theNewTargetIsDifferent());
    }

    @Test
    public void shouldUpdateCountWhenNewLocationArrives()
                    throws CacheException, IOException, InterruptedException {
        setUpTest(false);
        givenAFileUpdateForANewFileOnAPoolWithHostAndRackTags();
        givenUpdateHasBeenAddedToMapWithCountOf(1);
        givenANewLocationForFile();
        whenHandleUpdateIsCalled();
        assertTrue(theOperationCountIs(2));
    }

    @Test
    public void shouldUpdateWhenLocationArrivesWithPredefinedGroup()
                    throws CacheException, IOException, InterruptedException {
        setUpTest(false);
        givenAFileUpdateFromAPoolScan();
        whenHandleUpdateIsCalled();
        assertTrue(theOperationCountIs(1));
    }

    @Test
    public void shouldUpdateWhenNewLocationArrivesWithNoLocations()
                    throws CacheException, IOException, InterruptedException {
        setUpTest(false);
        givenAFileUpdateForAFileWithNoLocationsYetInAttributes();
        whenHandleUpdateIsCalled();
        assertTrue(theOperationCountIs(1));
    }

    private void afterInspectingSourceAndTarget() {
        FileOperation operation = fileOperationMap.getOperation(update.pnfsId);
        originalSource = operation.getSource();
        originalTarget = operation.getTarget();
    }

    private void assertThatCorrectMigrationTaskWasCreated() {
        Task inner = task.getMigrationTask();
        assertNotNull(inner);
        assertEquals(inner.getPnfsId(), update.pnfsId);
    }

    private void givenACustodialFileUpdateFromAPoolScan()
                    throws CacheException {
        loadNewFilesOnPoolsWithHostAndRackTags();
        setUpdateWithGroup(aCustodialOnlineFile(), MessageType.POOL_STATUS_DOWN,
                           SelectionAction.NONE);
    }

    private void givenANewLocationForFile() throws CacheException {
        Integer pool = poolInfoMap.getPoolIndex(update.pool);
        Integer group = poolInfoMap.getResilientPoolGroup(pool);
        Collection<Integer> pools = poolInfoMap.getPoolsOfGroup(group);
        for (Integer p : pools) {
            if (p != pool) {
                attributes.getLocations().add(poolInfoMap.getPool(p));
                setUpdate(attributes, MessageType.ADD_CACHE_LOCATION);
                break;
            }
        }
    }

    private void givenANewTagRequirementForFileStorageUnit() {
        Integer unit = poolInfoMap.getStorageUnitIndex(attributes);
        StorageUnitConstraints constraints
                        = poolInfoMap.getStorageUnitConstraints(unit);
        int req = constraints.getRequired();
        storageUnit = poolInfoMap.getGroupName(unit);
        poolInfoMap.setGroupConstraints(storageUnit, req,
                        ImmutableList.of("hostname","rack"));

    }

    private void givenAFileUpdateClearCacheLocationForAFileWithNoLocationsInNamespace()
                    throws CacheException {
        loadNewFilesOnPoolsWithHostAndRackTags();
        setUpdate(aReplicaOnlineFileWithBothTagsButNoLocations(),
                  MessageType.CLEAR_CACHE_LOCATION);
    }

    private void givenAFileUpdateForAFileDeletedFromNamespace()
                    throws CacheException {
        loadNewFilesOnPoolsWithHostAndRackTags();
        setUpdate(aDeletedReplicaOnlineFileWithBothTags(),
                  MessageType.CLEAR_CACHE_LOCATION);
    }

    private void givenAFileUpdateForAFileWithExcessLocations()
                    throws CacheException {
        loadFilesWithExcessLocations();
        setUpdate(aFileWithAReplicaOnAllResilientPools(),
                        MessageType.POOL_STATUS_UP);
    }

    private void givenAFileUpdateForAFileWithNoLocationsYetInAttributes()
                    throws CacheException {
        loadNewFilesOnPoolsWithHostAndRackTags();
        setUpdate(aReplicaOnlineFileWithBothTags(),
                  MessageType.ADD_CACHE_LOCATION);
        attributes.getLocations().clear();
    }

    private void givenAFileUpdateForAFileWithOneLocationOffline()
                    throws CacheException {
        loadFilesWithRequiredLocations();
        setUpdate(aReplicaOnlineFileWithNoTags(), MessageType.POOL_STATUS_DOWN);
        givenSourcePoolIsDown();
    }

    private void givenAFileUpdateForAFileWithRequiredLocations()
                    throws CacheException {
        loadFilesWithRequiredLocations();
        setUpdate(aReplicaOnlineFileWithNoTags(),
                        MessageType.POOL_STATUS_UP);
    }

    private void givenAFileUpdateForANewFileOnAPoolWithHostAndRackTags()
                    throws CacheException {
        loadNewFilesOnPoolsWithNoTags();
        setUpdate(aReplicaOnlineFileWithNoTags(),
                  MessageType.ADD_CACHE_LOCATION);
    }

    private void givenAFileUpdateForANewFileOnAPoolWithNoTags()
                    throws CacheException {
        loadNewFilesOnPoolsWithNoTags();
        setUpdate(aReplicaOnlineFileWithNoTags(),
                  MessageType.ADD_CACHE_LOCATION);
    }

    private void givenAFileUpdateForANewFileOnNonResilientPool()
                    throws CacheException {
        loadNonResilientFiles();
        setUpdate(aCustodialNearlineFile(), MessageType.ADD_CACHE_LOCATION);
    }

    private void givenAFileUpdateForANewFileResilientPoolButRequiringASingleCopy()
                    throws CacheException {
        loadNewFilesOnPoolsWithHostAndRackTags();
        setUpdate(aReplicaOnlineFileWithBothTags(),
                        MessageType.ADD_CACHE_LOCATION);
        String key = attributes.getStorageClass() + "@" + attributes.getHsm();
        makeNonResilient(key);
    }

    private void givenAFileUpdateFromAPoolScan() throws CacheException {
        loadNewFilesOnPoolsWithHostAndRackTags();
        setUpdateWithGroup(aReplicaOnlineFileWithBothTags(),
                           MessageType.POOL_STATUS_DOWN, SelectionAction.NONE);
    }

    private void givenAFileUpdateFromAPoolScanForPoolAddedToGroup()
                    throws CacheException {
        loadNewFilesOnPoolsWithHostAndRackTags();
        setUpdateWithGroup(aReplicaOnlineFileWithBothTags(),
                           MessageType.POOL_STATUS_UP, SelectionAction.ADD);
    }

    private void givenAFileUpdateFromAPoolScanForPoolRemovedFromGroup()
                    throws CacheException {
        loadNewFilesOnPoolsWithHostAndRackTags();
        setUpdateWithGroup(aReplicaOnlineFileWithBothTags(),
                           MessageType.POOL_STATUS_DOWN, SelectionAction.REMOVE);
    }

    private void givenAllPoolsOfflineExceptSourceAndTarget()
                    throws InterruptedException {
        poolInfoMap.getResilientPools().stream().forEach((p) -> {
            Integer index = poolInfoMap.getPoolIndex(p);
            if (index != originalSource && index != originalTarget) {
                shutPoolDown(p);
            }
        });
    }

    private void givenUpdateHasBeenAddedToMapWithCountOf(int count)
                    throws CacheException {
        Integer pool = poolInfoMap.getPoolIndex(update.pool);
        Integer group = update.getGroup();
        if (group == null) {
            group = poolInfoMap.getResilientPoolGroup(pool);
        }
        Integer unit = poolInfoMap.getStorageUnitIndex(attributes);
        update = new FileUpdate(update.pnfsId, update.pool, update.type,
                                pool, group, unit, attributes);
        update.setCount(count);
        fileOperationMap.register(update);
    }

    private void givenSourcePoolIsDown() {
        shutPoolDown(update.pool);
    }

    private boolean noOperationHasBeenAdded() {
        return fileOperationMap.getOperation(update.pnfsId) == null;
    }

    private void setUpTest(boolean remove)
                    throws InterruptedException, CacheException, IOException {
        setUpBase();
        setPoolMessageProcessor(this);
        setShortExecutionMode(Mode.NOP);
        setLongExecutionMode(Mode.NOP);
        if (remove) {
            setScheduledExecutionMode(Mode.RUN);
        } else {
            setScheduledExecutionMode(Mode.NOP);
        }
        createCounters();
        createPoolOperationHandler();
        createPoolOperationMap();
        createFileOperationHandler();
        createFileOperationMap();
        initializeCounters();
        wirePoolOperationMap();
        wirePoolOperationHandler();
        wireFileOperationMap();
        wireFileOperationHandler();
        testNamespaceAccess.setHandler(fileOperationHandler);
        poolOperationMap.setRescanWindow(Integer.MAX_VALUE);
        poolOperationMap.setDownGracePeriod(0);
        poolOperationMap.loadPools();
        fileOperationMap.initialize(()-> {});
        fileOperationMap.reload();
    }

    private void setUpdate(FileAttributes attributes, MessageType type) {
        this.attributes = attributes;
        PnfsId pnfsId = attributes.getPnfsId();
        Iterator<String> iterator = attributes.getLocations().iterator();
        if (iterator.hasNext()) {
            update = new FileUpdate(pnfsId, iterator.next(), type, true);
        } else {
            update = new FileUpdate(pnfsId, null, type, true);
        }
    }

    private void setUpdateWithGroup(FileAttributes attributes, MessageType type,
                    SelectionAction action) {
        this.attributes = attributes;
        PnfsId pnfsId = attributes.getPnfsId();
        Iterator<String> iterator = attributes.getLocations().iterator();
        if (iterator.hasNext()) {
            String pool = iterator.next();
            Integer group = poolInfoMap.getResilientPoolGroup(
                            poolInfoMap.getPoolIndex(pool));
            update = new FileUpdate(pnfsId, pool, type, action, group, false);
        } else {
            update = new FileUpdate(pnfsId, null, type, false);
        }
    }

    private void shutPoolDown(String pool) {
        PoolStateUpdate update = new PoolStateUpdate(pool,
                        new PoolV2Mode(PoolV2Mode.DISABLED_DEAD));
        poolInfoMap.updatePoolStatus(update);
    }

    private boolean theNewSourceIsDifferent() {
        FileOperation operation = fileOperationMap.getOperation(update.pnfsId);
        return operation.getSource() != originalSource;
    }

    private boolean theNewTargetIsDifferent() {
        FileOperation operation = fileOperationMap.getOperation(update.pnfsId);
        return operation.getTarget() != originalTarget;
    }

    private boolean theOperationCountIs(int count) {
        FileOperation operation = fileOperationMap.getOperation(
                        attributes.getPnfsId());
        return operation != null && operation.getOpCount() == count;
    }

    private void whenHandleUpdateIsCalled() throws CacheException {
        fileOperationHandler.handleLocationUpdate(update);
    }

    private void whenOperationFailsFatally() throws IOException {
        fileOperationMap.scan();
        fileOperationMap.updateOperation(update.pnfsId,
                                         new CacheException(CacheException.DEFAULT_ERROR_CODE,
                                        FORCED_FAILURE.toString()));
        fileOperationMap.scan();
    }

    private void whenOperationFailsWithBrokenFileError() throws IOException {
        fileOperationMap.scan();
        fileOperationMap.updateOperation(update.pnfsId,
                                         new CacheException(CacheException.FILE_CORRUPTED,
                                        "broken"));
        fileOperationMap.scan();
    }

    private void whenOperationFailsWithNewTargetError() throws IOException {
        fileOperationMap.scan();
        fileOperationMap.updateOperation(update.pnfsId,
                                         new CacheException(CacheException.FILE_IN_CACHE,
                                        FORCED_FAILURE.toString()));
        fileOperationMap.scan();
    }

    private void whenOperationFailsWithRetriableError() throws IOException {
        fileOperationMap.scan();
        fileOperationMap.updateOperation(update.pnfsId,
                                         new CacheException(
                                                         CacheException.FILE_NOT_IN_REPOSITORY,
                                                         FORCED_FAILURE.toString()));
        fileOperationMap.scan();
    }

    private void whenOperationFailsWithSourceError() throws IOException {
        fileOperationMap.scan();
        fileOperationMap.updateOperation(update.pnfsId,
                                         new CacheException(
                                                         CacheException.FILE_NOT_FOUND,
                                                         "Source pool failed"));
        fileOperationMap.scan();
    }

    private void whenScanIsRun() throws IOException{
        fileOperationMap.scan();
        /*
         *  Also force a save.
         */
        fileOperationMap.runCheckpointNow();
    }

    private void whenSourceAndTargetAreSelected() {
        FileOperation op = fileOperationMap.getOperation(update.pnfsId);
        op.setSource(4);
        op.setTarget(5);
    }

    private void whenTaskIsCreatedAndCalled() {
        task = new ResilientFileTask(update.pnfsId, 0,
                                     fileOperationHandler);
        task.call();
    }

    private void whenVerifyIsRun() {
        verifyType = fileOperationHandler.handleVerification(attributes).name();
    }
}
