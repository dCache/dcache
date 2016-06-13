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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import org.dcache.resilience.TestBase;
import org.dcache.resilience.TestSynchronousExecutor.Mode;
import org.dcache.resilience.handlers.PoolTaskCompletionHandler;
import org.dcache.vehicles.FileAttributes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public final class FileOperationMapTest extends TestBase {
    PnfsId         pnfsId;
    FileAttributes attributes;
    FileOperation  operation;
    File checkpoint = new File("checkpoint");

    @Before
    public void setUp() throws CacheException, InterruptedException {
        setUpBase();
        setMocks();
        createCounters();
        createFileOperationHandler();
        createFileOperationMap();
        poolTaskCompletionHandler = new PoolTaskCompletionHandler();
        poolTaskCompletionHandler.setMap(poolOperationMap);
        wireFileOperationMap();
        wireFileOperationHandler();
        initializeCounters();
        fileOperationMap.initialize(() -> {});
        fileOperationMap.setCopyThreads(1);
    }

    @Test
    public void shouldBehaveLikeCancelAllWhenOperationIsVoided()
                    throws CacheException, IOException {
        givenANewPnfsId();
        afterOperationAdded(3);
        whenScanIsRun();
        whenOperationIsVoided();
        whenScanIsRun();
        assertNull(fileOperationMap.getOperation(operation.getPnfsId()));
    }

    @Test
    public void shouldNotRemoveEntryWhenUpdateFailsOnLastTryButOtherSourceExists()
                    throws CacheException, IOException {
        givenAPnfsIdUpdateFromScan();
        afterOperationAdded(1);
        afterSourceAndTargetAreUpdatedTo(
                        attributes.getLocations().iterator().next(),
                        "resilient_pool-12");
        whenOperationFailsWithRetriableError();
        whenScanIsRun();
        whenOperationFailsWithRetriableError();
        /*
         * Another source exists.  Should not fail terminally.
         */
        whenScanIsRun();
        assertNotNull(fileOperationMap.getOperation(operation.getPnfsId()));
    }

    @Test
    public void shouldNotRemoveEntryWhenUpdateFailsWithRetriableError()
                    throws CacheException, IOException {
        givenANewPnfsId();
        afterOperationAdded(1);
        afterSourceAndTargetAreUpdatedTo(
                        attributes.getLocations().iterator().next(),
                        "resilient_pool-12");
        whenOperationFailsWithRetriableError();
        whenScanIsRun();
        assertNotNull(fileOperationMap.getOperation(operation.getPnfsId()));
    }

    @Test
    public void shouldNotRemoveEntryWhenUpdateSuccessfulButMoreWork()
                    throws CacheException, IOException {
        givenANewPnfsId();
        afterOperationAdded(2);
        afterSourceAndTargetAreUpdatedTo(
                        attributes.getLocations().iterator().next(),
                        "resilient_pool-12");
        whenOperationSucceedsFor(operation.getPnfsId());
        whenScanIsRun();
        assertNotNull(fileOperationMap.getOperation(operation.getPnfsId()));
        assertEquals(1, fileOperationMap.getOperation(
                        operation.getPnfsId()).getOpCount());
    }

    @Test
    public void shouldNotRemoveEntryWhenUpdateSuccessfulButNewRequestArrived()
                    throws CacheException, IOException {
        givenANewPnfsId();
        afterOperationAdded(1);
        afterSourceAndTargetAreUpdatedTo(
                        attributes.getLocations().iterator().next(),
                        "resilient_pool-12");
        whenOperationSucceedsFor(operation.getPnfsId());
        givenAnotherLocationForPnfsId();
        whenScanIsRun();
        assertNotNull(fileOperationMap.getOperation(operation.getPnfsId()));
        assertEquals(1, fileOperationMap.getOperation(
                        operation.getPnfsId()).getOpCount());
    }

    @Test
    public void shouldNotRemoveWhenCancelledOperationHasMoreWork()
                    throws CacheException, IOException {
        givenANewPnfsId();
        afterOperationAdded(3);
        afterSourceAndTargetAreUpdatedTo(
                        attributes.getLocations().iterator().next(),
                        "resilient_pool-12");
        whenScanIsRun();
        whenRunningOperationIsCancelled();
        whenScanIsRun();
        assertNotNull(fileOperationMap.getOperation(operation.getPnfsId()));
        assertEquals(2, fileOperationMap.getOperation(
                        operation.getPnfsId()).getOpCount());
    }

    @Test
    public void shouldNotReorderOperationWhenFailsButMoreTries()
                    throws CacheException, IOException {
        givenANewPnfsId();
        afterOperationAdded(2);
        afterSourceAndTargetAreUpdatedTo(
                        attributes.getLocations().iterator().next(),
                        "resilient_pool-12");
        whenScanIsRun();
        whenOperationFailsWithRetriableError();
        givenASecondPnfsId();
        afterOperationAdded(1);
        whenScanIsRun();
        assertThatOperationIsRunning(pnfsId);
    }

    @Test
    public void shouldRemoveEntryWhenUpdateFailsOnLastTryOfRetriableError()
                    throws CacheException, IOException {
        givenANewPnfsId();
        afterOperationAdded(1);
        afterSourceAndTargetAreUpdatedTo(
                        attributes.getLocations().iterator().next(),
                        "resilient_pool-12");
        whenScanIsRun();
        whenOperationFailsWithRetriableError();
        whenScanIsRun();
        whenOperationFailsWithRetriableError();

        /*
         * This should set retry with new source and target, but
         * there should be no other source when it retries, and a failure
         * should result.
         */
        whenScanIsRun();
        whenVerifyIsRun();
        whenScanIsRun();
        assertNull(fileOperationMap.getOperation(operation.getPnfsId()));
    }

    @Test
    public void shouldRemoveEntryWhenUpdateFailsWithFatalError()
                    throws CacheException, IOException {
        givenANewPnfsId();
        afterOperationAdded(1);
        afterSourceAndTargetAreUpdatedTo(
                        attributes.getLocations().iterator().next(),
                        "resilient_pool-12");
        whenScanIsRun();
        whenOperationFailsWithFatalError();
        whenScanIsRun();
        assertNull(fileOperationMap.getOperation(operation.getPnfsId()));
    }

    @Test
    public void shouldRemoveEntryWhenUpdateSuccessfulAndNoMoreWork()
                    throws CacheException, IOException {
        givenANewPnfsId();
        afterOperationAdded(1);
        afterSourceAndTargetAreUpdatedTo(
                        attributes.getLocations().iterator().next(),
                        "resilient_pool-12");
        whenScanIsRun();
        whenOperationSucceedsFor(operation.getPnfsId());
        whenScanIsRun();
        assertNull(fileOperationMap.getOperation(operation.getPnfsId()));
    }

    @Test
    public void shouldRemoveWhenEntireOperationIsCancelled()
                    throws CacheException, IOException {
        givenANewPnfsId();
        afterOperationAdded(3);
        afterSourceAndTargetAreUpdatedTo(
                        attributes.getLocations().iterator().next(),
                        "resilient_pool-12");
        whenScanIsRun();
        whenEntireOperationIsCancelled();
        whenScanIsRun();
        assertNull(fileOperationMap.getOperation(operation.getPnfsId()));
    }

    @Test
    public void shouldReorderOperationWhenCompletesButMoreWork()
                    throws CacheException, IOException {
        givenANewPnfsId();
        afterOperationAdded(2);
        afterSourceAndTargetAreUpdatedTo(
                        attributes.getLocations().iterator().next(),
                        "resilient_pool-12");
        whenScanIsRun();
        whenOperationSucceedsFor(operation.getPnfsId());
        givenASecondPnfsId();
        afterOperationAdded(1);
        whenScanIsRun();
        assertThatOperationIsNotRunning(pnfsId);
        assertThatOperationIsRunning(operation.getPnfsId());
    }

    @Test
    public void shouldResetEntryWhenUpdateFailsWithNewLocationError()
                    throws CacheException, IOException {
        givenANewPnfsId();
        afterOperationAdded(1);
        afterSourceAndTargetAreUpdatedTo(
                        attributes.getLocations().iterator().next(),
                        "resilient_pool-12");
        whenOperationFailsWithNewLocationError();
        whenScanIsRun();
        assertNotNull(fileOperationMap.getOperation(operation.getPnfsId()));
        assertEquals(0, fileOperationMap.getOperation(
                        operation.getPnfsId()).getRetried());
    }

    @Test
    public void shouldSaveAndRestoreCheckpointedOperation()
                    throws CacheException, IOException {
        givenANewPnfsId();
        afterOperationAdded(3);
        whenSaveIsCalled();
        whenLoadIsCalled();
        assertNotNull(fileOperationMap.getOperation(operation.getPnfsId()));
    }

    @After
    public void tearDown() {
        if (checkpoint.exists()) {
            checkpoint.delete();
        }
    }

    private void afterOperationAdded(int count) throws CacheException {
        PnfsId pnfsId = attributes.getPnfsId();
        String pool = attributes.getLocations().iterator().next();
        String unit = attributes.getStorageClass() + "@" + attributes.getHsm();
        Integer pindex = poolInfoMap.getPoolIndex(pool);
        Integer gindex = poolInfoMap.getResilientPoolGroup(pindex);
        Integer sindex = poolInfoMap.getGroupIndex(unit);
        FileUpdate update = new FileUpdate(pnfsId, pool,
                                           MessageType.ADD_CACHE_LOCATION, pindex, gindex, sindex,
                                           attributes);
        update.setCount(count);
        fileOperationMap.register(update);
        operation = new FileOperation(
                        fileOperationMap.getOperation(attributes.getPnfsId()));
    }

    private void afterSourceAndTargetAreUpdatedTo(String source,
                    String target) {
        fileOperationMap.updateOperation(attributes.getPnfsId(), source,
                                         target);
    }

    private void assertThatOperationIsNotRunning(PnfsId pnfsId) {
        assertNotEquals(pnfsId, fileOperationMap.running.peek().getPnfsId());
    }

    private void assertThatOperationIsRunning(PnfsId pnfsId) {
        assertEquals(pnfsId, fileOperationMap.running.peek().getPnfsId());
    }

    private void givenANewPnfsId() throws CacheException {
        loadNewFilesOnPoolsWithNoTags();
        attributes = aReplicaOnlineFileWithNoTags();
    }

    private void givenAPnfsIdUpdateFromScan() throws CacheException {
        loadFilesWithRequiredLocations();
        attributes = aReplicaOnlineFileWithNoTags();
    }

    private void givenASecondPnfsId() throws CacheException {
        pnfsId = attributes.getPnfsId();
        attributes = aReplicaOnlineFileWithHostTag();
    }

    private void givenAnotherLocationForPnfsId() {
        fileOperationMap.updateCount(operation.getPnfsId());
    }

    private void setMocks() {
        setShortExecutionMode(Mode.NOP);
        setLongExecutionMode(Mode.NOP);
    }

    private void whenEntireOperationIsCancelled() {
        fileOperationMap.cancel(operation.getPnfsId(), true);
    }

    private void whenLoadIsCalled() throws IOException {
        fileOperationMap.reload();
    }

    private void whenOperationFailsWithFatalError() {
        fileOperationMap.updateOperation(operation.getPnfsId(),
                                         new CacheException(CacheException.DEFAULT_ERROR_CODE,
                                        FORCED_FAILURE.toString()));
    }

    private void whenOperationFailsWithNewLocationError() {
        fileOperationMap.updateOperation(operation.getPnfsId(),
                                         new CacheException(CacheException.FILE_NOT_FOUND,
                                        FORCED_FAILURE.toString()));
    }

    private void whenOperationFailsWithRetriableError() {
        fileOperationMap.updateOperation(operation.getPnfsId(),
                                         new CacheException(CacheException.HSM_DELAY_ERROR,
                                        FORCED_FAILURE.toString()));
    }

    private void whenOperationIsVoided() {
        fileOperationMap.voidOperation(operation.getPnfsId());
    }

    private void whenOperationSucceedsFor(PnfsId pnfsId) {
        /*
         *  Simulate previous launch without doing a full scan.
         */
        fileOperationMap.updateOperation(pnfsId, null);
    }

    private void whenRunningOperationIsCancelled() {
        fileOperationMap.cancel(operation.getPnfsId(), false);
    }

    private void whenSaveIsCalled() throws IOException {
        fileOperationMap.setCheckpointFilePath(checkpoint.getAbsolutePath());
        fileOperationMap.checkpointer.save();
    }

    private void whenScanIsRun() throws IOException {
        fileOperationMap.scan();
    }

    private void whenVerifyIsRun() throws IOException {
        fileOperationMap.getOperation(operation.getPnfsId()).getTask().call();
    }
}
