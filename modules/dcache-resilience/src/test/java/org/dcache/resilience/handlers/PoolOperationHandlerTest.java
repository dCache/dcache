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

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.CacheException;
import org.dcache.resilience.TestBase;
import org.dcache.resilience.TestSynchronousExecutor.Mode;
import org.dcache.resilience.data.FileFilter;
import org.dcache.resilience.data.PoolStateUpdate;
import org.dcache.resilience.data.PoolStatusForResilience;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PoolOperationHandlerTest extends TestBase {
    String pool;

    @Before
    public void setUp() throws CacheException, InterruptedException {
        setUpBase();
        setShortExecutionMode(Mode.NOP);
        setLongExecutionMode(Mode.RUN);
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
        poolOperationMap.setRestartGracePeriod(0);
        poolOperationMap.loadPools();
        setAllPoolsToEnabled();
    }

    @Test
    public void shouldNotSubmitUpdateWhenPoolIsNotResilient() {
        givenADownStatusChangeFor("standard_pool-0");
        assertNull(poolOperationMap.getState("standard_pool-0"));
    }

    @Test
    public void shouldNotSubmitUpdateWhenReadOnlyIsReceivedOnResilientPool() {
        givenAReadOnlyDownStatusChangeFor("resilient_pool-0");
        assertEquals("IDLE", poolOperationMap.getState(pool));
    }

    @Test
    public void shouldSubmitUpdateWhenUpIsReceivedOnResilientPool() {
        givenADownStatusChangeFor("resilient_pool-0");
        givenAnUpStatusChangeFor("resilient_pool-0");
        assertEquals("WAITING", poolOperationMap.getState(pool));
    }

    @Test
    public void shouldProcess0FilesWhenPoolWithMinReplicasRestarts() {
        givenMinimumReplicasOnPool();
        givenARestartStatusChangeFor(pool);
        whenPoolOpScanIsRun();
        theResultingNumberOfPnfsOperationsSubmittedWas(0);
    }

    @Test
    public void shouldProcess10FilesWhenPoolWithSingleReplicasGoesDown() {
        givenSingleReplicasOnPool();
        givenADownStatusChangeFor(pool);
        whenPoolOpScanIsRun();

        /*
         * 5 REPLICA ONLINE, 5 CUSTODIAL
         * (the inaccessible handler is invoked later, during
         * the verification phase)
         */
        theResultingNumberOfPnfsOperationsSubmittedWas(10);
    }

    @Test
    public void shouldProcess10FilesWhenPoolWithSingleReplicasRestarts() {
        givenSingleReplicasOnPool();
        givenADownStatusChangeFor(pool);
        givenARestartStatusChangeFor(pool);
        whenPoolOpScanIsRun();
        /*
         *  5 REPLICA ONLINE, 5 CUSTODIAL ONLINE
         */
        theResultingNumberOfPnfsOperationsSubmittedWas(10);
    }

    @Test
    public void shouldProcess4FilesWhenPoolWithExcessReplicasDown() {
        givenExcessReplicasOnPool();
        givenADownStatusChangeFor(pool);
        whenPoolOpScanIsRun();

        /*
         * 2 REPLICA ONLINE, 2 CUSTODIAL ONLINE
         */
        theResultingNumberOfPnfsOperationsSubmittedWas(4);
    }

    @Test
    public void shouldProcess4FilesWhenPoolWithMinReplicasDown() {
        givenMinimumReplicasOnPool();
        givenADownStatusChangeFor(pool);
        whenPoolOpScanIsRun();

        /*
         * 2 REPLICA ONLINE, 2 CUSTODIAL ONLINE
         */
        theResultingNumberOfPnfsOperationsSubmittedWas(4);
    }

    @Test
    public void shouldProcess6FilesWhenPoolWithExcessReplicasRestarts() {
        givenExcessReplicasOnPool();
        givenADownStatusChangeFor(pool);
        givenARestartStatusChangeFor(pool);
        whenPoolOpScanIsRun();

        /*
         * 3 REPLICA ONLINE, 3 CUSTODIAL ONLINE
         */
        theResultingNumberOfPnfsOperationsSubmittedWas(6);
    }

    @Test
    public void shouldSubmitUpdateWhenDownIsReceivedOnResilientPool() {
        givenADownStatusChangeFor("resilient_pool-0");
        assertEquals(PoolStatusForResilience.DOWN,
                        poolOperationMap.getCurrentStatus(pool));
        assertEquals("WAITING", poolOperationMap.getState(pool));
    }

    @Test
    public void shouldSubmitUpdateWhenRestartIsReceivedOnResilientPool() {
        givenADownStatusChangeFor("resilient_pool-0");
        givenARestartStatusChangeFor("resilient_pool-0");
        assertEquals(PoolStatusForResilience.ENABLED,
                        poolOperationMap.getCurrentStatus(pool));
        assertEquals("WAITING", poolOperationMap.getState(pool));
    }

    @Test
    public void shouldSubmitUpdateWhenWriteDisabled() {
        givenADisabledStrictStatusChangeFor("resilient_pool-0");
        assertEquals(PoolStatusForResilience.DOWN,
                        poolOperationMap.getCurrentStatus(pool));
        assertEquals("WAITING", poolOperationMap.getState(pool));
    }

    private void givenADownStatusChangeFor(String pool) {
        this.pool = pool;
        PoolStateUpdate update = new PoolStateUpdate(pool,
                        new PoolV2Mode(PoolV2Mode.DISABLED_DEAD));
        poolInfoMap.updatePoolStatus(update);
        poolOperationHandler.handlePoolStatusChange(update);
    }

    private void givenAReadOnlyDownStatusChangeFor(String pool) {
        this.pool = pool;
        PoolStateUpdate update = new PoolStateUpdate(pool,
                        new PoolV2Mode(PoolV2Mode.DISABLED_RDONLY));
        poolInfoMap.updatePoolStatus(update);
        poolOperationHandler.handlePoolStatusChange(update);
    }

    private void givenARestartStatusChangeFor(String pool) {
        this.pool = pool;
        PoolStateUpdate update = new PoolStateUpdate(pool,
                        new PoolV2Mode(PoolV2Mode.ENABLED));
        poolInfoMap.updatePoolStatus(update);
        poolOperationHandler.handlePoolStatusChange(update);
    }

    private void givenADisabledStrictStatusChangeFor(String pool) {
        this.pool = pool;
        PoolStateUpdate update = new PoolStateUpdate(pool,
                        new PoolV2Mode(PoolV2Mode.DISABLED_STRICT));
        poolInfoMap.updatePoolStatus(update);
        poolOperationHandler.handlePoolStatusChange(update);
    }

    private void givenAnUpStatusChangeFor(String pool) {
        this.pool = pool;
        PoolStateUpdate update = new PoolStateUpdate(pool,
                        new PoolV2Mode(PoolV2Mode.DISABLED_RDONLY));
        poolInfoMap.updatePoolStatus(update);
        poolOperationHandler.handlePoolStatusChange(update);
    }

    private void givenExcessReplicasOnPool() {
        loadFilesWithExcessLocations();
        pool = "resilient_pool-5";
    }

    private void givenMinimumReplicasOnPool() {
        loadFilesWithRequiredLocations();
        pool = "resilient_pool-13";
    }

    private void givenSingleReplicasOnPool() {
        loadNewFilesOnPoolsWithHostAndRackTags();
        pool = "resilient_pool-10";
    }

    private void setAllPoolsToEnabled() {
        poolInfoMap.getResilientPools().stream().forEach((p)-> {
            PoolStateUpdate update
                = new PoolStateUpdate(p, new PoolV2Mode(PoolV2Mode.ENABLED));
            poolInfoMap.updatePoolStatus(update);
        });
    }

    private void theResultingNumberOfPnfsOperationsSubmittedWas(int submitted) {
        FileFilter filter = new FileFilter();
        filter.setState(ImmutableSet.of("WAITING"));
        assertEquals(submitted, fileOperationMap.count(filter, new StringBuilder()));
    }

    private void whenPoolOpScanIsRun() {
        poolOperationMap.scan();
    }
}
