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

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import org.dcache.resilience.TestBase;
import org.dcache.resilience.TestSynchronousExecutor.Mode;
import org.dcache.util.CacheExceptionFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PoolOperationMapTest extends TestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(
                    PoolOperationMapTest.class);

    String pool;
    int numberOfPools;
    int children;

    @Before
    public void setUp() throws CacheException, InterruptedException {
        setUpBase();
        setShortExecutionMode(Mode.NOP);
        setLongExecutionMode(Mode.NOP);
        createCounters();
        createPoolOperationHandler();
        createPoolOperationMap();
        wirePoolOperationMap();
        wirePoolOperationHandler();
        initializeCounters();
        poolOperationMap.setRestartGracePeriod(0);
        poolOperationMap.loadPools();
        numberOfPools = poolOperationMap.idle.size();

        poolInfoMap.getResilientPools().stream()
                   .forEach((p) -> {
                       PoolV2Mode mode = new PoolV2Mode(PoolV2Mode.ENABLED);
                       poolOperationMap.update(new PoolStateUpdate(p, mode));
                   });
    }

    @Test
    public void shouldContinueToRunMaxConcurrentWaitingOperationsUntilAllComplete() {
        givenRescanWindowInHoursIs(0);

        while (!poolOperationMap.waiting.isEmpty()) {
            whenQueuesAreScanned();
            assertTrue(poolOperationMap.getMaxConcurrentRunning()
                            >= poolOperationMap.running.size());
            assertEquals(numberOfPools
                                            - poolOperationMap.getMaxConcurrentRunning(),
                            poolOperationMap.waiting.size()
                                            + poolOperationMap.idle.size());
            whenNextOperationCompletes();
        }

        assertTrue(poolOperationMap.running.isEmpty());
        assertTrue(poolOperationMap.waiting.isEmpty());
        assertEquals(numberOfPools, poolOperationMap.idle.size());
    }

    @Test
    public void shouldDemoteRunningToIdleWhenCancelled() {
        givenDownGracePeriodInMinutesIs(0);
        givenRescanWindowInHoursIs(10);
        givenPoolIsDown("resilient_pool-3");
        whenQueuesAreScanned();
        assertTrue(poolOperationMap.running.containsKey(pool));
        assertFalse(poolOperationMap.idle.containsKey(pool));
        whenOperationIsCancelled();
        assertFalse(poolOperationMap.running.containsKey(pool));
        assertTrue(poolOperationMap.idle.containsKey(pool));
    }

    @Test
    public void shouldDemoteWaitingToIdleWhenCancelled() {
        givenDownGracePeriodInMinutesIs(10);
        givenRescanWindowInHoursIs(10);
        givenPoolIsDown("resilient_pool-3");
        whenQueuesAreScanned();
        assertTrue(poolOperationMap.waiting.containsKey(pool));
        assertFalse(poolOperationMap.idle.containsKey(pool));
        whenOperationIsCancelled();
        assertFalse(poolOperationMap.waiting.containsKey(pool));
        assertTrue(poolOperationMap.idle.containsKey(pool));
    }

    @Test
    public void shouldContinueWaitingWhenDownUpdatedToUp() {
        givenRestartsAreNotHandled();
        givenPoolIsDown("resilient_pool-3");
        givenPoolIsUp(pool);
        assertTrue(poolOperationMap.waiting.containsKey(pool));
        assertEquals(PoolStatusForResilience.READ_ONLY,
                        poolOperationMap.waiting.get(pool).currStatus);
    }

    @Test
    public void shouldNotDemoteWaitingToIdleWhenDownUpdatedToUpAndRestartsAreHandled() {
        givenPoolIsDown("resilient_pool-3");
        givenPoolIsUp(pool);
        assertTrue(poolOperationMap.waiting.containsKey(pool));
        assertFalse(poolOperationMap.idle.containsKey(pool));
        assertEquals(PoolStatusForResilience.READ_ONLY,
                     poolOperationMap.waiting.get(pool).currStatus);
    }

    @Test
    public void shouldDoNothingWhenUpdatedToNOP() {
        givenPoolIsDown("resilient_pool-3");
        givenPoolIsDown(pool);
        assertTrue(poolOperationMap.waiting.containsKey(pool));
        assertEquals(PoolStatusForResilience.DOWN,
                        poolOperationMap.waiting.get(pool).currStatus);
        assertFalse(poolOperationMap.idle.containsKey(pool));
    }

    @Test
    public void shouldDoNothingWhenUpdatedToUpFromUninitialized() {
        givenPoolIsUp("resilient_pool-3");
        assertTrue(poolOperationMap.idle.containsKey(pool));
        assertFalse(poolOperationMap.waiting.containsKey(pool));
    }

    @Test
    public void shouldFailToScanBecauseAlreadyRunning() {
        givenRescanWindowInHoursIs(10);
        givenPoolIsDown("resilient_pool-3");
        givenPoolIsRestarted("resilient_pool-3");
        whenQueuesAreScanned();
        assertFalse(givenForcedScanOf(pool));
        assertTrue(poolOperationMap.running.containsKey(pool));
    }

    @Test
    public void shouldFailBecauseAlreadyWaiting() {
        givenRescanWindowInHoursIs(10);
        givenPoolIsDown("resilient_pool-3");
        givenPoolIsRestarted("resilient_pool-3");
        assertFalse(givenForcedScanOf(pool));
        assertTrue(poolOperationMap.waiting.containsKey(pool));
    }

    @Test
    public void shouldForceOperationToWaitingState() {
        assertTrue(givenForcedScanOf("resilient_pool-1"));
        assertTrue(givenForcedScanOf("resilient_pool-2"));
        assertTrue(givenForcedScanOf("resilient_pool-3"));
        assertTrue(givenForcedScanOf("resilient_pool-4"));
    }

    @Test
    public void shouldNotPromoteIdleToWaitingWhenScannedAndRescanWindowNotExpired() {
        givenRestartsAreNotHandled();
        givenRescanWindowInHoursIs(10);
        whenQueuesAreScanned();
        assertTrue(poolOperationMap.waiting.isEmpty());
        assertEquals(numberOfPools, poolOperationMap.idle.size());
    }

    @Test
    public void shouldNotPromoteWaitingToRunningWhenScannedAndGracePeriodNotExpired() {
        givenDownGracePeriodInMinutesIs(10);
        givenRescanWindowInHoursIs(1);
        givenPoolIsDown("resilient_pool-3");
        whenQueuesAreScanned();
        assertFalse(poolOperationMap.running.containsKey(pool));
        assertTrue(poolOperationMap.waiting.containsKey(pool));
    }

    @Test
    public void shouldPromoteIdleToWaitingWhenScannedAndRescanHandled() {
        givenRestartsAreNotHandled();
        givenRescanWindowInHoursIs(0);
        whenQueuesAreScanned();
        assertTrue(poolOperationMap.idle.isEmpty());
        assertEquals(poolOperationMap.getMaxConcurrentRunning(),
                     poolOperationMap.running.size());
        assertEquals(numberOfPools - poolOperationMap.getMaxConcurrentRunning(),
                     poolOperationMap.waiting.size());
    }

    @Test
    public void shouldPromoteIdleToWaitingWhenUpdatedToDown() {
        givenPoolIsDown("resilient_pool-3");
        assertTrue(poolOperationMap.waiting.containsKey(pool));
        assertEquals(PoolStatusForResilience.DOWN,
                        poolOperationMap.waiting.get(pool).currStatus);
        assertFalse(poolOperationMap.idle.containsKey(pool));
    }

    @Test
    public void shouldPromoteIdleToWaitingWhenUpdatedToRestart() {
        givenPoolIsDown("resilient_pool-3");
        givenPoolIsRestarted("resilient_pool-3");
        assertTrue(poolOperationMap.waiting.containsKey(pool));
        assertEquals(PoolStatusForResilience.ENABLED,
                     poolOperationMap.waiting.get(pool).currStatus);
        assertFalse(poolOperationMap.idle.containsKey(pool));
    }

    @Test
    public void shouldPromoteWaitingToRunningWhenScannedAndGracePeriodExpired() {
        givenDownGracePeriodInMinutesIs(0);
        givenRescanWindowInHoursIs(1);
        givenPoolIsDown("resilient_pool-3");
        whenQueuesAreScanned();
        assertTrue(poolOperationMap.running.containsKey(pool));
        assertFalse(poolOperationMap.waiting.containsKey(pool));
    }

    @Test
    public void shouldRemainRunningWhenOperationCompletesButChildrenNotTerminated() {
        givenRescanWindowInHoursIs(10);
        givenPoolIsDown("resilient_pool-3");
        givenPoolIsRestarted("resilient_pool-3");
        givenFileCountOfPoolIs(4);
        whenQueuesAreScanned();
        assertTrue(poolOperationMap.running.containsKey(pool));
        assertFalse(poolOperationMap.idle.containsKey(pool));
        whenOperationCompletes();
        assertTrue(poolOperationMap.running.containsKey(pool));
        assertFalse(poolOperationMap.idle.containsKey(pool));
    }

    @Test
    public void shouldRemainRunningWhenOperationFailsButChildrenNotTerminated() {
        givenRescanWindowInHoursIs(10);
        givenPoolIsDown("resilient_pool-3");
        givenPoolIsRestarted("resilient_pool-3");
        givenFileCountOfPoolIs(4);
        whenQueuesAreScanned();
        assertTrue(poolOperationMap.running.containsKey(pool));
        assertFalse(poolOperationMap.idle.containsKey(pool));
        whenOperationFails();
        assertTrue(poolOperationMap.running.containsKey(pool));
        assertFalse(poolOperationMap.idle.containsKey(pool));
    }

    @Test
    public void shouldReturnToIdleWhenOperationFailsAndChildrenComplete() {
        givenRescanWindowInHoursIs(10);
        givenPoolIsDown("resilient_pool-3");
        givenPoolIsRestarted("resilient_pool-3");
        givenFileCountOfPoolIs(4);
        whenQueuesAreScanned();
        assertTrue(poolOperationMap.running.containsKey(pool));
        assertFalse(poolOperationMap.idle.containsKey(pool));
        whenOperationFails();
        whenAllChildrenCompleteFor("resilient_pool-3");
        assertFalse(poolOperationMap.running.containsKey(pool));
        assertTrue(poolOperationMap.idle.containsKey(pool));
    }

    @Test
    public void shouldReturnToIdleWhenOperationFailsAndNoChildren() {
        givenRescanWindowInHoursIs(10);
        givenPoolIsDown("resilient_pool-3");
        givenPoolIsRestarted("resilient_pool-3");
        givenFileCountOfPoolIs(0);
        whenQueuesAreScanned();
        assertTrue(poolOperationMap.running.containsKey(pool));
        assertFalse(poolOperationMap.idle.containsKey(pool));
        whenOperationFails();
        assertFalse(poolOperationMap.running.containsKey(pool));
        assertTrue(poolOperationMap.idle.containsKey(pool));
    }

    @Test
    public void shouldReturnToIdleWhenOperationSucceedsAndChildrenComplete() {
        givenRescanWindowInHoursIs(10);
        givenPoolIsDown("resilient_pool-3");
        givenPoolIsRestarted("resilient_pool-3");
        givenFileCountOfPoolIs(4);
        whenQueuesAreScanned();
        assertTrue(poolOperationMap.running.containsKey(pool));
        assertFalse(poolOperationMap.idle.containsKey(pool));
        whenOperationCompletes();
        whenAllChildrenCompleteFor("resilient_pool-3");
        assertFalse(poolOperationMap.running.containsKey(pool));
        assertTrue(poolOperationMap.idle.containsKey(pool));
    }

    @Test
    public void shouldReturnToIdleWhenOperationSucceedsNoChildren() {
        givenRescanWindowInHoursIs(10);
        givenPoolIsDown("resilient_pool-3");
        givenPoolIsRestarted("resilient_pool-3");
        givenFileCountOfPoolIs(0);
        whenQueuesAreScanned();
        assertTrue(poolOperationMap.running.containsKey(pool));
        assertFalse(poolOperationMap.idle.containsKey(pool));
        whenOperationCompletes();
        assertFalse(poolOperationMap.running.containsKey(pool));
        assertTrue(poolOperationMap.idle.containsKey(pool));
    }

    @Test
    public void shouldRunOnlyMaxConcurrentWaitingOperations() {
        givenRescanWindowInHoursIs(0);
        whenQueuesAreScanned();
        assertEquals(poolOperationMap.getMaxConcurrentRunning(),
                        poolOperationMap.running.size());
        assertEquals(numberOfPools - poolOperationMap.getMaxConcurrentRunning(),
                        poolOperationMap.waiting.size());
        assertTrue(poolOperationMap.idle.isEmpty());
    }

    private void givenDownGracePeriodInMinutesIs(int minutes) {
        poolOperationMap.setDownGracePeriod(minutes);
    }

    private boolean givenForcedScanOf(String pool) {
        return poolOperationMap.scan(poolInfoMap.getPoolState(pool));
    }

    private void givenFileCountOfPoolIs(int count) {
        children = count;
    }

    private void givenPoolIsDown(String pool) {
        this.pool = pool;
        PoolStateUpdate update = new PoolStateUpdate(pool,
                        new PoolV2Mode(PoolV2Mode.DISABLED_DEAD));
        poolOperationMap.update(update);
    }

    private void givenPoolIsRestarted(String pool) {
        this.pool = pool;
        PoolStateUpdate update = new PoolStateUpdate(pool,
                        new PoolV2Mode(PoolV2Mode.ENABLED));
        poolOperationMap.update(update);
    }

    private void givenPoolIsUp(String pool) {
        this.pool = pool;
        PoolStateUpdate update = new PoolStateUpdate(pool,
                        new PoolV2Mode(PoolV2Mode.DISABLED_RDONLY));
        poolOperationMap.update(update);
    }

    private void givenRescanWindowInHoursIs(int hours) {
        poolOperationMap.setRescanWindow(hours);
    }

    private void givenRestartsAreNotHandled() {
        poolOperationMap.setRestartGracePeriod(Integer.MAX_VALUE);
    }

    private void whenAllChildrenCompleteFor(String pool) {
        for (int c = 0; c < children; c++) {
            poolOperationMap.update(pool, (PnfsId) null);
        }
    }

    private void whenNextOperationCompletes() {
        Iterator<String> iterator = poolOperationMap.running.keySet().iterator();
        if (iterator.hasNext()) {
            String pool = iterator.next();
            poolOperationMap.update(pool, children);
        }
    }

    private void whenOperationCompletes() {
        poolOperationMap.update(pool, children);
    }

    private void whenOperationFails() {
        poolOperationMap.update(pool, children,
                        CacheExceptionFactory.exceptionOf(
                                        CacheException.NO_POOL_ONLINE,
                                        "Cannot reach pool."));
    }

    private void whenOperationIsCancelled() {
        PoolFilter filter = new PoolFilter();
        filter.setPools(pool);
        poolOperationMap.cancel(filter);
    }

    private void whenQueuesAreScanned() {
        poolOperationMap.scan();
    }

}
