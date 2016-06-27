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

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import org.dcache.resilience.TestBase;
import org.dcache.resilience.data.PoolOperation.State;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class PoolFilterTest extends TestBase {
    PoolFilter    filter;
    PoolOperation poolOperation;
    FileOperation fileOperation;
    String        pool;

    @Before
    public void setUp() throws CacheException {
        setUpBase();
        loadNewFilesOnPoolsWithNoTags();
    }

    @Test
    public void shouldMatchFileOperationWhenParentMatchesParentPool()
                    throws CacheException {
        givenFilterWith("resilient_pool-0", null, null, true, false, false, null, null,
                        null, null);
        givenFileOperationWith("resilient_pool-0", null, null);
        assertTrue(filter.matches(fileOperation, poolInfoMap));
    }

    @Test
    public void shouldMatchFileOperationWhenSourceMatchesSourcePool()
                    throws CacheException {
        givenFilterWith("resilient_pool-0", null, null, false, true, false, null, null,
                        null, null);
        givenFileOperationWith(null, "resilient_pool-0", null);
        assertTrue(filter.matches(fileOperation, poolInfoMap));
    }

    @Test
    public void shouldMatchFileOperationWhenTargetMatchesTargetPool()
                    throws CacheException {
        givenFilterWith("resilient_pool-0", null, null, false, false, true, null, null,
                        null, null);
        givenFileOperationWith(null, null, "resilient_pool-0");
        assertTrue(filter.matches(fileOperation, poolInfoMap));
    }

    @Test
    public void shouldNotMatchFileOperationWhenParentDoesNotMatchParentPool()
                    throws CacheException {
        givenFilterWith("resilient_pool-0", null, null, true, false, false, null, null,
                        null, null);
        givenFileOperationWith(null, null, null);
        assertFalse(filter.matches(fileOperation, poolInfoMap));
    }

    @Test
    public void shouldNotMatchFileOperationWhenSourceDoesNotMatchSourcePool()
                    throws CacheException {
        givenFilterWith("resilient_pool-0", null, null, false, true, false, null, null,
                        null, null);
        givenFileOperationWith(null, null, null);
        assertFalse(filter.matches(fileOperation, poolInfoMap));
    }

    @Test
    public void shouldNotMatchFileOperationWhenTargetDoesNotMatchTargetPool()
                    throws CacheException {
        givenFilterWith("resilient_pool-0", null, null, false, false, true, null, null,
                        null, null);
        givenFileOperationWith(null, null, null);
        assertFalse(filter.matches(fileOperation, poolInfoMap));
    }

    @Test
    public void shouldMatchPoolOperationWhenAfterScanMatches() {
        givenFilterWith(null, null, null, false, false, false, null, null,
                        System.currentTimeMillis(), null);
        givenPoolOperationWith(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1),
                               System.currentTimeMillis(), "DOWN", "WAITING");
        assertTrue(filter.matches("resilient_pool-0", poolOperation));
    }

    @Test
    public void shouldMatchPoolOperationWhenAfterUpdateMatches() {
        givenFilterWith(null, null, null, false, false, false,
                        System.currentTimeMillis(), null, null, null);
        givenPoolOperationWith(System.currentTimeMillis(),
                               System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1),
                               "DOWN", "WAITING");
        assertTrue(filter.matches("resilient_pool-0", poolOperation));
    }

    @Test
    public void shouldMatchPoolOperationWhenBeforeScanMatches() {
        givenFilterWith(null, null, null, false, false, false, null, null,
                        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1),
                        null);
        givenPoolOperationWith(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2),
                               System.currentTimeMillis(), "DOWN", "WAITING");
        assertTrue(filter.matches("resilient_pool-0", poolOperation));
    }

    @Test
    public void shouldMatchPoolOperationWhenBeforeUpdateMatches() {
        givenFilterWith(null, null, null, false, false, false,
                        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1), null,
                        null, null);
        givenPoolOperationWith(System.currentTimeMillis(),
                               System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2),
                               "DOWN", "WAITING");
        assertTrue(filter.matches("resilient_pool-0", poolOperation));
    }

    @Test
    public void shouldMatchPoolOperationWhenPoolMatches() {
        givenFilterWith("resilient_pool-0", null, null, false, false, false, null, null,
                        null, null);
        givenPoolOperationWith(System.currentTimeMillis(),
                               System.currentTimeMillis(), "DOWN", "WAITING");
        assertTrue(filter.matches("resilient_pool-0", poolOperation));
    }

    @Test
    public void shouldMatchPoolOperationWhenPoolStateMatches() {
        givenFilterWith(null, "WAITING,RUNNING", null, false, false, false,
                        null, null, null, null);
        givenPoolOperationWith(System.currentTimeMillis(),
                        System.currentTimeMillis(), "DOWN", "WAITING");
        assertTrue(filter.matches("resilient_pool-0", poolOperation));
    }

    @Test
    public void shouldMatchPoolOperationWhenPoolStatusMatches() {
        givenFilterWith(null, null, "DOWN", false, false, false, null, null,
                        null, null);
        givenPoolOperationWith(System.currentTimeMillis(),
                        System.currentTimeMillis(), "DOWN", "WAITING");
        assertTrue(filter.matches("resilient_pool-0", poolOperation));
    }

    @Test
    public void shouldNotMatchPoolOperationWhenAfterScanDoesNotMatch() {
        givenFilterWith(null, null, "RESTART", false, false, false, null, null,
                        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1),
                        null);
        givenPoolOperationWith(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2),
                        System.currentTimeMillis(), "DOWN", "WAITING");
        assertFalse(filter.matches("resilient_pool-0", poolOperation));
    }

    @Test
    public void shouldNotMatchPoolOperationWhenAfterUpdateDoesNotMatch() {
        givenFilterWith(null, null, "RESTART", false, false, false,
                        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1),
                        null, null, null);
        givenPoolOperationWith(System.currentTimeMillis(),
                        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2),
                        "DOWN", "WAITING");
        assertFalse(filter.matches("resilient_pool-0", poolOperation));
    }

    @Test
    public void shouldNotMatchPoolOperationWhenBeforeScanDoesNotMatch() {
        givenFilterWith(null, null, "RESTART", false, false, false, null, null,
                        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2),
                        null);
        givenPoolOperationWith(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1),
                        System.currentTimeMillis(), "DOWN", "WAITING");
        assertFalse(filter.matches("resilient_pool-0", poolOperation));
    }

    @Test
    public void shouldNotMatchPoolOperationWhenBeforeUpdateDoesNotMatch() {
        givenFilterWith(null, null, "RESTART", false, false, false,
                        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2),
                        null, null, null);
        givenPoolOperationWith(System.currentTimeMillis(),
                        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1),
                        "DOWN", "WAITING");
        assertFalse(filter.matches("resilient_pool-0", poolOperation));
    }

    @Test
    public void shouldNotMatchPoolOperationWhenPoolDoesNotMatch() {
        givenFilterWith("pool2", null, null, false, false, false, null, null,
                        null, null);
        givenPoolOperationWith(System.currentTimeMillis(),
                        System.currentTimeMillis(), "DOWN", "WAITING");
        assertFalse(filter.matches("resilient_pool-0", poolOperation));
    }

    @Test
    public void shouldNotMatchPoolOperationWhenPoolStateDoesNotMatch() {
        givenFilterWith(null, "WAITING,RUNNING", null, false, false, false,
                        null, null, null, null);
        givenPoolOperationWith(System.currentTimeMillis(),
                        System.currentTimeMillis(), null, "IDLE");
        assertFalse(filter.matches("resilient_pool-0", poolOperation));
    }

    @Test
    public void shouldNotMatchPoolOperationWhenPoolStatusDoesNotMatch() {
        givenFilterWith(null, null, "RESTART", false, false, false, null, null,
                        null, null);
        givenPoolOperationWith(System.currentTimeMillis(),
                        System.currentTimeMillis(), "DOWN", "WAITING");
        assertFalse(filter.matches("resilient_pool-0", poolOperation));
    }

    private void givenFilterWith(String pool, String states, String status,
                    boolean parent, boolean source, boolean target,
                    Long updateBefore, Long updateAfter, Long scanBefore,
                    Long scanAfter) {
        filter = new PoolFilter();
        filter.setLastUpdateBefore(updateBefore);
        filter.setLastUpdateAfter(updateAfter);
        filter.setLastScanBefore(scanBefore);
        filter.setLastScanAfter(scanAfter);
        filter.setParent(parent);
        filter.setPools(pool);
        filter.setPoolStatus(status);
        filter.setSource(source);
        filter.setTarget(target);
        if (states != null) {
            filter.setState(ImmutableSet.copyOf(states.split("[,]")));
        }
    }

    private void givenFileOperationWith(String parent, String source, String target)
                    throws CacheException {
        fileOperation = new FileOperation(aReplicaOnlineFileWithNoTags().getPnfsId(), 1, 2L);

        Integer p = parent != null ? poolInfoMap.getPoolIndex(parent) : null;
        Integer s = source != null ? poolInfoMap.getPoolIndex(source) : null;
        Integer t = target != null ? poolInfoMap.getPoolIndex(target) : null;

        fileOperation.setParentOrSource(p, p != null);
        fileOperation.setSource(s);
        fileOperation.setTarget(t);
    }

    private void givenPoolOperationWith(Long lastScan, Long lastUpdate,
                                        String status, String state) {
        poolOperation = new PoolOperation();
        poolOperation.lastScan = lastScan;
        poolOperation.lastUpdate = lastUpdate;

        if (status != null) {
            poolOperation.currStatus = PoolStatusForResilience.valueOf(status);
        }

        if (state != null) {
            poolOperation.state = State.valueOf(state);
        }
    }
}
