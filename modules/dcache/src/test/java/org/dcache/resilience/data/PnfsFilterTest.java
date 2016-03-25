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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import org.dcache.resilience.TestBase;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class PnfsFilterTest extends TestBase {
    PnfsFilter filter;
    PnfsOperation operation;

    @Before
    public void setUp() throws CacheException {
        setUpBase();
        loadNewFilesOnPoolsWithNoTags();
        loadNonResilientFiles();
    }

    @Test
    public void shouldIndicateSimplePnfsMatch() throws Exception {
        String pnfsids = aReplicaOnlineFileWithNoTags().getPnfsId().toString();
        givenFilterWith(pnfsids, null, null, null, null, null, false);
        assertTrue(filter.isSimplePnfsMatch());
    }

    @Test
    public void shouldIndicateSimplePnfsMatchNotwithstandingOtherAttributes()
                    throws Exception {
        String pnfsids = aReplicaOnlineFileWithNoTags().getPnfsId().toString();
        givenFilterWith(pnfsids, "RUNNING", null, null, null, null, false);
        assertTrue(filter.isSimplePnfsMatch());
    }

    @Test
    public void shouldMatchWhenAfterMatches() throws Exception {
        givenFilterWith(null, null, null, null, System.currentTimeMillis(),
                        null, false);
        givenOperationWith(aReplicaOnlineFileWithNoTags().getPnfsId(),
                        "REPLICA", "resilient-2.dcache-devel-test@enstore",
                        "RUNNING", System.currentTimeMillis()
                                        + TimeUnit.HOURS.toMillis(1));
        assertTrue(filter.matches(operation, poolInfoMap));
    }

    @Test
    public void shouldMatchWhenBeforeMatches() throws Exception {
        givenFilterWith(null, null, null, null, null,
                        System.currentTimeMillis(), false);
        givenOperationWith(aReplicaOnlineFileWithNoTags().getPnfsId(),
                        "REPLICA", "resilient-2.dcache-devel-test@enstore",
                        "RUNNING", System.currentTimeMillis()
                                        - TimeUnit.HOURS.toMillis(1));
        assertTrue(filter.matches(operation, poolInfoMap));
    }

    @Test
    public void shouldMatchWhenMultipleAttributesMatch() throws Exception {
        String pnfsids = Joiner.on(",").join(
                        aReplicaOnlineFileWithNoTags().getPnfsId().toString(),
                        aNonResilientFile().getPnfsId().toString());
        givenFilterWith(pnfsids, "WAITING", null,
                        "resilient-2.dcache-devel-test@enstore", null, null,
                        false);
        givenOperationWith(aReplicaOnlineFileWithNoTags().getPnfsId(),
                        "REPLICA", "resilient-2.dcache-devel-test@enstore",
                        "WAITING", System.currentTimeMillis());
        assertTrue(filter.matches(operation, poolInfoMap));
    }

    @Test
    public void shouldMatchWhenPolicyMatches() throws Exception {
        givenFilterWith(null, null, "REPLICA", null, null, null, false);
        givenOperationWith(aReplicaOnlineFileWithNoTags().getPnfsId(),
                        "REPLICA", "resilient-2.dcache-devel-test@enstore",
                        "RUNNING", System.currentTimeMillis());
        assertTrue(filter.matches(operation, poolInfoMap));
    }

    @Test
    public void shouldMatchWhenSpecifiedPnfsIdMatches() throws Exception {
        String pnfsids = Joiner.on(",").join(
                        aReplicaOnlineFileWithNoTags().getPnfsId().toString(),
                        aNonResilientFile().getPnfsId().toString());
        givenFilterWith(pnfsids, null, null, null, null, null, false);
        givenOperationWith(aReplicaOnlineFileWithNoTags().getPnfsId(),
                        "REPLICA", "resilient-2.dcache-devel-test@enstore",
                        "RUNNING", System.currentTimeMillis());
        assertTrue(filter.matches(operation, poolInfoMap));
    }

    @Test
    public void shouldMatchWhenStatesMatch() throws Exception {
        givenFilterWith(null, "RUNNING", null, null, null, null, false);
        givenOperationWith(aReplicaOnlineFileWithNoTags().getPnfsId(),
                        "REPLICA", null, "RUNNING", System.currentTimeMillis());
        assertTrue(filter.matches(operation, poolInfoMap));
    }

    @Test
    public void shouldMatchWhenStorageUnitMatches() throws Exception {
        givenFilterWith(null, null, null,
                        "resilient-2.dcache-devel-test@enstore", null, null,
                        false);
        givenOperationWith(aReplicaOnlineFileWithNoTags().getPnfsId(),
                        "REPLICA", "resilient-2.dcache-devel-test@enstore",
                        "RUNNING", System.currentTimeMillis());
        assertTrue(filter.matches(operation, poolInfoMap));
    }

    @Test
    public void shouldNotIndicateSimplePnfsMatchBecauseOfMultiplePnfsIds()
                    throws Exception {
        String pnfsids = Joiner.on(",").join(
                        aReplicaOnlineFileWithNoTags().getPnfsId().toString(),
                        aNonResilientFile().getPnfsId().toString());
        givenFilterWith(pnfsids, null, null, null, null, null, false);
        assertFalse(filter.isSimplePnfsMatch());
    }

    @Test
    public void shouldNotMatchWhenAfterDoesNotMatch() throws Exception {
        givenFilterWith(null, null, null, null, System.currentTimeMillis(),
                        null, false);
        givenOperationWith(aReplicaOnlineFileWithNoTags().getPnfsId(),
                        "REPLICA", "resilient-2.dcache-devel-test@enstore",
                        "RUNNING", System.currentTimeMillis()
                                        - TimeUnit.HOURS.toMillis(1));
        assertFalse(filter.matches(operation, poolInfoMap));
    }

    @Test
    public void shouldNotMatchWhenBeforeDoesNotMatch() throws Exception {
        givenFilterWith(null, null, null, null, null,
                        System.currentTimeMillis(), false);
        givenOperationWith(aReplicaOnlineFileWithNoTags().getPnfsId(),
                        "REPLICA", "resilient-2.dcache-devel-test@enstore",
                        "RUNNING", System.currentTimeMillis() +
                                        TimeUnit.HOURS.toMillis(1));
        assertFalse(filter.matches(operation, poolInfoMap));
    }

    @Test
    public void shouldNotMatchWhenOneOfSeveralAttributesDoesNotMatch()
                    throws Exception {
        String pnfsids = Joiner.on(",").join(
                        aReplicaOnlineFileWithNoTags().getPnfsId().toString(),
                        aNonResilientFile().getPnfsId().toString());
        givenFilterWith(pnfsids, "WAITING", null,
                        "resilient-3.dcache-devel-test@enstore", null, null,
                        false);
        givenOperationWith(aReplicaOnlineFileWithNoTags().getPnfsId(),
                        "REPLICA", "resilient-2.dcache-devel-test@enstore",
                        "WAITING", System.currentTimeMillis());
        assertFalse(filter.matches(operation, poolInfoMap));
    }

    @Test
    public void shouldNotMatchWhenPolicyDoesNotMatch() throws Exception {
        givenFilterWith(null, null, "CUSTODIAL", null, null, null, false);
        givenOperationWith(aReplicaOnlineFileWithNoTags().getPnfsId(),
                        "REPLICA", null, "RUNNING", System.currentTimeMillis());
        assertFalse(filter.matches(operation, poolInfoMap));
    }

    @Test
    public void shouldNotMatchWhenSpecifiedPnfsIdDoesNotMatch()
                    throws Exception {
        givenFilterWith(aCustodialNearlineFile().getPnfsId().toString(), null,
                        null, null, null, null, false);
        givenOperationWith(aReplicaOnlineFileWithNoTags().getPnfsId(),
                        "REPLICA", "resilient-2.dcache-devel-test@enstore",
                        "RUNNING", System.currentTimeMillis());
        assertFalse(filter.matches(operation, poolInfoMap));
    }

    @Test
    public void shouldNotMatchWhenStatesDoNotMatch() throws Exception {
        givenFilterWith(null, "WAITING", null, null, null, null, false);
        givenOperationWith(aReplicaOnlineFileWithNoTags().getPnfsId(),
                        "REPLICA", null, "RUNNING", System.currentTimeMillis());
        assertFalse(filter.matches(operation, poolInfoMap));
    }

    @Test
    public void shouldNotMatchWhenStorageUnitDoesNotMatch() throws Exception {
        givenFilterWith(null, null, null,
                        "resilient-3.dcache-devel-test@enstore", null, null,
                        false);
        givenOperationWith(aReplicaOnlineFileWithNoTags().getPnfsId(),
                        "REPLICA", "resilient-2.dcache-devel-test@enstore",
                        "RUNNING", System.currentTimeMillis());
        assertFalse(filter.matches(operation, poolInfoMap));
    }

    private void givenFilterWith(String pnfsIds, String states,
                    String retentionPolicy, String storageUnit, Long after,
                    Long before, boolean force) {
        filter = new PnfsFilter();
        filter.setForceRemoval(force);
        filter.setLastUpdateAfter(after);
        filter.setLastUpdateBefore(before);
        filter.setPnfsIds(pnfsIds);
        filter.setRetentionPolicy(retentionPolicy);
        if (states != null) {
            filter.setState(ImmutableSet.copyOf(states.split("[,]")));
        }
        filter.setStorageUnit(storageUnit);
    }

    private void givenOperationWith(PnfsId pnfsId, String retentionPolicy,
                    String storageUnit, String state, Long update) {
        Integer sunit = storageUnit != null ?
                        poolInfoMap.getGroupIndex(storageUnit) : null;

        operation = new PnfsOperation(pnfsId, -1, sunit, 0, 1, 2L);

        if (retentionPolicy != null) {
            operation.setRetentionPolicy(retentionPolicy);
        }

        operation.setLastUpdate(update);

        if (state != null) {
            operation.setState(state);
        }
    }
}
