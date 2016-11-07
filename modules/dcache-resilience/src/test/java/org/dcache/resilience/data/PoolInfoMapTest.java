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
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.stream.Collectors;

import diskCacheV111.util.CacheException;
import org.dcache.resilience.TestBase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public final class PoolInfoMapTest extends TestBase {
    Integer group;
    Collection<String> collection;
    PoolInfoDiff diff;

    @Before
    public void setUp() throws CacheException {
        setUpBase();
    }

    /*
     *  Minimal test.  Full testing of the diff/apply routing
     *  is in PoolMonitorChangeHelperTest.
     */
    @Test
    public void shouldReplaceCurrentPsuWithNewPsu() {
        givenNewPsuWith("new-pool",
                        "resilient-group");
        whenCompareIsCalledWithNewPsu();
        whenApplyIsCalled();
        assertThatPoolInfoMapContainsPool("new-pool");
        assertThatPoolBelongsToResilientGroup("new-pool", "resilient-group");
    }

    private void assertThatPoolBelongsToResilientGroup(String pool, String group) {
        assertEquals(poolInfoMap.getGroupIndex(group),
                     poolInfoMap.getResilientPoolGroup(poolInfoMap.getPoolIndex(pool)));
    }

    private void assertThatPoolInfoMapContainsPool(String pool) {
        assertNotNull(poolInfoMap.getPoolIndex(pool));
    }

    private void givenNewPsuWith(String newPool, String poolGroup) {
        createNewPoolMonitor();
        createNewPool(newPool);
        getUpdatedPsu().addToPoolGroup(poolGroup, newPool);
    }

    private void whenCompareIsCalledWithNewPsu() {
         diff = poolInfoMap.compare(newPoolMonitor);
    }

    private void whenApplyIsCalled() {
        poolInfoMap.apply(diff);
    }

    @Test
    public void shouldNotVerifyConstraintsWhenTooManyLocationsAreRequired() {
        givenResilientGroup();
        givenAResilientStorageGroupWithRequirementsEqualTo(9, ImmutableList.of("hostname", "rack"));
        assertThatContraintsSatisfiableIs(false);
    }

    @Test
    public void shouldReturnSixStorageClassesForResilientPoolGroup() {
        whenStorageGroupsAreRequestedFor("resilient-group");
        assertEquals(6, collection.size());
    }

    @Test
    public void shouldReturnOneStorageClassForNonResilientPoolGroup() {
        whenStorageGroupsAreRequestedFor("standard-group");
        assertEquals(1, collection.size());
    }

    @Test
    public void shouldVerifyContraintsForCurrentSettings() {
        givenResilientGroup();
        assertThatContraintsSatisfiableIs(true);
    }

    private void assertThatContraintsSatisfiableIs(boolean satisfiable) {
        IllegalStateException exception = null;
        try {
            poolInfoMap.verifyConstraints(group);
        } catch (IllegalStateException e) {
            exception = e;
        }
        assertEquals(satisfiable, exception == null);
    }

    private void givenAResilientStorageGroupWithRequirementsEqualTo(
                    int required, Collection<String> tags) {
        poolInfoMap.setGroupConstraints("resilient-0.dcache-devel-test@enstore",
                        required, tags);
    }

    private void givenResilientGroup() {
        group = poolInfoMap.getGroupIndex("resilient-group");
    }

    private void whenStorageGroupsAreRequestedFor(String poolGroup) {
        collection = poolInfoMap.getStorageUnitsFor(poolGroup).stream().map(
                        (i) -> poolInfoMap.getGroupName(i)).collect(
                        Collectors.toList());
    }
}
