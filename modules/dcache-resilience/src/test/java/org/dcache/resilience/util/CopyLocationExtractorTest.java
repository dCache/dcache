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
package org.dcache.resilience.util;

import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import diskCacheV111.poolManager.StorageUnit;
import diskCacheV111.util.CacheException;
import org.dcache.resilience.TestBase;
import org.dcache.resilience.data.StorageUnitConstraints;
import org.dcache.vehicles.FileAttributes;

import static org.junit.Assert.assertTrue;

public final class CopyLocationExtractorTest extends TestBase {
    private String pool;
    private FileAttributes attributes;
    private StorageUnit unit;
    private Collection<String> selected;
    private CopyLocationExtractor extractor;

    @Before
    public void setUp() throws CacheException {
        setUpBase();
    }

    @Test
    public void shouldChooseAnyPoolWhenSourceWithBothConstraintsOnPoolWithNoTags()
                    throws CacheException {
        givenNewFilesOnPoolsWithNoTags();
        givenAReplicaOnlineFileWithBothTags();
        givenTheSourceAndConstraintsOfThisFile();

        whenTheExtractorIsCalledForTheNextSetOfLocations();

        assertTrue(theSelectedPoolsWere(allAvailablePools()));
    }

    @Test
    public void shouldChooseAnyPoolWhenSourceWithHostConstraintOnPoolWithNoTags()
                    throws CacheException {
        givenNewFilesOnPoolsWithNoTags();
        givenAReplicaOnlineFileWithHostTag();
        givenTheSourceAndConstraintsOfThisFile();

        whenTheExtractorIsCalledForTheNextSetOfLocations();

        assertTrue(theSelectedPoolsWere(allAvailablePools()));
    }

    @Test
    public void shouldChooseAnyPoolWhenSourceWithNoConstraintsOnPoolWithBothTags()
                    throws CacheException {
        givenNewFilesOnPoolsWithHostAndRackTags();
        givenAReplicaOnlineFileWithNoTags();
        givenTheSourceAndConstraintsOfThisFile();

        whenTheExtractorIsCalledForTheNextSetOfLocations();

        assertTrue(theSelectedPoolsWere(allAvailablePools()));
    }

    @Test
    public void shouldChooseAnyPoolWhenSourceWithNoConstraintsOnPoolWithHostTag()
                    throws CacheException {
        givenNewFilesOnPoolsWithHostTags();
        givenAReplicaOnlineFileWithNoTags();
        givenTheSourceAndConstraintsOfThisFile();

        whenTheExtractorIsCalledForTheNextSetOfLocations();

        assertTrue(theSelectedPoolsWere(allAvailablePools()));
    }

    @Test
    public void shouldChooseAnyPoolWhenSourceWithNoConstraintsOnPoolWithNoTags()
                    throws CacheException {
        givenNewFilesOnPoolsWithNoTags();
        givenAReplicaOnlineFileWithNoTags();
        givenTheSourceAndConstraintsOfThisFile();

        whenTheExtractorIsCalledForTheNextSetOfLocations();

        assertTrue(theSelectedPoolsWere(allAvailablePools()));
    }

    @Test
    public void shouldChooseAnyPoolWhenSourceWithRackConstraintOnPoolWithHostTag()
                    throws CacheException {
        givenNewFilesOnPoolsWithHostTags();
        givenAReplicaOnlineFileWithRackTag();
        givenTheSourceAndConstraintsOfThisFile();

        whenTheExtractorIsCalledForTheNextSetOfLocations();

        assertTrue(theSelectedPoolsWere(allAvailablePools()));
    }

    @Test
    public void shouldChooseAnyPoolWhenSourceWithRackConstraintOnPoolWithNoTags()
                    throws CacheException {
        givenNewFilesOnPoolsWithNoTags();
        givenAReplicaOnlineFileWithRackTag();
        givenTheSourceAndConstraintsOfThisFile();

        whenTheExtractorIsCalledForTheNextSetOfLocations();

        assertTrue(theSelectedPoolsWere(allAvailablePools()));
    }

    @Test
    public void shouldChoosePoolOnDifferentHostAndRackWhenSourceWithBothConstraintsOnPoolWithBothTags()
                    throws CacheException {
        givenNewFilesOnPoolsWithHostAndRackTags();
        givenAReplicaOnlineFileWithBothTags();
        givenTheSourceAndConstraintsOfThisFile();

        whenTheExtractorIsCalledForTheNextSetOfLocations();

        assertTrue(theSelectedPoolsWere(
                        allPoolsNotOnSameHostAndRackAsSource()));
    }

    @Test
    public void shouldChoosePoolOnDifferentHostWhenSourceWithBothConstraintsOnPoolWithHostTag()
                    throws CacheException {
        givenNewFilesOnPoolsWithHostTags();
        givenAReplicaOnlineFileWithBothTags();
        givenTheSourceAndConstraintsOfThisFile();

        whenTheExtractorIsCalledForTheNextSetOfLocations();

        assertTrue(theSelectedPoolsWere(allPoolsNotOnSameHostAsSource()));
    }

    @Test
    public void shouldChoosePoolOnDifferentHostWhenSourceWithHostConstraintOnPoolWithBothTags()
                    throws CacheException {
        givenNewFilesOnPoolsWithHostAndRackTags();
        givenAReplicaOnlineFileWithHostTag();
        givenTheSourceAndConstraintsOfThisFile();

        whenTheExtractorIsCalledForTheNextSetOfLocations();

        assertTrue(theSelectedPoolsWere(allPoolsNotOnSameHostAsSource()));
    }

    @Test
    public void shouldChoosePoolOnDifferentHostWhenSourceWithHostConstraintOnPoolWithHostTag()
                    throws CacheException {
        givenNewFilesOnPoolsWithHostTags();
        givenAReplicaOnlineFileWithHostTag();
        givenTheSourceAndConstraintsOfThisFile();

        whenTheExtractorIsCalledForTheNextSetOfLocations();

        assertTrue(theSelectedPoolsWere(allPoolsNotOnSameHostAsSource()));
    }

    @Test
    public void shouldChoosePoolOnDifferentRackWhenSourceWithRackConstraintOnPoolWithBothTags()
                    throws CacheException {
        givenNewFilesOnPoolsWithHostAndRackTags();
        givenAReplicaOnlineFileWithRackTag();
        givenTheSourceAndConstraintsOfThisFile();

        whenTheExtractorIsCalledForTheNextSetOfLocations();

        assertTrue(theSelectedPoolsWere(allPoolsNotOnSameRackAsSource()));
    }

    @Test
    public void shouldNotFindAnyAvailablePoolIfFileIsOverconstrained()
                    throws CacheException {
        givenFilesWithExcessLocations();
        givenAReplicaOnlineFileWithBothTags();
        givenTheSourceAndConstraintsOfThisFile();
        givenAllTaglessPoolsAreOffline();
        offlinePools("resilient_pool-9");

        whenTheExtractorIsCalledForTheNextSetOfLocations();

        assertTrue(theSelectedPoolsWere(noneAvailable()));
    }

    private Collection<String> allAvailablePools() {
        return memberPools().stream()
                            .map((i) -> poolInfoMap.getPool(i))
                            .filter((p) -> poolInfoMap.getPoolState(pool).mode.isEnabled())
                            .collect(Collectors.toSet());
    }

    private Collection<String> allPoolsNotOnSameHostAndRackAsSource() {
        return filterPools("hostname", "rack");
    }

    private Collection<String> allPoolsNotOnSameHostAsSource() {
        return filterPools("hostname");
    }

    private Collection<String> allPoolsNotOnSameRackAsSource() {
        return filterPools("rack");
    }

    private Collection<String> filterPools(String... constraints) {
        Map<String, String> sourceTags = poolInfoMap.getTags(
                        poolInfoMap.getPoolIndex(pool));
        return memberPools().stream().filter((p) -> {
            Map<String, String> tags = poolInfoMap.getTags(p);

            if (tags.isEmpty()) {
                return true;
            }

            for (String key : constraints) {
                if (tags.containsKey(key) && sourceTags.containsKey(key)
                                && tags.get(key).equals(sourceTags.get(key))) {
                    return false;
                }
            }

            return true;
        }).map((i) -> poolInfoMap.getPool(i)).collect(Collectors.toSet());
    }

    private void givenAReplicaOnlineFileWithBothTags() throws CacheException {
        attributes = aReplicaOnlineFileWithBothTags();
    }

    private void givenAReplicaOnlineFileWithHostTag() throws CacheException {
        attributes = aReplicaOnlineFileWithHostTag();
    }

    private void givenAReplicaOnlineFileWithNoTags() throws CacheException {
        attributes = aReplicaOnlineFileWithNoTags();
    }

    private void givenAReplicaOnlineFileWithRackTag() throws CacheException {
        attributes = aReplicaOnlineFileWithRackTag();
    }

    private void givenAllTaglessPoolsAreOffline() {
        Collection<String> offline = memberPools().stream().filter(
                        (pool) -> poolInfoMap.getTags(pool).isEmpty()).map(
                        (i) -> poolInfoMap.getPool(i)).collect(
                        Collectors.toList());
        offlinePools(offline.toArray(new String[0]));
    }

    private void givenFilesWithExcessLocations() {
        loadFilesWithExcessLocations();
    }

    private void givenNewFilesOnPoolsWithHostAndRackTags() {
        loadNewFilesOnPoolsWithHostAndRackTags();
    }

    private void givenNewFilesOnPoolsWithHostTags() {
        loadNewFilesOnPoolsWithHostTags();
    }

    private void givenNewFilesOnPoolsWithNoTags() {
        loadNewFilesOnPoolsWithNoTags();
    }

    private void givenTheSourceAndConstraintsOfThisFile() {
        pool = attributes.getLocations().iterator().next();
        String storageGroupName = attributes.getStorageClass() + "@"
                        + attributes.getHsm();
        Integer sindex = poolInfoMap.getUnitIndex(storageGroupName);
        StorageUnitConstraints constraints = poolInfoMap.getStorageUnitConstraints(sindex);
        extractor = new CopyLocationExtractor(constraints.getOneCopyPer(),
                        poolInfoMap);
    }

    private Collection<Integer> memberPools() {
        return poolInfoMap.getPoolsOfGroup(poolInfoMap.getResilientPoolGroup(
                        poolInfoMap.getPoolIndex(pool)));
    }

    private Collection<String> noneAvailable() {
        return Collections.EMPTY_SET;
    }

    private boolean theSelectedPoolsWere(Collection<String> pools) {
        for (String pool : pools) {
            if (!selected.contains(pool)) {
                return false;
            }
        }

        for (String pool : selected) {
            if (!pools.contains(pool)) {
                return false;
            }
        }

        return true;
    }

    private void whenTheExtractorIsCalledForTheNextSetOfLocations() {
        attributes.getLocations().stream().forEach(
                        (p) -> extractor.addSeenTagsFor(p));
        selected = extractor.getCandidateLocations(allAvailablePools());
    }
}
