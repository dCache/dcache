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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.CacheException;
import org.dcache.resilience.TestBase;
import org.dcache.resilience.data.PoolStateUpdate;
import org.dcache.vehicles.FileAttributes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public final class LocationSelectorTest extends TestBase {
    String pool;
    String selected;
    Integer group;
    FileAttributes attributes;
    Collection<String> collection;
    Set<String> readable;
    Set<String> available;
    Set<Integer> tried;

    @Before
    public void setUp() throws CacheException {
        setUpBase();
    }

    @Test
    public void shouldFailToSelectAWritableCopyLocation() throws Exception {
        givenAFileWithSeveralLocations();
        givenTheSetOfOnlinePoolsForTheGroup();
        givenThisManyOtherPoolsInTheGroupAreDown(12);
        whenCopyTargetIsSelected();
        assertNull(selected);
    }

    @Test
    public void shouldFailToSelectAWritableRemoveLocation() throws Exception {
        givenAFileWithSeveralLocations();
        givenThisManyLocationsAreOffline(attributes.getLocations().size());
        whenRemoveTargetIsSelected();
        assertNull(selected);
    }

    @Test
    public void shouldReturnMultipleLocations() throws CacheException {
        givenAFileWithSeveralLocations();
        whenReadableMemberLocationsAreRequested();
        assertTrue(1 < collection.size());
        assertTrue(collection.contains(pool));
    }

    @Test
    public void shouldReturnNoLocation() throws CacheException {
        givenAFileOnlyOnOnePool();
        givenLocationGoesOffline(pool);
        whenReadableMemberLocationsAreRequested();
        assertEquals(0, collection.size());
    }

    @Test
    public void shouldReturnOnlyCurrentLocation() throws CacheException {
        givenAFileOnlyOnOnePool();
        whenReadableMemberLocationsAreRequested();
        assertEquals(1, collection.size());
        assertEquals(pool, collection.iterator().next());
    }

    @Test
    public void shouldSelectARandomGroupMemberWhichHasNotBeenTried()
                    throws Exception {
        givenAFileOnlyOnOnePool();
        givenTheSetOfOnlinePoolsForTheGroup();
        givenTheCurrentLocationHasBeenTried();
        whenSourceIsSelected();
        assertNotNull(selected);
        assertFalse(pool.equals(selected));
    }

    @Test
    public void shouldSelectAWritableCopyLocation() throws Exception {
        givenAFileWithSeveralLocations();
        givenTheSetOfOnlinePoolsForTheGroup();
        givenThisManyOtherPoolsInTheGroupAreDown(11);
        whenCopyTargetIsSelected();
        assertNotNull(selected);
        assertFalse(attributes.getLocations().contains(selected));
        assertTrue(available.contains(selected));
    }

    @Test
    public void shouldSelectAWritableRemoveLocation() throws Exception {
        givenAFileWithSeveralLocations();
        givenThisManyLocationsAreOffline(attributes.getLocations().size() - 1);
        whenRemoveTargetIsSelected();
        assertNotNull(selected);
        assertTrue(attributes.getLocations().contains(selected));
        assertTrue(poolInfoMap.getPoolState(selected).mode.isEnabled());
    }

    private void givenAFileOnlyOnOnePool() throws CacheException {
        loadNewFilesOnPoolsWithNoTags();
        attributes = aReplicaOnlineFileWithNoTags();
        setInfoFromAttributes();
    }

    private void givenAFileWithSeveralLocations() throws CacheException {
        loadFilesWithRequiredLocations();
        attributes = aReplicaOnlineFileWithNoTags();
        setInfoFromAttributes();
    }

    private void givenLocationGoesOffline(String location)
                    throws CacheException {
        PoolStateUpdate update = new PoolStateUpdate(location,
                                    new PoolV2Mode(PoolV2Mode.DISABLED_STRICT));
        poolInfoMap.updatePoolStatus(update);
    }

    private void givenTheCurrentLocationHasBeenTried() {
        tried = ImmutableSet.of(poolInfoMap.getPoolIndex(pool));
    }

    private void givenTheSetOfOnlinePoolsForTheGroup() {
        readable = poolInfoMap.getPoolsOfGroup(group).stream()
                        .map((i) -> poolInfoMap.getPool(i))
                        .filter((p) -> poolInfoMap.getPoolState(p).mode.isEnabled())
                        .collect(Collectors.toSet());
    }

    private void givenThisManyLocationsAreOffline(int offline)
                    throws CacheException {
        collection = attributes.getLocations();
        int count = 0;

        for (String location : collection) {
            givenLocationGoesOffline(location);
            if (++count >= offline) {
                break;
            }
        }
    }

    private void givenThisManyOtherPoolsInTheGroupAreDown(int down)
                    throws CacheException {
        available = Sets.difference(readable,
                        ImmutableSet.copyOf(attributes.getLocations()));
        down = Math.min(available.size(), down);
        int count = 0;
        for (String pool : available) {
            givenLocationGoesOffline(pool);
            if (++count >= down) {
                break;
            }
        }
    }

    private void setInfoFromAttributes() {
        pool = attributes.getLocations().iterator().next();
        group = poolInfoMap.getResilientPoolGroup(
                        poolInfoMap.getPoolIndex(pool));
    }

    private void whenCopyTargetIsSelected() {
        try {
            selected = locationSelector.selectCopyTarget(group,
                                                         attributes.getLocations(),
                                                         Collections.EMPTY_SET,
                                                         Collections.EMPTY_SET);
        } catch (Exception e) {
            LOGGER.error("{}", new ExceptionMessage(e));
        }
    }

    private void whenReadableMemberLocationsAreRequested() {
        collection = poolInfoMap.getMemberLocations(group, attributes.getLocations());
        collection = poolInfoMap.getReadableLocations(collection);
    }

    private void whenRemoveTargetIsSelected() {
        try {
            selected = locationSelector.selectRemoveTarget(collection, null);
        } catch (Exception e) {
            LOGGER.error("{}", new ExceptionMessage(e));
        }
    }

    private void whenSourceIsSelected() throws Exception {
        selected = locationSelector.selectSource(readable, tried);
    }
}
