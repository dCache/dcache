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
package org.dcache.pool.migration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.dcache.pool.classic.IoQueueManager;
import org.dcache.util.pool.PoolTagProvider;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for HostnameConstrainedPoolList.
 */
public class HostnameConstrainedPoolListTest {

    @Mock
    private RefreshablePoolList mockDelegate;

    @Mock
    private RefreshablePoolList mockSourceList;

    @Mock
    private PoolTagProvider mockTagProvider;

    private HostnameConstrainedPoolList constrainedList;

    // Test pool information objects
    private PoolManagerPoolInformation pool1OnHost1;
    private PoolManagerPoolInformation pool2OnHost1;
    private PoolManagerPoolInformation pool3OnHost2;
    private PoolManagerPoolInformation pool4OnHost3;
    private PoolManagerPoolInformation sourcePoolOnHost1;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        // Create test pool information objects with different hostname patterns
        pool1OnHost1 = createPoolInfo("pool1@host1");
        pool2OnHost1 = createPoolInfo("pool2@host1");
        pool3OnHost2 = createPoolInfo("pool3@host2");
        pool4OnHost3 = createPoolInfo("pool4@host3");
        sourcePoolOnHost1 = createPoolInfo("sourcePool@host1");

        // Set up mock tag provider to extract hostname from pool names for testing
        when(mockTagProvider.getPoolTags("pool1@host1")).thenReturn(Map.of("hostname", "host1"));
        when(mockTagProvider.getPoolTags("pool2@host1")).thenReturn(Map.of("hostname", "host1"));
        when(mockTagProvider.getPoolTags("pool3@host2")).thenReturn(Map.of("hostname", "host2"));
        when(mockTagProvider.getPoolTags("pool4@host3")).thenReturn(Map.of("hostname", "host3"));
        when(mockTagProvider.getPoolTags("sourcePool@host1")).thenReturn(Map.of("hostname", "host1"));
        when(mockTagProvider.getPoolTags("hostA_pool1")).thenReturn(Map.of("hostname", "hostA"));
        when(mockTagProvider.getPoolTags("hostB_pool1")).thenReturn(Map.of("hostname", "hostB"));
        when(mockTagProvider.getPoolTags("hostA_sourcePool")).thenReturn(Map.of("hostname", "hostA"));
        when(mockTagProvider.getPoolTags("host1")).thenReturn(Map.of("hostname", "host1"));
        when(mockTagProvider.getPoolTags("host2")).thenReturn(Map.of("hostname", "host2"));
        when(mockTagProvider.getPoolTags("offlinePool1@host1")).thenReturn(Map.of("hostname", "host1"));
        when(mockTagProvider.getPoolTags("offlinePool2@host2")).thenReturn(Map.of("hostname", "host2"));
    }

    private PoolManagerPoolInformation createPoolInfo(String poolName) {
        return new PoolManagerPoolInformation(poolName,
            new PoolCostInfo(poolName, IoQueueManager.DEFAULT_QUEUE), 0);
    }

    @Test
    public void shouldFilterPoolsWithSameHostnameAsSource() {
        // Given: source list has a pool on host1, target list has pools on multiple hosts
        when(mockSourceList.getPools()).thenReturn(ImmutableList.of(sourcePoolOnHost1));
        when(mockSourceList.getOfflinePools()).thenReturn(ImmutableList.of());
        when(mockDelegate.getPools()).thenReturn(ImmutableList.of(pool1OnHost1, pool2OnHost1, pool3OnHost2, pool4OnHost3));
        when(mockDelegate.getOfflinePools()).thenReturn(ImmutableList.of());
        when(mockDelegate.isValid()).thenReturn(true);
        when(mockSourceList.isValid()).thenReturn(true);

        constrainedList = new HostnameConstrainedPoolList(mockDelegate, mockSourceList,
            Collections.singletonList("hostname"), mockTagProvider);

        // When: getting filtered pools
        ImmutableList<PoolManagerPoolInformation> result = constrainedList.getPools();

        // Then: should exclude pools on host1 (same as source), include pools on other hosts
        assertEquals("Should have 2 pools (host2 and host3)", 2, result.size());
        assertTrue("Should contain pool3@host2", result.contains(pool3OnHost2));
        assertTrue("Should contain pool4@host3", result.contains(pool4OnHost3));
        assertFalse("Should not contain pool1@host1", result.contains(pool1OnHost1));
        assertFalse("Should not contain pool2@host1", result.contains(pool2OnHost1));
    }

    @Test
    public void shouldReturnAllPoolsWhenNoSourcePools() {
        // Given: no source pools
        when(mockSourceList.getPools()).thenReturn(ImmutableList.of());
        when(mockSourceList.getOfflinePools()).thenReturn(ImmutableList.of());
        when(mockDelegate.getPools()).thenReturn(ImmutableList.of(pool1OnHost1, pool3OnHost2, pool4OnHost3));
        when(mockDelegate.getOfflinePools()).thenReturn(ImmutableList.of());
        when(mockDelegate.isValid()).thenReturn(true);
        when(mockSourceList.isValid()).thenReturn(true);

        constrainedList = new HostnameConstrainedPoolList(mockDelegate, mockSourceList,
            Collections.singletonList("hostname"), mockTagProvider);

        // When: getting filtered pools
        ImmutableList<PoolManagerPoolInformation> result = constrainedList.getPools();

        // Then: should return all pools without filtering
        assertEquals("Should have all 3 pools", 3, result.size());
        assertTrue("Should contain all original pools",
            result.containsAll(Arrays.asList(pool1OnHost1, pool3OnHost2, pool4OnHost3)));
    }

    @Test
    public void shouldReturnAllPoolsWhenNullSourceList() {
        // Given: null source list
        when(mockDelegate.getPools()).thenReturn(ImmutableList.of(pool1OnHost1, pool3OnHost2, pool4OnHost3));
        when(mockDelegate.getOfflinePools()).thenReturn(ImmutableList.of());
        when(mockDelegate.isValid()).thenReturn(true);

        constrainedList = new HostnameConstrainedPoolList(mockDelegate, null,
            Collections.singletonList("hostname"), mockTagProvider);

        // When: getting filtered pools
        ImmutableList<PoolManagerPoolInformation> result = constrainedList.getPools();

        // Then: should return all pools without filtering
        assertEquals("Should have all 3 pools", 3, result.size());
        assertTrue("Should contain all original pools",
            result.containsAll(Arrays.asList(pool1OnHost1, pool3OnHost2, pool4OnHost3)));
    }

    @Test
    public void shouldFilterOfflinePoolsWithSameHostname() {
        // Given: source and offline pools with overlapping hostnames
        when(mockSourceList.getPools()).thenReturn(ImmutableList.of(sourcePoolOnHost1));
        when(mockSourceList.getOfflinePools()).thenReturn(ImmutableList.of());
        when(mockDelegate.getPools()).thenReturn(ImmutableList.of(pool3OnHost2, pool4OnHost3));
        when(mockDelegate.getOfflinePools()).thenReturn(ImmutableList.of("offlinePool1@host1", "offlinePool2@host2"));
        when(mockDelegate.isValid()).thenReturn(true);
        when(mockSourceList.isValid()).thenReturn(true);

        constrainedList = new HostnameConstrainedPoolList(mockDelegate, mockSourceList,
            Collections.singletonList("hostname"), mockTagProvider);

        // When: getting filtered offline pools
        ImmutableList<String> result = constrainedList.getOfflinePools();

        // Then: should exclude offline pools on host1
        assertEquals("Should have 1 offline pool", 1, result.size());
        assertTrue("Should contain offlinePool2@host2", result.contains("offlinePool2@host2"));
        assertFalse("Should not contain offlinePool1@host1", result.contains("offlinePool1@host1"));
    }

    @Test
    public void shouldHandlePoolNamesWithUnderscoreFormat() {
        // Given: pools with underscore hostname format
        PoolManagerPoolInformation hostAPool = createPoolInfo("hostA_pool1");
        PoolManagerPoolInformation hostBPool = createPoolInfo("hostB_pool1");
        PoolManagerPoolInformation sourceHostAPool = createPoolInfo("hostA_sourcePool");

        when(mockSourceList.getPools()).thenReturn(ImmutableList.of(sourceHostAPool));
        when(mockSourceList.getOfflinePools()).thenReturn(ImmutableList.of());
        when(mockDelegate.getPools()).thenReturn(ImmutableList.of(hostAPool, hostBPool));
        when(mockDelegate.getOfflinePools()).thenReturn(ImmutableList.of());
        when(mockDelegate.isValid()).thenReturn(true);
        when(mockSourceList.isValid()).thenReturn(true);

        constrainedList = new HostnameConstrainedPoolList(mockDelegate, mockSourceList,
            Collections.singletonList("hostname"), mockTagProvider);

        // When: getting filtered pools
        ImmutableList<PoolManagerPoolInformation> result = constrainedList.getPools();

        // Then: should exclude pools on hostA, include pools on hostB
        assertEquals("Should have 1 pool", 1, result.size());
        assertTrue("Should contain hostB_pool1", result.contains(hostBPool));
        assertFalse("Should not contain hostA_pool1", result.contains(hostAPool));
    }

    @Test
    public void shouldHandlePoolNamesWithoutSpecialCharacters() {
        // Given: pools with simple names (no @ or _)
        PoolManagerPoolInformation simplePool1 = createPoolInfo("host1");
        PoolManagerPoolInformation simplePool2 = createPoolInfo("host2");
        PoolManagerPoolInformation sourcePool = createPoolInfo("host1");

        when(mockSourceList.getPools()).thenReturn(ImmutableList.of(sourcePool));
        when(mockSourceList.getOfflinePools()).thenReturn(ImmutableList.of());
        when(mockDelegate.getPools()).thenReturn(ImmutableList.of(simplePool1, simplePool2));
        when(mockDelegate.getOfflinePools()).thenReturn(ImmutableList.of());
        when(mockDelegate.isValid()).thenReturn(true);
        when(mockSourceList.isValid()).thenReturn(true);

        constrainedList = new HostnameConstrainedPoolList(mockDelegate, mockSourceList,
            Collections.singletonList("hostname"), mockTagProvider);

        // When: getting filtered pools
        ImmutableList<PoolManagerPoolInformation> result = constrainedList.getPools();

        // Then: should exclude pools with same name as source
        assertEquals("Should have 1 pool", 1, result.size());
        assertTrue("Should contain host2", result.contains(simplePool2));
        assertFalse("Should not contain host1", result.contains(simplePool1));
    }

    @Test
    public void shouldRefreshDelegateAndSourceLists() {
        // Given: a constrained list
        constrainedList = new HostnameConstrainedPoolList(mockDelegate, mockSourceList,
            Collections.singletonList("hostname"), mockTagProvider);

        // When: calling refresh
        constrainedList.refresh();

        // Then: should refresh both delegate and source lists
        verify(mockDelegate, times(1)).refresh();
        verify(mockSourceList, times(1)).refresh();
    }

    @Test
    public void shouldRefreshOnlyDelegateWhenSourceListIsNull() {
        // Given: a constrained list with null source list
        constrainedList = new HostnameConstrainedPoolList(mockDelegate, null,
            Collections.singletonList("hostname"), mockTagProvider);

        // When: calling refresh
        constrainedList.refresh();

        // Then: should refresh only delegate
        verify(mockDelegate, times(1)).refresh();
        verify(mockSourceList, never()).refresh();
    }

    @Test
    public void shouldInvalidateCacheOnRefresh() {
        // Given: a constrained list with initial cached results
        when(mockSourceList.getPools()).thenReturn(ImmutableList.of(sourcePoolOnHost1));
        when(mockSourceList.getOfflinePools()).thenReturn(ImmutableList.of());
        when(mockDelegate.getPools()).thenReturn(ImmutableList.of(pool3OnHost2));
        when(mockDelegate.getOfflinePools()).thenReturn(ImmutableList.of());
        when(mockDelegate.isValid()).thenReturn(true);
        when(mockSourceList.isValid()).thenReturn(true);

        constrainedList = new HostnameConstrainedPoolList(mockDelegate, mockSourceList,
            Collections.singletonList("hostname"), mockTagProvider);

        // When: getting pools (triggers caching)
        ImmutableList<PoolManagerPoolInformation> initialResult = constrainedList.getPools();
        assertEquals("Should have 1 pool initially", 1, initialResult.size());

        // And: changing underlying data and refreshing
        when(mockDelegate.getPools()).thenReturn(ImmutableList.of(pool3OnHost2, pool4OnHost3));
        constrainedList.refresh();

        // Then: should re-filter with new data
        ImmutableList<PoolManagerPoolInformation> newResult = constrainedList.getPools();
        assertEquals("Should have 2 pools after refresh", 2, newResult.size());
        assertTrue("Should contain both pools", newResult.containsAll(Arrays.asList(pool3OnHost2, pool4OnHost3)));
    }

    @Test
    public void shouldReturnValidWhenBothDelegateAndSourceAreValid() {
        // Given: both delegate and source are valid
        when(mockDelegate.isValid()).thenReturn(true);
        when(mockSourceList.isValid()).thenReturn(true);

        constrainedList = new HostnameConstrainedPoolList(mockDelegate, mockSourceList,
            Collections.singletonList("hostname"), mockTagProvider);

        // When/Then: should be valid
        assertTrue("Should be valid when both are valid", constrainedList.isValid());
    }

    @Test
    public void shouldReturnInvalidWhenDelegateIsInvalid() {
        // Given: delegate is invalid
        when(mockDelegate.isValid()).thenReturn(false);
        when(mockSourceList.isValid()).thenReturn(true);

        constrainedList = new HostnameConstrainedPoolList(mockDelegate, mockSourceList,
            Collections.singletonList("hostname"), mockTagProvider);

        // When/Then: should be invalid
        assertFalse("Should be invalid when delegate is invalid", constrainedList.isValid());
    }

    @Test
    public void shouldReturnInvalidWhenSourceListIsInvalid() {
        // Given: source list is invalid
        when(mockDelegate.isValid()).thenReturn(true);
        when(mockSourceList.isValid()).thenReturn(false);

        constrainedList = new HostnameConstrainedPoolList(mockDelegate, mockSourceList,
            Collections.singletonList("hostname"), mockTagProvider);

        // When/Then: should be invalid
        assertFalse("Should be invalid when source list is invalid", constrainedList.isValid());
    }

    @Test
    public void shouldReturnValidWhenSourceListIsNull() {
        // Given: source list is null
        when(mockDelegate.isValid()).thenReturn(true);

        constrainedList = new HostnameConstrainedPoolList(mockDelegate, null,
            Collections.singletonList("hostname"), mockTagProvider);

        // When/Then: should be valid (null source list is considered valid)
        assertTrue("Should be valid when source list is null", constrainedList.isValid());
    }

    @Test
    public void shouldUseDefaultHostnameTagWhenConstraintTagsIsNull() {
        // Given: null constraint tags
        when(mockSourceList.getPools()).thenReturn(ImmutableList.of(sourcePoolOnHost1));
        when(mockSourceList.getOfflinePools()).thenReturn(ImmutableList.of());
        when(mockDelegate.getPools()).thenReturn(ImmutableList.of(pool1OnHost1, pool3OnHost2));
        when(mockDelegate.getOfflinePools()).thenReturn(ImmutableList.of());
        when(mockDelegate.isValid()).thenReturn(true);
        when(mockSourceList.isValid()).thenReturn(true);

        constrainedList = new HostnameConstrainedPoolList(mockDelegate, mockSourceList, null, mockTagProvider);

        // When: getting filtered pools
        ImmutableList<PoolManagerPoolInformation> result = constrainedList.getPools();

        // Then: should still filter by hostname (default behavior)
        assertEquals("Should have 1 pool", 1, result.size());
        assertTrue("Should contain pool3@host2", result.contains(pool3OnHost2));
        assertFalse("Should not contain pool1@host1", result.contains(pool1OnHost1));
    }

    @Test
    public void shouldLazilyApplyConstraintsOnFirstAccess() {
        // Given: a constrained list that hasn't been accessed yet
        constrainedList = new HostnameConstrainedPoolList(mockDelegate, mockSourceList,
            Collections.singletonList("hostname"), mockTagProvider);

        // When: creating the list (no access to getPools() yet)
        // Then: delegate should not be called yet
        verify(mockDelegate, never()).getPools();
        verify(mockSourceList, never()).getPools();

        // When: accessing pools for the first time
        when(mockSourceList.getPools()).thenReturn(ImmutableList.of(sourcePoolOnHost1));
        when(mockSourceList.getOfflinePools()).thenReturn(ImmutableList.of());
        when(mockDelegate.getPools()).thenReturn(ImmutableList.of(pool3OnHost2));
        when(mockDelegate.getOfflinePools()).thenReturn(ImmutableList.of());

        constrainedList.getPools();

        // Then: delegate and source should be called
        verify(mockDelegate, times(1)).getPools();
        verify(mockSourceList, times(1)).getPools();
    }
}
