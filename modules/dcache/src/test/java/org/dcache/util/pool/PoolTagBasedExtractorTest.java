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
package org.dcache.util.pool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for PoolTagBasedExtractor.
 */
public class PoolTagBasedExtractorTest {

    /**
     * Test implementation of PoolTagProvider for testing.
     */
    private static class TestPoolTagProvider implements PoolTagProvider {
        private final Map<String, Map<String, String>> poolTags = new HashMap<>();

        public void setPoolTags(String poolName, Map<String, String> tags) {
            poolTags.put(poolName, tags);
        }

        @Override
        public Map<String, String> getPoolTags(String location) {
            return poolTags.getOrDefault(location, Collections.emptyMap());
        }
    }

    private TestPoolTagProvider testTagProvider;
    private PoolTagBasedExtractor extractor;

    @Before
    public void setUp() {
        testTagProvider = new TestPoolTagProvider();
        extractor = new PoolTagBasedExtractor(
            Collections.singletonList("hostname"), testTagProvider);
    }

    @Test
    public void shouldReturnPoolTagsFromTagProvider() {
        // Given: pool with hostname and rack tags
        Map<String, String> expectedTags = ImmutableMap.of(
            "hostname", "storage01.example.com",
            "rack", "rack-A-01",
            "site", "datacenter-west"
        );
        testTagProvider.setPoolTags("pool1", expectedTags);

        // When: getting pool tags
        Map<String, String> actualTags = extractor.getPoolTagsFor("pool1");

        // Then: should return the tags from tag provider
        assertEquals("Should return all pool tags", expectedTags, actualTags);
    }

    @Test
    public void shouldReturnEmptyMapWhenTagProviderReturnsNull() {
        // Given: tag provider that returns null for unknown pool
        PoolTagProvider nullReturningProvider = location -> null;
        PoolTagBasedExtractor testExtractor = new PoolTagBasedExtractor(
            Collections.singletonList("hostname"), nullReturningProvider);

        // When: getting pool tags for unknown pool
        Map<String, String> tags = testExtractor.getPoolTagsFor("unknownpool");

        // Then: should return empty map
        assertTrue("Should return empty map for unknown pool", tags.isEmpty());
    }

    @Test
    public void shouldReturnEmptyMapWhenTagProviderIsNull() {
        // Given: extractor with null tag provider
        PoolTagBasedExtractor nullProviderExtractor =
            new PoolTagBasedExtractor(Collections.singletonList("hostname"), null);

        // When: getting pool tags
        Map<String, String> tags = nullProviderExtractor.getPoolTagsFor("pool1");

        // Then: should return empty map
        assertTrue("Should return empty map when tag provider is null", tags.isEmpty());
    }

    @Test
    public void shouldFilterPoolsBasedOnActualTags() {
        // Given: pools with different hostname tags
        testTagProvider.setPoolTags("pool1", ImmutableMap.of("hostname", "host1.example.com"));
        testTagProvider.setPoolTags("pool2", ImmutableMap.of("hostname", "host1.example.com"));  // Same hostname
        testTagProvider.setPoolTags("pool3", ImmutableMap.of("hostname", "host2.example.com"));  // Different hostname
        testTagProvider.setPoolTags("pool4", ImmutableMap.of("hostname", "host3.example.com"));  // Different hostname

        // And: extractor has seen pool1's hostname
        extractor.addSeenTagsFor("pool1");

        // When: getting candidates
        Collection<String> candidates = Arrays.asList("pool2", "pool3", "pool4");
        Collection<String> result = extractor.getCandidateLocations(candidates);

        // Then: should exclude pools with same hostname as seen pool
        assertEquals("Should have 2 candidate pools", 2, result.size());
        assertTrue("Should contain pool3", result.contains("pool3"));
        assertTrue("Should contain pool4", result.contains("pool4"));
        assertFalse("Should not contain pool2 (same hostname)", result.contains("pool2"));
    }

    @Test
    public void shouldFilterOnMultipleConstraintTags() {
        // Given: extractor configured for both hostname and rack constraints
        PoolTagBasedExtractor multiTagExtractor = new PoolTagBasedExtractor(
            Arrays.asList("hostname", "rack"), testTagProvider);

        // And: pools with overlapping hostname or rack tags
        testTagProvider.setPoolTags("pool1", ImmutableMap.of("hostname", "host1", "rack", "rackA"));
        testTagProvider.setPoolTags("pool2", ImmutableMap.of("hostname", "host2", "rack", "rackA"));  // Same rack
        testTagProvider.setPoolTags("pool3", ImmutableMap.of("hostname", "host1", "rack", "rackB"));  // Same hostname
        testTagProvider.setPoolTags("pool4", ImmutableMap.of("hostname", "host3", "rack", "rackC"));  // Different both

        // When: adding seen tags for pool1 and filtering
        multiTagExtractor.addSeenTagsFor("pool1");
        Collection<String> result = multiTagExtractor.getCandidateLocations(
            Arrays.asList("pool2", "pool3", "pool4"));

        // Then: should exclude pools with any matching constraint tag
        assertEquals("Should have 1 candidate pool", 1, result.size());
        assertTrue("Should contain pool4 only", result.contains("pool4"));
        assertFalse("Should not contain pool2 (same rack)", result.contains("pool2"));
        assertFalse("Should not contain pool3 (same hostname)", result.contains("pool3"));
    }

    @Test
    public void shouldHandlePoolsWithMissingConstraintTags() {
        // Given: pool without hostname tag
        Map<String, String> tagsWithoutHostname = ImmutableMap.of(
            "rack", "rack-C-01",
            "zone", "zone-west"
        );
        testTagProvider.setPoolTags("pool-no-hostname", tagsWithoutHostname);

        // When: getting pool tags
        Map<String, String> tags = extractor.getPoolTagsFor("pool-no-hostname");

        // Then: should return all available tags (without hostname)
        assertEquals("Should return available tags", tagsWithoutHostname, tags);
        assertFalse("Should not contain hostname tag", tags.containsKey("hostname"));
    }

    @Test
    public void shouldAllowPoolsWithMissingConstraintTagsInFiltering() {
        // Given: pools where some have hostname tags and others don't
        testTagProvider.setPoolTags("pool-with-hostname", ImmutableMap.of("hostname", "host1.example.com"));
        testTagProvider.setPoolTags("pool-without-hostname", ImmutableMap.of("rack", "rack-A"));

        // And: extractor has seen a hostname
        extractor.addSeenTagsFor("pool-with-hostname");

        // When: filtering candidates including pool without hostname tag
        Collection<String> result = extractor.getCandidateLocations(
            Arrays.asList("pool-without-hostname"));

        // Then: should include pool without hostname tag (no constraint violation)
        assertEquals("Should have 1 candidate pool", 1, result.size());
        assertTrue("Should contain pool without hostname tag", result.contains("pool-without-hostname"));
    }

    @Test
    public void shouldReturnAllCandidatesWhenNoSeenTags() {
        // Given: pools with various hostnames
        testTagProvider.setPoolTags("pool1", ImmutableMap.of("hostname", "host1.example.com"));
        testTagProvider.setPoolTags("pool2", ImmutableMap.of("hostname", "host2.example.com"));
        testTagProvider.setPoolTags("pool3", ImmutableMap.of("hostname", "host3.example.com"));

        // When: getting candidates without any seen tags
        Collection<String> candidates = Arrays.asList("pool1", "pool2", "pool3");
        Collection<String> result = extractor.getCandidateLocations(candidates);

        // Then: should return all candidates
        assertEquals("Should return all candidates when no constraints", 3, result.size());
        assertTrue("Should contain all candidates", result.containsAll(candidates));
    }

    @Test
    public void shouldWorkWithEmptyConstraintTags() {
        // Given: extractor with empty constraint tags
        PoolTagBasedExtractor emptyTagsExtractor =
            new PoolTagBasedExtractor(Collections.emptyList(), testTagProvider);

        // When: filtering candidates (should skip tag checking entirely)
        Collection<String> result = emptyTagsExtractor.getCandidateLocations(
            Arrays.asList("pool1", "pool2"));

        // Then: should return all candidates (no constraints to apply)
        assertEquals("Should return all candidates with no constraint tags", 2, result.size());
    }
}
