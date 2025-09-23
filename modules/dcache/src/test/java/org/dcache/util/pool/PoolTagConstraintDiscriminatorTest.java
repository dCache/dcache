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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/**
 * Unit tests for PoolTagConstraintDiscriminator.
 */
public class PoolTagConstraintDiscriminatorTest {

    /**
     * Test implementation of PoolTagConstraintDiscriminator for testing purposes.
     */
    private static class TestPoolTagConstraintDiscriminator extends PoolTagConstraintDiscriminator {

        private final Map<String, Map<String, String>> poolTags;

        public TestPoolTagConstraintDiscriminator(Collection<String> onlyOneCopyPer,
                                                 Map<String, Map<String, String>> poolTags) {
            super(onlyOneCopyPer);
            this.poolTags = poolTags != null ? poolTags : Collections.emptyMap();
        }

        @Override
        public Collection<String> getCandidateLocations(Collection<String> locations) {
            // Simple implementation: return all locations for testing
            return ImmutableList.copyOf(locations);
        }

        @Override
        protected Map<String, String> getPoolTagsFor(String location) {
            return poolTags.getOrDefault(location, Collections.emptyMap());
        }

        // Expose protected field for testing
        public Collection<String> getPartitionKeys() {
            return partitionKeys;
        }
    }

    @Test
    public void shouldCreateEmptyPartitionKeysWhenOnlyOneCopyPerIsNull() {
        TestPoolTagConstraintDiscriminator discriminator =
            new TestPoolTagConstraintDiscriminator(null, Collections.emptyMap());

        assertNotNull("Partition keys should not be null", discriminator.getPartitionKeys());
        assertTrue("Partition keys should be empty when onlyOneCopyPer is null",
                   discriminator.getPartitionKeys().isEmpty());
    }

    @Test
    public void shouldCreateEmptyPartitionKeysWhenOnlyOneCopyPerIsEmpty() {
        TestPoolTagConstraintDiscriminator discriminator =
            new TestPoolTagConstraintDiscriminator(Collections.emptyList(), Collections.emptyMap());

        assertNotNull("Partition keys should not be null", discriminator.getPartitionKeys());
        assertTrue("Partition keys should be empty when onlyOneCopyPer is empty",
                   discriminator.getPartitionKeys().isEmpty());
    }

    @Test
    public void shouldCreatePartitionKeysFromOnlyOneCopyPerCollection() {
        List<String> tagNames = Arrays.asList("hostname", "rack", "site");
        TestPoolTagConstraintDiscriminator discriminator =
            new TestPoolTagConstraintDiscriminator(tagNames, Collections.emptyMap());

        Collection<String> partitionKeys = discriminator.getPartitionKeys();

        assertNotNull("Partition keys should not be null", partitionKeys);
        assertEquals("Partition keys should contain all tag names", 3, partitionKeys.size());
        assertTrue("Partition keys should contain 'hostname'", partitionKeys.contains("hostname"));
        assertTrue("Partition keys should contain 'rack'", partitionKeys.contains("rack"));
        assertTrue("Partition keys should contain 'site'", partitionKeys.contains("site"));
    }

    @Test
    public void shouldCreateImmutablePartitionKeys() {
        List<String> tagNames = Arrays.asList("hostname", "rack");
        TestPoolTagConstraintDiscriminator discriminator =
            new TestPoolTagConstraintDiscriminator(tagNames, Collections.emptyMap());

        Collection<String> partitionKeys = discriminator.getPartitionKeys();

        // Verify it's an ImmutableSet by checking class name or attempting modification
        assertEquals("Partition keys should be ImmutableSet",
                     "com.google.common.collect.RegularImmutableSet",
                     partitionKeys.getClass().getName());
    }

    @Test
    public void shouldHandleDuplicateTagNamesInOnlyOneCopyPer() {
        List<String> tagNamesWithDuplicates = Arrays.asList("hostname", "rack", "hostname", "site", "rack");
        TestPoolTagConstraintDiscriminator discriminator =
            new TestPoolTagConstraintDiscriminator(tagNamesWithDuplicates, Collections.emptyMap());

        Collection<String> partitionKeys = discriminator.getPartitionKeys();

        assertNotNull("Partition keys should not be null", partitionKeys);
        assertEquals("Partition keys should deduplicate tag names", 3, partitionKeys.size());
        assertTrue("Partition keys should contain 'hostname'", partitionKeys.contains("hostname"));
        assertTrue("Partition keys should contain 'rack'", partitionKeys.contains("rack"));
        assertTrue("Partition keys should contain 'site'", partitionKeys.contains("site"));
    }

    @Test
    public void shouldReturnPoolTagsFromImplementation() {
        Map<String, Map<String, String>> poolTags = ImmutableMap.of(
            "pool1", ImmutableMap.of("hostname", "host1", "rack", "r1"),
            "pool2", ImmutableMap.of("hostname", "host2", "rack", "r2")
        );

        TestPoolTagConstraintDiscriminator discriminator =
            new TestPoolTagConstraintDiscriminator(Arrays.asList("hostname"), poolTags);

        Map<String, String> pool1Tags = discriminator.getPoolTagsFor("pool1");
        Map<String, String> pool2Tags = discriminator.getPoolTagsFor("pool2");
        Map<String, String> unknownPoolTags = discriminator.getPoolTagsFor("unknown");

        assertEquals("Pool1 should have hostname=host1", "host1", pool1Tags.get("hostname"));
        assertEquals("Pool1 should have rack=r1", "r1", pool1Tags.get("rack"));
        assertEquals("Pool2 should have hostname=host2", "host2", pool2Tags.get("hostname"));
        assertEquals("Pool2 should have rack=r2", "r2", pool2Tags.get("rack"));
        assertTrue("Unknown pool should return empty tags", unknownPoolTags.isEmpty());
    }

    @Test
    public void shouldReturnAllLocationsFromGetCandidateLocations() {
        TestPoolTagConstraintDiscriminator discriminator =
            new TestPoolTagConstraintDiscriminator(Arrays.asList("hostname"), Collections.emptyMap());

        List<String> inputLocations = Arrays.asList("pool1", "pool2", "pool3");
        Collection<String> candidateLocations = discriminator.getCandidateLocations(inputLocations);

        assertEquals("Should return all input locations", 3, candidateLocations.size());
        assertTrue("Should contain pool1", candidateLocations.contains("pool1"));
        assertTrue("Should contain pool2", candidateLocations.contains("pool2"));
        assertTrue("Should contain pool3", candidateLocations.contains("pool3"));
    }
}
