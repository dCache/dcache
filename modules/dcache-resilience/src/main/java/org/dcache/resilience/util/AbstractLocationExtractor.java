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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * <p>Implementation of the {@link PoolTagConstraintDiscriminator} which returns
 *      a list containing the pools or locations with zero weight.  The weights
 *      are computed in terms of the sum of the size of the value partitions
 *      to which they belong.  For instance, if pool1 has tag1 with value A and
 *      tag2 with value X, and A is shared by 3 other pools and X by 2, then
 *      pool1 has a weight of 5.</p>
 *
 * <p>The use case envisaged here is as follows:  the component has a set
 *      of available locations from which it must choose one or more for
 *      replication. First, it will add the tags of all current replica locations
 *      to the seenTags map. It will then run getCandidateLocations() on the
 *      list one or more times, each time picking a location from the returned
 *      list using some selection algorithm, removing this location from the
 *      available list and adding its tags to the seenTags map, and finally
 *      passing the modified available list in to any further iteration.</p>
 */
public abstract class AbstractLocationExtractor extends PoolTagConstraintDiscriminator {
    protected final Multimap<String, String> seenTags = HashMultimap.create();

    protected AbstractLocationExtractor(Collection<String> onlyOneCopyPer) {
        super(onlyOneCopyPer);
    }

    /**
     * <p>Add tag names and values for the location/pool
     *      to the map of seen tags.</p>
     *
     * @param location which has been selected.
     */
    public void addSeenTagsFor(String location) {
        Map<String, String> tags = getPoolTagsFor(location);

        if (tags == null || tags.isEmpty()) {
            return;
        }

        for (String tagName : partitionKeys) {
            String tagValue = tags.get(tagName);

            if (tagValue == null) {
                continue;
            }

            seenTags.put(tagName, tagValue);
        }
    }

    @Override
    public Collection<String> getCandidateLocations(Collection<String> locations) {
        return locations.stream().filter((l) -> checkPartitionConstraints(l) == 0)
                        .collect(Collectors.toList());
    }

    public void reset() {
        seenTags.clear();
    }

    /**
     * @param location to examine
     * @return the number of tags whose values match values already
     *          stored in the seenTags map –– that is, of locations/pools
     *          which either already exist or have been selected.
     */
    private int checkPartitionConstraints(String location) {
        Map<String, String> tags = getPoolTagsFor(location);

        /*
         *  An "undefined" situation.  For maximum flexibility,
         *  we allow the pool to be selected.
         */
        if (tags == null || tags.isEmpty()) {
            return 0;
        }

        int violations = 0;

        for (String tagName : partitionKeys) {
            String tagValue = tags.get(tagName);

            /*
             *  Again, this is an "undefined" situation, so
             *  we provisionally qualify the pool.
             */
            if (tagValue == null) {
                continue;
            }

            Collection<String> values = seenTags.get(tagName);

            if (values != null && values.contains(tagValue)) {
                ++violations;
            }
        }

        return violations;
    }
}
