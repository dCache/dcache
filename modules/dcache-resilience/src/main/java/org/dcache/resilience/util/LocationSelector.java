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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import diskCacheV111.vehicles.PoolManagerPoolInformation;
import org.dcache.pool.migration.PoolSelectionStrategy;
import org.dcache.resilience.data.FileOperation;
import org.dcache.resilience.data.PoolInfoMap;

/**
 * <p>Manages the selection of locations from the underlying pool map
 *      information.  Encapsulates the logic involved in extracting
 *      sets of valid readable or writable pools in connection with
 *      the verify, copy and remove phases of the pnfs operation.</p>
 */
public final class LocationSelector {
    private static final Logger LOGGER = LoggerFactory.getLogger(
                    LocationSelector.class);

    private PoolInfoMap poolInfoMap;
    private PoolSelectionStrategy poolSelectionStrategy;

    public Set<String> getReadableMemberLocations(Integer gindex,
                    Collection<String> locations) {
        return poolInfoMap.getMemberPools(gindex, locations, false);
    }

    public String selectCopySource(FileOperation operation,
                    Set<String> locations) throws LocationSelectionException {
        LOGGER.trace("selecting source for {}", operation);
        if (locations.size() == 1) {
            if (!operation.getTried().isEmpty()) {
                throw new LocationSelectionException(String.format("Cannot find "
                                + "a new source because only one exists:  %s.",
                                locations));
            }
            return locations.iterator().next();
        }
        return selectSource(locations, operation.getTried());
    }

    public String selectCopyTarget(FileOperation operation, Integer gindex,
                                   Collection<String> locations, Collection<String> tags)
                    throws LocationSelectionException {
        LOGGER.trace("selecting target for {}", operation);
        return selectCopyTarget(gindex, locations, operation.getTried(), tags);
    }

    public String selectRemoveTarget(FileOperation operation,
                    Collection<String> locations, Collection<String> tags)
                    throws LocationSelectionException {
        LOGGER.trace("selecting target for {}", operation);
        if (locations.size() == 1) {
            String message = String.format("Remove replica was selected, but "
                                            + "the principal pool %s is the "
                                            + "only location; this is a bug.",
                            locations.iterator().next());
            throw new LocationSelectionException(message);
        }
        return selectRemoveTarget(locations, tags);
    }

    public void setPoolInfoMap(PoolInfoMap poolInfoMap) {
        this.poolInfoMap = poolInfoMap;
    }

    public void setPoolSelectionStrategy(PoolSelectionStrategy poolSelectionStrategy) {
        this.poolSelectionStrategy = poolSelectionStrategy;
    }

    private Set<String> getEligibleCopyTargets(Integer gindex,
                    Collection<String> locations, Set<Integer> tried) {
        Collection<Integer> pools = poolInfoMap.getPoolsOfGroup(gindex);
        Set<Integer> writableMembers = poolInfoMap.getValidLocations(pools,
                        true);
        Set<Integer> lindices = poolInfoMap.getPoolIndices(locations);
        Set<Integer> valid = Sets.difference(writableMembers, lindices);
        valid = Sets.difference(valid, tried);
        return poolInfoMap.getPools(valid);
    }

    private Set<String> getEligibleRemoveTargets(Collection<String> locations) {
        Collection<Integer> indices = poolInfoMap.getPoolIndices(locations);
        indices = poolInfoMap.getValidLocations(indices, true);
        return poolInfoMap.getPools(indices);
    }

    /**
     * <p>Filters first the writable pools in the pool group which do
     *      not yet have a copy of this pnfsid, then applies any tag-induced
     *      partitioning on the basis of the tags of copies that already exist.
     *      From that list, it then chooses the pool using the
     *      configured poolSelectionStrategy.</p>
     */
     @VisibleForTesting
     String selectCopyTarget(Integer gindex,
                    Collection<String> locations, Set<Integer> tried,
                    Collection<String> oneCopyPer) throws LocationSelectionException {
        /*
         *  Writable locations in the pool group without a copy of this
         *  pnfsId.
         */
        Set<String> possible = getEligibleCopyTargets(gindex, locations, tried);

        /*
         * Filter by tag constraints.
         */
        CopyLocationExtractor extractor = new CopyLocationExtractor(oneCopyPer,
                        poolInfoMap);
        locations.stream().forEach(extractor::addSeenTagsFor);
        Collection<String> candidates = extractor.getCandidateLocations(
                        possible);

        if (candidates.isEmpty()) {
            throw new LocationSelectionException(String.format("Cannot satisfy "
                            + "copy request because there are no (further) "
                            + "possible locations; candidates %s", candidates));
        }

        /*
         *  Get pool cost for the candidates, then select the one using
         *  the inject poolSelectionStrategy.
         */
        List<PoolManagerPoolInformation> info = new ArrayList<>();
        for (String c : candidates) {
            /*
             * throws InterruptedException
             */
            info.add(poolInfoMap.getPoolManagerInfo(poolInfoMap.getPoolIndex(c)));
        }

        PoolManagerPoolInformation target = poolSelectionStrategy.select(info);

        LOGGER.debug("Pool selection poolSelectionStrategy "
                        + "selected {} as copy target.", target);

        if (target == null) {
            throw new LocationSelectionException(String.format("Cannot satisfy "
                            + "copy request because the selection "
                            + "algorithm returned no viable locations; "
                            + "locations: %s; possible %s", locations, candidates));
        }

        return target.getName();
    }

    /**
     * <p>Filters first the writable pools in the pool group which contain a
     *      copy of this pnfsid, then applies any tag-induced partitioning on
     *      the basis of the tags of copies that already exist to select a
     *      maximally constrained pool.  If more than one pool has the same
     *      weight, one of them is chosen randomly.</p>
     */
    @VisibleForTesting
    String selectRemoveTarget(Collection<String> locations,
                    Collection<String> oneCopyPer) throws LocationSelectionException {
        Set<String> possible = getEligibleRemoveTargets(locations);
        RemoveLocationExtractor extractor = new RemoveLocationExtractor(
                        oneCopyPer, poolInfoMap);
        List<String> maximallyConstrained = extractor.getCandidateLocations(
                        possible);
        String target = RandomSelectionStrategy.SELECTOR.apply(maximallyConstrained);

        if (target == null) {
            throw new LocationSelectionException(String.format("Cannot satisfy "
                            + "remove request because the selection algorithm "
                            + "returned no viable locations: locations: %s; "
                            + "possible: %s", locations, possible));
        }

        return target;
    }

    /**
     * <p>Chooses a source randomly from among the readable locations which have
     *      not yet been tried.</p>
     */
    @VisibleForTesting
    String selectSource(Set<String> readable, Collection<Integer> tried)
                    throws LocationSelectionException {
        Set<String> excluded = poolInfoMap.getPools(tried);
        Set<String> possible = Sets.difference(readable, excluded);
        if (possible.isEmpty()) {
            throw new LocationSelectionException(String.format("Cannot find  "
                            + "a readable source because there "
                            + "are no other viable locations; "
                            + "readable: %s; tried: %s", readable, excluded));
        }

        return RandomSelectionStrategy.SELECTOR.apply(possible);
    }
}
