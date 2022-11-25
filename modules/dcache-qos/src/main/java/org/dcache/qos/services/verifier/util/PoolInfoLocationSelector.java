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
package org.dcache.qos.services.verifier.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.dcache.pool.migration.PoolSelectionStrategy;
import org.dcache.qos.data.FileQoSRequirements;
import org.dcache.qos.services.verifier.data.PoolInfoMap;
import org.dcache.qos.services.verifier.data.VerifyOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the selection of locations from the underlying pool info map information. Encapsulates
 * the logic involved in extracting sets of valid readable or writable pools in connection with the
 * verification operation.
 */
public final class PoolInfoLocationSelector implements LocationSelector {

    private static final Logger LOGGER = LoggerFactory.getLogger(PoolInfoLocationSelector.class);

    private PoolInfoMap poolInfoMap;
    private PoolSelectionStrategy poolSelectionStrategy;

    public String selectCopySource(VerifyOperation operation,
          Set<String> locations)
          throws LocationSelectionException {
        LOGGER.debug("selecting copy source for {} from {}.", operation.getPnfsId(), locations);
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

    public String selectCopyTarget(VerifyOperation operation,
          String poolGroup,
          Collection<String> locations,
          Collection<String> tags)
          throws LocationSelectionException {
        LOGGER.debug("selecting copy target for {} from {}; pool group {}, tags {}.",
              operation.getPnfsId(), locations, poolGroup, tags);
        return selectCopyTarget(poolGroup, locations, operation.getTried(), tags);
    }

    public String selectTargetToPersist(FileQoSRequirements requirements,
          Collection<String> sticky,
          Collection<String> notSticky,
          Collection<String> tags) {
        if (notSticky.isEmpty()) {
            return null;
        }

        LOGGER.debug("selecting target for {} from possible non-sticky locations",
              requirements.getPnfsId());
        return selectTargetToPersist(sticky, notSticky, tags);
    }

    public String selectTargetToCache(FileQoSRequirements requirements,
          Collection<String> sticky,
          Collection<String> locations,
          Collection<String> tags)
          throws LocationSelectionException {
        LOGGER.debug("selecting target to cache for {} from {}; tags {}.",
              requirements.getPnfsId(),
              locations,
              tags);
        if (sticky.size() == 1) {
            String message = String.format("cacheable replica was selected, but "
                        + "the principal pool %s is the "
                        + "only location; this is a bug.",
                  locations.iterator().next());
            throw new LocationSelectionException(message);
        }
        return selectTargetToCache(locations, tags);
    }

    public void setPoolInfoMap(PoolInfoMap poolInfoMap) {
        this.poolInfoMap = poolInfoMap;
    }

    public void setPoolSelectionStrategy(PoolSelectionStrategy poolSelectionStrategy) {
        this.poolSelectionStrategy = poolSelectionStrategy;
    }

    private Set<String> getEligibleCopyTargets(String pgroup,
          Collection<String> locations,
          Set<String> tried) {
        Collection<String> pools = poolInfoMap.getPoolsOfGroup(pgroup);
        Set<String> writableMembers = poolInfoMap.getValidLocations(pools, true);
        Set<String> locset = new HashSet(locations);
        Set<String> valid = Sets.difference(writableMembers, locset);
        return Sets.difference(valid, tried);
    }

    private Set<String> getEligibleCacheableTargets(Collection<String> locations) {
        return poolInfoMap.getValidLocations(locations, true);
    }

    @VisibleForTesting
    String selectTargetToPersist(Collection<String> sticky,
          Collection<String> notSticky,
          Collection<String> oneCopyPer) {
        Set<String> p2p = poolInfoMap.getValidLocations(notSticky, false);
        Set<String> possible = eliminateNonDeliverableLocations(p2p);

        /*
         * Filter by tag constraints.
         */
        AbstractLocationExtractor extractor = poolInfoMap.getLocationExtractor(oneCopyPer);
        sticky.forEach(extractor::addSeenTagsFor);
        return extractor.getCandidateLocations(possible).stream().findFirst().orElse(null);
    }

    /*
     *  Filters first the writable pools in the pool group which do not yet have a replica
     *  of this file, then applies any tag-induced partitioning on the basis of the tags
     *  of replicas that already exist. From that list, it then chooses the pool using the
     *  configured poolSelectionStrategy.
     */
    @VisibleForTesting
    String selectCopyTarget(String pgroup,
          Collection<String> locations,
          Set<String> tried,
          Collection<String> oneCopyPer)
          throws LocationSelectionException {
        /*
         *  Writable locations in the pool group without a copy of this file,
         *  or locations which exist which are not sticky.
         */
        Set<String> possible = getEligibleCopyTargets(pgroup, locations, tried);

        /*
         *  Filter by tag constraints.
         */
        AbstractLocationExtractor extractor = poolInfoMap.getLocationExtractor(oneCopyPer);
        locations.forEach(extractor::addSeenTagsFor);
        Collection<String> candidates = extractor.getCandidateLocations(possible);

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
        for (String pool : candidates) {
            /*
             *  throws InterruptedException
             */
            info.add(poolInfoMap.getPoolManagerInfo(pool));
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

    /*
     *  Filters first the writable pools in the pool group which contain a replica of this file,
     *  then applies any tag-induced partitioning on the basis of the tags of replicas that
     *  already exist to select a maximally constrained pool. If more than one pool has the same
     *  weight, one of them is chosen randomly.
     */
    @VisibleForTesting
    String selectTargetToCache(Collection<String> locations, Collection<String> oneCopyPer)
          throws LocationSelectionException {
        Set<String> possible = getEligibleCacheableTargets(locations);
        ConstraintDiscriminator extractor = new PoolTagLocationExtractor(oneCopyPer,
              poolInfoMap);
        Collection<String> maximallyConstrained = extractor.getCandidateLocations(possible);
        String target = RandomSelectionStrategy.SELECTOR.apply(maximallyConstrained);

        if (target == null) {
            throw new LocationSelectionException(String.format("Cannot satisfy "
                  + "cache request because the selection algorithm "
                  + "returned no viable locations: locations: %s; "
                  + "possible: %s", locations, possible));
        }

        return target;
    }

    /*
     *  Chooses a source randomly from among the readable locations which have not yet been tried.
     */
    @VisibleForTesting
    String selectSource(Set<String> readable, Collection<String> tried)
          throws LocationSelectionException {
        Set<String> excluded = new HashSet<>(tried);
        Set<String> possible = Sets.difference(readable, excluded);
        if (possible.isEmpty()) {
            throw new LocationSelectionException(String.format("Cannot find  "
                  + "a readable source because there "
                  + "are no other viable locations; "
                  + "readable: %s; tried: %s", readable, excluded));
        }

        return RandomSelectionStrategy.SELECTOR.apply(possible);
    }

    /*
     *  Replicas may end up on pools (for instance, via staging) that are not accessible to
     *  external reads.
     *
     *  We need to ensure that we don't set the sticky bit on cache locations
     *  that are not readable to external clients.   The 'p2p' enabled requirement
     *  (PoolV2Mode) for QoS-writable files is not sufficient (since this has to do
     *  with the disable command in the PoolSelectionUnit, and not link preferences).
     */
    private Set<String> eliminateNonDeliverableLocations(Set<String> cached) {
        return cached.stream().filter(p -> !poolInfoMap.isReadPref0(p)).collect(Collectors.toSet());
    }
}
