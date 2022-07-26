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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.dcache.qos.services.verifier.data.PoolInfoMap;
import org.dcache.qos.util.RepositoryReplicaVerifier;

/**
 * Implementation of the {@link ConstraintDiscriminator}, but specifically for evicting/caching
 * locations based on pool tags.<
 * <p/>
 * The algorithm is similar to the one described in {@link AbstractLocationExtractor}.
 */
public final class PoolTagLocationExtractor extends ConstraintDiscriminator
      implements EvictingLocationExtractor {

    class WeightedLocation {

        final String location;
        int weight;

        public WeightedLocation(String location, int weight) {
            this.location = location;
            this.weight = weight;
        }

        void incrementBy(int count) {
            weight += count;
        }
    }

    private final Multimap<String, String> tagValuesByLocation = ArrayListMultimap.create();
    private final Map<String, WeightedLocation> weights = new HashMap<>();
    private final PoolInfoMap info;

    private int maximal = 0;

    public PoolTagLocationExtractor(Collection<String> onlyOneCopyPer,
          PoolInfoMap info) {
        super(onlyOneCopyPer);
        this.info = info;
    }

    @Override
    public List<String> getCandidateLocations(Collection<String> locations) {
        return extractMaximal(calculateWeights(locations));
    }

    public int getLastComputedMaximum() {
        return maximal;
    }

    @Override
    protected Map<String, String> getKeyValuesFor(String location) {
        return info.getTags(location);
    }

    private Map<String, WeightedLocation>
    calculateWeights(Collection<String> locations) {
        locations.stream()
              .forEach((l) -> weights.put(l, new WeightedLocation(l, 0)));

        for (String tag : partitionKeys) {
            for (String location : locations) {
                Map<String, String> tags = getKeyValuesFor(location);

                if (tags.isEmpty()) {
                    continue;
                }

                String value = tags.get(tag);
                if (value != null) {
                    tagValuesByLocation.put(value, location);
                }
            }

            for (String value : tagValuesByLocation.keySet()) {
                Collection<String> locs = tagValuesByLocation.get(value);
                int count = locs.size();
                for (String location : locs) {
                    weights.get(location).incrementBy(count);
                }
            }

            tagValuesByLocation.clear();
        }

        return weights;
    }

    public Optional<String> findALocationToEvict(Collection<String> locations,
          Collection verified) {
        Set<String> seen = new HashSet<>();
        for (String location : locations) {
            Map<String, String> tags = getKeyValuesFor(location);
            for (String tag : partitionKeys) {
                if (tags.containsKey(tag)) {
                    String nameValue = tag + tags.get(tag);
                    if (seen.contains(nameValue)
                          && RepositoryReplicaVerifier.isRemovable(location, verified)) {
                        return Optional.of(location);
                    }
                    seen.add(nameValue);
                }
            }
        }
        return Optional.empty();
    }

    private List<String> extractMaximal(Map<String, WeightedLocation> weights) {
        final int max = getMaximal(weights.values());
        List<String> maximal = weights.values().stream()
              .filter((w) -> w.weight == max)
              .map((w) -> w.location)
              .collect(Collectors.toList());
        weights.clear();
        return maximal;
    }

    private int getMaximal(Collection<WeightedLocation> weightedLocations) {
        maximal = 0;
        for (WeightedLocation location : weightedLocations) {
            if (location.weight > maximal) {
                maximal = location.weight;
            }
        }
        return maximal;
    }
}
