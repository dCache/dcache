package org.dcache.pool.migration;

import com.google.common.collect.ImmutableList;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.dcache.util.pool.PoolTagBasedExtractor;
import org.dcache.util.pool.PoolTagProvider;

/**
 * RefreshablePoolList decorator that filters pools to prevent multiple replicas on pools with the
 * same hostname tag value.
 */
public class HostnameConstrainedPoolList implements RefreshablePoolList {

    private final RefreshablePoolList delegate;
    private final RefreshablePoolList sourceList;
    private final PoolTagBasedExtractor hostnameExtractor;

    private ImmutableList<PoolManagerPoolInformation> filteredPools;
    private ImmutableList<String> filteredOfflinePools;

    /**
     * Creates a hostname-constrained pool list wrapper using pool tag-based hostname extraction.
     *
     * @param delegate       the underlying pool list to filter
     * @param sourceList     the source pool list (pools that already have replicas)
     * @param constraintTags the tag names to use for hostname constraints (e.g., "hostname")
     * @param tagProvider    the tag provider for querying pool tags
     */
    public HostnameConstrainedPoolList(RefreshablePoolList delegate,
          RefreshablePoolList sourceList,
          Collection<String> constraintTags,
          PoolTagProvider tagProvider) {
        this.delegate = delegate;
        this.sourceList = sourceList;
        this.hostnameExtractor = new PoolTagBasedExtractor(
              constraintTags != null ? constraintTags : Collections.singletonList("hostname"),
              tagProvider);
    }

    @Override
    public void refresh() {
        delegate.refresh();
        if (sourceList != null) {
            sourceList.refresh();
        }
        // Invalidate cached filtered results to force re-filtering
        filteredPools = null;
        filteredOfflinePools = null;
    }

    @Override
    public boolean isValid() {
        return delegate.isValid() && (sourceList == null || sourceList.isValid());
    }

    @Override
    public ImmutableList<PoolManagerPoolInformation> getPools() {
        if (filteredPools == null) {
            applyHostnameConstraints();
        }
        return filteredPools;
    }

    @Override
    public ImmutableList<String> getOfflinePools() {
        if (filteredOfflinePools == null) {
            applyHostnameConstraints();
        }
        return filteredOfflinePools;
    }

    /**
     * Applies hostname constraints to filter out pools that would create replicas on the same host
     * as existing replicas.
     */
    private void applyHostnameConstraints() {
        ImmutableList<PoolManagerPoolInformation> allPools = delegate.getPools();
        ImmutableList<String> allOfflinePools = delegate.getOfflinePools();

        if (sourceList == null) {
            // No source list, no constraints to apply
            filteredPools = allPools;
            filteredOfflinePools = allOfflinePools;
            return;
        }

        // Reset the extractor's state (clear seen tags)
        hostnameExtractor.reset();

        // Feed existing replica locations to the extractor
        ImmutableList<PoolManagerPoolInformation> sourcePools = sourceList.getPools();
        for (PoolManagerPoolInformation sourcePool : sourcePools) {
            hostnameExtractor.addSeenTagsFor(sourcePool.getName());
        }

        // Filter active pools
        // We need to pass just the names to the extractor, then map back to objects
        List<String> candidateNames = new ArrayList<>(allPools.size());
        for (PoolManagerPoolInformation pool : allPools) {
            candidateNames.add(pool.getName());
        }

        Collection<String> validNames = hostnameExtractor.getCandidateLocations(candidateNames);

        filteredPools = allPools.stream()
              .filter(p -> validNames.contains(p.getName()))
              .collect(ImmutableList.toImmutableList());

        // Filter offline pools
        Collection<String> validOfflineNames = hostnameExtractor.getCandidateLocations(
              allOfflinePools);
        filteredOfflinePools = ImmutableList.copyOf(validOfflineNames);
    }

    @Override
    public String toString() {
        ImmutableList<PoolManagerPoolInformation> pools = getPools();
        if (pools.isEmpty()) {
            return "";
        }

        StringBuilder s = new StringBuilder();
        s.append(pools.get(0).getName());
        for (int i = 1; i < pools.size(); i++) {
            s.append(',').append(pools.get(i).getName());
        }
        return s.toString();
    }
}
