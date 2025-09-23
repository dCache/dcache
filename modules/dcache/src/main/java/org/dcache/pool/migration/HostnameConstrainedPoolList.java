package org.dcache.pool.migration;

import com.google.common.collect.ImmutableList;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.dcache.util.pool.PoolTagBasedExtractor;
import org.dcache.util.pool.PoolTagProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RefreshablePoolList decorator that filters pools to prevent multiple replicas
 * on pools with the same hostname tag value.
 */
public class HostnameConstrainedPoolList implements RefreshablePoolList {

    private static final Logger LOGGER = LoggerFactory.getLogger(HostnameConstrainedPoolList.class);

    private final RefreshablePoolList delegate;
    private final RefreshablePoolList sourceList;
    private final PoolTagBasedExtractor hostnameExtractor;

    private ImmutableList<PoolManagerPoolInformation> filteredPools;
    private ImmutableList<String> filteredOfflinePools;

    /**
     * Creates a hostname-constrained pool list wrapper using pool tag-based hostname extraction.
     *
     * @param delegate the underlying pool list to filter
     * @param sourceList the source pool list (pools that already have replicas)
     * @param constraintTags the tag names to use for hostname constraints (e.g., "hostname")
     * @param tagProvider the tag provider for querying pool tags
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
     * Applies hostname constraints to filter out pools that would create
     * replicas on the same host as existing replicas.
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

        // Get source pools once to avoid multiple calls
        ImmutableList<PoolManagerPoolInformation> sourcePools = sourceList.getPools();

        if (sourcePools.isEmpty()) {
            // No existing replicas, no constraints to apply
            filteredPools = allPools;
            filteredOfflinePools = allOfflinePools;
            return;
        }

        // Add hostname tags from existing source pools (pools that already have replicas)
        for (PoolManagerPoolInformation sourcePool : sourcePools) {
            hostnameExtractor.addSeenTagsFor(sourcePool.getName());
        }

        // Filter candidate pools to exclude those with conflicting hostname tags
        List<String> candidatePoolNames = allPools.stream()
                .map(PoolManagerPoolInformation::getName)
                .collect(Collectors.toList());

        Collection<String> validPoolNames = hostnameExtractor.getCandidateLocations(candidatePoolNames);

        // Filter the pools based on valid pool names
        List<PoolManagerPoolInformation> validPools = allPools.stream()
                .filter(pool -> validPoolNames.contains(pool.getName()))
                .collect(Collectors.toList());

        // Filter offline pools based on hostname constraints using the extractor
        Collection<String> validOfflinePoolNames = hostnameExtractor.getCandidateLocations(allOfflinePools);
        List<String> validOfflinePools = allOfflinePools.stream()
                .filter(validOfflinePoolNames::contains)
                .collect(Collectors.toList());

        filteredPools = ImmutableList.copyOf(validPools);
        filteredOfflinePools = ImmutableList.copyOf(validOfflinePools);

        LOGGER.debug("Applied hostname constraints: {} pools -> {} pools, {} offline -> {} offline",
                allPools.size(), filteredPools.size(),
                allOfflinePools.size(), filteredOfflinePools.size());
    }
}
