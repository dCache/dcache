package org.dcache.pool.migration;

import com.google.common.collect.ImmutableList;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.dcache.util.pool.PoolTagProvider;

/**
 * RefreshablePoolList decorator that filters pools to prevent multiple replicas on pools with the
 * same tag value for any tag present on the source pool(s).
 * <p>
 * For example, if a source pool has tags (rack=A, hostname=H1), any destination pool having rack=A
 * OR hostname=H1 will be excluded.
 */
public class TagConstrainedPoolList implements RefreshablePoolList {

    private final RefreshablePoolList delegate;
    private final RefreshablePoolList sourceList;
    private final PoolTagProvider tagProvider;

    private ImmutableList<PoolManagerPoolInformation> filteredPools;
    private ImmutableList<String> filteredOfflinePools;

    /**
     * Creates a tag-constrained pool list wrapper.
     *
     * @param delegate       the underlying pool list to filter
     * @param sourceList     the source pool list (pools that already have replicas)
     * @param tagProvider    the tag provider for querying pool tags
     */
    public TagConstrainedPoolList(RefreshablePoolList delegate,
          RefreshablePoolList sourceList,
          PoolTagProvider tagProvider) {
        this.delegate = delegate;
        this.sourceList = sourceList;
        this.tagProvider = tagProvider;
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
            applyTagConstraints();
        }
        return filteredPools;
    }

    @Override
    public ImmutableList<String> getOfflinePools() {
        if (filteredOfflinePools == null) {
            applyTagConstraints();
        }
        return filteredOfflinePools;
    }

    /**
     * Applies tag constraints to filter out pools that would create replicas on pools sharing
     * any tag value with existing replicas or other selected replicas.
     */
    private void applyTagConstraints() {
        ImmutableList<PoolManagerPoolInformation> allPools = delegate.getPools();
        ImmutableList<String> allOfflinePools = delegate.getOfflinePools();

        // 1. Collect tags from source pools (if any)
        Set<Map.Entry<String, String>> forbiddenTags = new HashSet<>();
        if (sourceList != null) {
            for (PoolManagerPoolInformation sourcePool : sourceList.getPools()) {
                Map<String, String> tags = tagProvider.getPoolTags(sourcePool.getName());
                if (tags != null) {
                    forbiddenTags.addAll(tags.entrySet());
                }
            }
        }

        // 2. Select Mutually Exclusive Active Pools
        filteredPools = selectMutuallyExclusivePools(allPools, forbiddenTags);

        // 3. Select Mutually Exclusive Offline Pools
        filteredOfflinePools = selectMutuallyExclusiveOfflinePools(allOfflinePools, forbiddenTags);
    }

    /**
     * Select Mutually Exclusive Active Pools By Tag.
     */
    private ImmutableList<PoolManagerPoolInformation> selectMutuallyExclusivePools(
          ImmutableList<PoolManagerPoolInformation> candidates,
          Set<Map.Entry<String, String>> baseForbiddenTags) {

        ImmutableList.Builder<PoolManagerPoolInformation> result = ImmutableList.builder();
        Set<Map.Entry<String, String>> occupiedTags = new HashSet<>(baseForbiddenTags);

        for (PoolManagerPoolInformation pool : candidates) {
            Map<String, String> tags = tagProvider.getPoolTags(pool.getName());
            if (tags == null || tags.isEmpty()) {
                // Pools with no tags don't conflict
                result.add(pool);
                continue;
            }

            if (Collections.disjoint(occupiedTags, tags.entrySet())) {
                result.add(pool);
                occupiedTags.addAll(tags.entrySet());
            }
        }
        return result.build();
    }

    /**
     * Select Mutually Exclusive Offline Pools By Tag.
     */
    private ImmutableList<String> selectMutuallyExclusiveOfflinePools(
          ImmutableList<String> candidates,
          Set<Map.Entry<String, String>> baseForbiddenTags) {

        ImmutableList.Builder<String> result = ImmutableList.builder();
        Set<Map.Entry<String, String>> occupiedTags = new HashSet<>(baseForbiddenTags);

        for (String poolName : candidates) {
            Map<String, String> tags = tagProvider.getPoolTags(poolName);
            if (tags == null || tags.isEmpty()) {
                result.add(poolName);
                continue;
            }

            if (Collections.disjoint(occupiedTags, tags.entrySet())) {
                result.add(poolName);
                occupiedTags.addAll(tags.entrySet());
            }
        }
        return result.build();
    }

    @Override
    public String toString() {
        ImmutableList<PoolManagerPoolInformation> pools = getPools();
        return (pools.isEmpty() ? "" : pools.stream().map(PoolManagerPoolInformation::getName).collect(
              Collectors.joining(",")));
    }

    @Override
    public String getBrokenMessage() {
        String msg = delegate.getBrokenMessage();
        if (msg != null) {
            return msg;
        }
        return (sourceList != null) ? sourceList.getBrokenMessage() : null;
    }
}
