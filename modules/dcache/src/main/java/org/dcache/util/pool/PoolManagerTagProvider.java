package org.dcache.util.pool;

import com.google.common.collect.ImmutableMap;
import diskCacheV111.vehicles.PoolManagerGetPoolMonitor;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.dcache.cells.CellStub;
import org.dcache.poolmanager.PoolInfo;
import org.dcache.poolmanager.SerializablePoolMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of PoolTagProvider that retrieves pool tags by querying the PoolManager.
 * This provider can obtain tag information for any pool in the system by requesting
 * the pool monitor from the pool manager and using its cost module.
 */
public class PoolManagerTagProvider implements PoolTagProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(PoolManagerTagProvider.class);

    private final CellStub poolManagerStub;
    private SerializablePoolMonitor cachedPoolMonitor;
    private long lastRefreshTime = 0;
    private static final long CACHE_DURATION_MS = TimeUnit.MINUTES.toMillis(5); // Cache for 5 minutes

    public PoolManagerTagProvider(CellStub poolManagerStub) {
        this.poolManagerStub = poolManagerStub;
    }

    @Override
    public Map<String, String> getPoolTags(String location) {
        if (poolManagerStub == null) {
            return ImmutableMap.of();
        }

        try {
            SerializablePoolMonitor poolMonitor = getPoolMonitor();
            if (poolMonitor != null && poolMonitor.getCostModule() != null) {
                PoolInfo poolInfo = poolMonitor.getCostModule().getPoolInfo(location);
                if (poolInfo != null) {
                    Map<String, String> tags = poolInfo.getTags();
                    return tags != null ? tags : ImmutableMap.of();
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to query pool tags for pool '{}' from PoolManager: {}", location, e.getMessage());
        }

        return ImmutableMap.of();
    }

    /**
     * Get the pool monitor from the pool manager, using caching to avoid excessive queries.
     */
    private SerializablePoolMonitor getPoolMonitor() {
        long currentTime = System.currentTimeMillis();

        // Use cached monitor if it's still fresh
        if (cachedPoolMonitor != null && (currentTime - lastRefreshTime) < CACHE_DURATION_MS) {
            return cachedPoolMonitor;
        }

        try {
            PoolManagerGetPoolMonitor request = new PoolManagerGetPoolMonitor();
            PoolManagerGetPoolMonitor response = poolManagerStub.sendAndWait(request, TimeUnit.MINUTES.toMillis(1));

            if (response.getReturnCode() == 0 && response.getPoolMonitor() != null) {
                cachedPoolMonitor = response.getPoolMonitor();
                lastRefreshTime = currentTime;
                return cachedPoolMonitor;
            } else {
                LOGGER.warn("Failed to get pool monitor from PoolManager: return code {}", response.getReturnCode());
            }
        } catch (Exception e) {
            LOGGER.warn("Exception while querying pool monitor from PoolManager: {}", e.getMessage());
        }

        return null;
    }
}
