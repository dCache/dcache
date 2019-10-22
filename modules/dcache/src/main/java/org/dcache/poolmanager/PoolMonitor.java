package org.dcache.poolmanager;

import java.util.Collection;
import java.util.Set;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.util.FileLocality;
import diskCacheV111.vehicles.ProtocolInfo;

import org.dcache.vehicles.FileAttributes;

/**
 * A PoolMonitor is the primary component for selecting pools.
 *
 * PoolSelectionUnit, CostModule and PartitionManager are collaborators of
 * PoolMonitor. PoolMonitor is however more than just a facade, as it provides
 * functionality not found in its collaborators.
 */
public interface PoolMonitor
{
    PoolSelectionUnit getPoolSelectionUnit();

    CostModule getCostModule();

    PartitionManager getPartitionManager();

    PoolSelector getPoolSelector(
            FileAttributes fileAttributes,
            ProtocolInfo protocolInfo,
            String linkGroup,
            Set<String> excludedHosts);

    Collection<PoolCostInfo> queryPoolsByLinkName(String linkName);

    FileLocality getFileLocality(FileAttributes attributes, String hostName);
}
