package org.dcache.poolmanager;

import com.google.common.collect.Maps;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.pools.PoolCostInfo;

public class Utils {

    /*
     * no instance allowed
     */

    private Utils() {
        // static methods only
    }

    public static Map<String, PoolLinkGroupInfo> linkGroupInfos(PoolSelectionUnit selectionUnit, CostModule costModule)
    {
        return Maps.newHashMap(Maps.transformValues(selectionUnit.getLinkGroups(),
                                                    linkGroup -> toPoolLinkGroupInfo(linkGroup, costModule)));
    }

    private static PoolLinkGroupInfo toPoolLinkGroupInfo(PoolSelectionUnit.SelectionLinkGroup linkGroup,
                                                         CostModule costModule)
    {
        long linkAvailableSpace = 0;
        long linkTotalSpace = 0;

        Set<String> referencedPools = new HashSet<>();

        for (PoolSelectionUnit.SelectionLink link : linkGroup.getLinks()) {
            for (PoolSelectionUnit.SelectionPool pool : link.getPools()) {
                if (pool.isEnabled()) {
                    String poolName = pool.getName();
                    /* calculate pool space only once. This can be an issue if pool
                     * exist in to different links used by same pool group.
                     */
                    if (!referencedPools.contains(poolName)) {
                        referencedPools.add(poolName);
                        PoolCostInfo poolCostInfo =
                                costModule.getPoolCostInfo(poolName);
                        if (poolCostInfo != null) {
                            linkAvailableSpace += poolCostInfo.getSpaceInfo().getFreeSpace()
                                                  + poolCostInfo.getSpaceInfo().getRemovableSpace();
                            linkTotalSpace += poolCostInfo.getSpaceInfo().getTotalSpace();
                        }
                    }
                }
            }
        }

        return new PoolLinkGroupInfo(linkGroup, linkTotalSpace, linkAvailableSpace);
    }
}
