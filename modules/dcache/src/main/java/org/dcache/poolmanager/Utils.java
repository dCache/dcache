package org.dcache.poolmanager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.vehicles.PoolLinkGroupInfo;

public class Utils {

    /*
     * no instance allowed
     */

    private Utils() {
        // static methods only
    }



    public static Map<String, PoolLinkGroupInfo> linkGroupInfos(PoolSelectionUnit selectionUnit, CostModule costModule ) {

        String [] linkGroups = selectionUnit.getLinkGroups();
        Map<String, PoolLinkGroupInfo> linkGroupInfos = new HashMap<String, PoolLinkGroupInfo>(linkGroups.length);

        /*
         * get list of all defined link groups
         * for each link group get list of links
         * for each link in the group find all active pools and
         * calculate available space ( free + removable )
         */


        for (String linkGroup1 : linkGroups) {

            String[] links = selectionUnit.getLinksByGroupName(linkGroup1);
            long linkAvailableSpace = 0;
            long linkTotalSpace = 0;

            Set<String> referencedPools = new HashSet<String>();

            for (String link : links) {

                PoolSelectionUnit.SelectionLink selectionLink = selectionUnit
                        .getLinkByName(link);

                for (PoolSelectionUnit.SelectionPool pool : selectionLink
                        .pools()) {

                    if (pool.isEnabled()) {
                        String poolName = pool.getName();
                        /*
                        * calculate pool space only once. This can be an issue if pool exist in to different links
                        * used by same pool group
                        */
                        if (!referencedPools.contains(poolName)) {
                            referencedPools.add(poolName);
                            PoolCostInfo poolCostInfo = costModule
                                    .getPoolCostInfo(poolName);
                            if (poolCostInfo != null) {
                                linkAvailableSpace += poolCostInfo
                                        .getSpaceInfo()
                                        .getFreeSpace() + poolCostInfo
                                        .getSpaceInfo().getRemovableSpace();
                                linkTotalSpace += poolCostInfo.getSpaceInfo()
                                        .getTotalSpace();
                            }
                        }
                    }
                }
            }

            PoolSelectionUnit.SelectionLinkGroup linkGroup = selectionUnit
                    .getLinkGroupByName(linkGroup1);
            PoolLinkGroupInfo linkGroupInfo = new PoolLinkGroupInfo(linkGroup, linkTotalSpace, linkAvailableSpace);
            linkGroupInfos.put(linkGroup1, linkGroupInfo);
        }

        return linkGroupInfos;
    }

}
