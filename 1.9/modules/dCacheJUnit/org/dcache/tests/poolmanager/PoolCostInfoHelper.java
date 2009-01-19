package org.dcache.tests.poolmanager;

import diskCacheV111.poolManager.CostModule;

import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.vehicles.PoolManagerPoolUpMessage;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;

public class PoolCostInfoHelper {


    public static void setCost(CostModule cm, String pool,long total , long free , long precious , long removable ){


        PoolV2Mode poolMode = new PoolV2Mode(PoolV2Mode.ENABLED);
        long serialId = System.currentTimeMillis();

        PoolCostInfo poolCost = new PoolCostInfo(pool);

        poolCost.setSpaceUsage(total, free, precious, removable);


        PoolManagerPoolUpMessage poolUpMessage = new PoolManagerPoolUpMessage(pool,
                serialId, poolMode, poolCost);

        CellMessage cellMessage  = new CellMessage( new CellPath("CostModule"), poolUpMessage);

        cm.messageArrived(cellMessage);

    }

}
