package org.dcache.tests.poolmanager;

import diskCacheV111.poolManager.CostModuleV1;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.vehicles.PoolManagerPoolUpMessage;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellMessage;

import org.dcache.pool.classic.IoQueueManager;

public class PoolCostInfoHelper {


    public static void setCost(CostModuleV1 cm, String pool,long total , long free , long precious , long removable ){


        PoolV2Mode poolMode = new PoolV2Mode(PoolV2Mode.ENABLED);
        long serialId = System.currentTimeMillis();

        PoolCostInfo poolCost = new PoolCostInfo(pool, IoQueueManager.DEFAULT_QUEUE);

        poolCost.setSpaceUsage(total, free, precious, removable);

        CellMessage envelope = new CellMessage(new CellAddressCore("PoolManager"), null);
        envelope.addSourceAddress(new CellAddressCore(pool));
        PoolManagerPoolUpMessage poolUpMessage = new PoolManagerPoolUpMessage(pool,
                serialId, poolMode, poolCost);

        cm.messageArrived(envelope, poolUpMessage);
    }

}
