package org.dcache.tests.poolmanager;

import org.junit.Before;
import org.junit.Test;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;

import static org.junit.Assert.*;

import org.dcache.tests.cells.CellAdapterHelper;

import diskCacheV111.poolManager.CostModuleV1;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.vehicles.PoolCheckable;
import diskCacheV111.vehicles.PoolCostCheckable;
import diskCacheV111.vehicles.PoolManagerPoolUpMessage;

public class CostModuleTest {

    private final static CellAdapterHelper _cell = new CellAdapterHelper( "CostModuleTest", "");
    private CostModuleV1 _costModule;

    @Before
    public void setUp() throws Exception {
        _costModule = new CostModuleV1();
        _costModule.setCellEndpoint(_cell);
    }

    @Test
    public void testPoolNotExist() throws Exception {

        assertNull("should return null on non existing pool", _costModule.getPoolCostInfo("aPool"));

    }


    @Test
    public void testPoolUp() throws Exception {

        long serialId = System.currentTimeMillis();

        PoolV2Mode poolMode = new PoolV2Mode(PoolV2Mode.ENABLED);

        PoolManagerPoolUpMessage poolUpMessage = new PoolManagerPoolUpMessage("aPool",
                serialId, poolMode);

        CellMessage cellMessage = new CellMessage( new CellPath("CostModule"), poolUpMessage);

        _costModule.messageArrived(cellMessage);

        assertNull("should return null on a pool without costInfo", _costModule.getPoolCostInfo("aPool"));

    }

    @Test
    public void testPoolUpWithCost() throws Exception {

        long serialId = System.currentTimeMillis();

        PoolV2Mode poolMode = new PoolV2Mode(PoolV2Mode.ENABLED);

        PoolCostInfo poolCost = new PoolCostInfo("aPool");

        poolCost.setSpaceUsage(100, 20, 30, 50);


        PoolManagerPoolUpMessage poolUpMessage = new PoolManagerPoolUpMessage("aPool",
                serialId, poolMode, poolCost);

        CellMessage cellMessage = new CellMessage( new CellPath("CostModule"), poolUpMessage);

        _costModule.messageArrived(cellMessage);

        PoolCostInfo recivedCost = _costModule.getPoolCostInfo("aPool");

        assertNotNull("should return non null cost on a pool with costInfo", recivedCost);

    }


    @Test
    public void testPoolUpAndDown() throws Exception {

        CellMessage cellMessage = null;
        PoolCostInfo recivedCost = null;

        long serialId = System.currentTimeMillis();

        PoolV2Mode poolMode = new PoolV2Mode(PoolV2Mode.ENABLED);

        PoolCostInfo poolCost = new PoolCostInfo("aPool");

        poolCost.setSpaceUsage(100, 20, 30, 50);


        PoolManagerPoolUpMessage poolUpMessage = new PoolManagerPoolUpMessage("aPool",
                serialId, poolMode, poolCost);

        cellMessage = new CellMessage( new CellPath("CostModule"), poolUpMessage);

        _costModule.messageArrived(cellMessage);

        recivedCost = _costModule.getPoolCostInfo("aPool");

        assertNotNull("should return non null cost on a pool with costInfo", recivedCost);


        // set pool down

        poolMode.setMode(PoolV2Mode.DISABLED);


        PoolManagerPoolUpMessage poolDownMessage = new PoolManagerPoolUpMessage("aPool",
                serialId, poolMode, poolCost);

        cellMessage = new CellMessage( new CellPath("CostModule"), poolDownMessage);

        _costModule.messageArrived(cellMessage);
        recivedCost = _costModule.getPoolCostInfo("aPool");

        assertNull("should return null cost on a DOWN pool", recivedCost);
    }


//    @Test
//    public void testPoolDiskCost() throws Exception {
//
//        long serialId = System.currentTimeMillis();
//
//        PoolV2Mode poolMode = new PoolV2Mode(PoolV2Mode.ENABLED);
//
//        PoolCostInfo poolCost = new PoolCostInfo("aPool");
//
//        /*
//         *  make it simple:
//         *      total    : 100
//         *      free     : 20
//         *      precious : 30
//         *      removable: 50
//         */
//        poolCost.setSpaceUsage(100, 20, 30, 50);
//
//
//        PoolManagerPoolUpMessage poolUpMessage = new PoolManagerPoolUpMessage("aPool",
//                serialId, poolMode, poolCost);
//
//        CellMessage cellMessage = new CellMessage( new CellPath("CostModule"), poolUpMessage);
//
//        _costModule.messageArrived(cellMessage);
//
//        PoolCostInfo recivedCost = _costModule.getPoolCostInfo("aPool");
//
//        assertNotNull("should return non null cost on a pool with costInfo", recivedCost);
//
//        PoolCostCheckable poolCostCheckable =  _costModule.getPoolCost("aPool", 18);
//        System.out.println("Pool cost: " + poolCostCheckable.getSpaceCost() );
//
//        poolCostCheckable =  _costModule.getPoolCost("aPool", 40);
//        System.out.println("Pool cost: " + poolCostCheckable.getSpaceCost() );
//
//        poolCostCheckable =  _costModule.getPoolCost("aPool", 60);
//        System.out.println("Pool cost: " + poolCostCheckable.getSpaceCost() );
//
//        poolCostCheckable =  _costModule.getPoolCost("aPool", 120);
//        System.out.println("Pool cost: " + poolCostCheckable.getSpaceCost() );
//
//    }
}
