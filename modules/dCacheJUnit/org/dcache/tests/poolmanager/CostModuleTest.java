package org.dcache.tests.poolmanager;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import org.dcache.tests.cells.CellAdapterHelper;

import diskCacheV111.poolManager.CostModuleV1;

public class CostModuleTest {

    private final static CellAdapterHelper _cell = new CellAdapterHelper( "CostModuleTest", "");
    private CostModuleV1 _costModule;

    @Before
    public void setUp() throws Exception {
        _costModule = new CostModuleV1(_cell);
    }

    @Test
    public void testPoolNotExist() throws Exception {

        assertNull("should return null on non existing pool", _costModule.getPoolCostInfo("aPool"));

    }
}
