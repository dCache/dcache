package org.dcache.tests.poolmanager;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import diskCacheV111.pools.PoolCostInfo;

import static org.junit.Assert.*;


public class PoolCostInfoTest {

    private PoolCostInfo poolCost;

    @Before
    public void setUp() {
        poolCost = new PoolCostInfo("aPool");
    }

    @Test
    public void testTotalLessThanFree() {

        try {

            poolCost.setSpaceUsage(1, 2, 0, 0);

            fail("total < free");
        }catch(IllegalArgumentException i) {
            // OK
        }
    }

    @Test
    public void testTotalLessThanPrecious() {

        try {

            poolCost.setSpaceUsage(1, 0, 2, 0);

            fail("total < precious");
        }catch(IllegalArgumentException i) {
            // OK
        }
    }

    @Test
    public void testTotalLessThanRemoveable() {

        try {

            poolCost.setSpaceUsage(1, 0, 0, 2);

            fail("total < removable");
        }catch(IllegalArgumentException i) {
            // OK
        }
    }

    @Ignore("Current implementation does not guarantee this consistency")
    @Test
    public void testTotalLessThanSum() {

        try {

            poolCost.setSpaceUsage(2, 1, 1, 1);

            fail("total < precious + removeable + free");
        }catch(IllegalArgumentException i) {
            // OK
        }
    }


}
