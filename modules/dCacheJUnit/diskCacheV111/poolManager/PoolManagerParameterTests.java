package diskCacheV111.poolManager;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class PoolManagerParameterTests {

    public static final String COSTCUT_ABSOLUTE_VALUE_STRING = "3.15";
    public static final double COSTCUT_ABSOLUTE_VALUE_NUMBER = Double.parseDouble( COSTCUT_ABSOLUTE_VALUE_STRING);

    public static final double COSTCUT_PERCENTILE_VALUE_NUMBER = 0.95;
    public static final String COSTCUT_PERCENTILE_VALUE_STRING = Double.toString(100 * COSTCUT_PERCENTILE_VALUE_NUMBER) + "%";

    public static final String COSTCUT_PERCENTILE_VALUE_TOO_LARGE = "100%";
    public static final String COSTCUT_PERCENTILE_VALUE_TOO_SMALL = "0%";

    PoolManagerParameter _param;

    @Before
    public void setUp() throws Exception {
        _param = new PoolManagerParameter();
    }

    /*
     *   TESTS FOR costCut RELATED METHODS
     */
    @Test
    public void testInitiallyNotValid() {
        assertFalse( "Initial costCutSet value", _param._costCutSet);
    }

    @Test
    public void testCostCutAbsoluteSetThenGet() {
        _param.setCostCut( COSTCUT_ABSOLUTE_VALUE_STRING);
        assertEquals( "set/get mismatch", COSTCUT_ABSOLUTE_VALUE_NUMBER, _param.getCostCut(), 0.0);
        assertFalse( "check isCostCutPercentile", _param.isCostCutPercentile());
        assertTrue( "costCutSet value", _param._costCutSet);
    }

    @Test
    public void testCostCutPercentileSetThenGet() {
        _param.setCostCut( COSTCUT_PERCENTILE_VALUE_STRING);
        assertEquals( "set/get mismatch", COSTCUT_PERCENTILE_VALUE_NUMBER, _param.getCostCut(), 0.0);
        assertTrue( "check isCostCutPercentile", _param.isCostCutPercentile());
        assertTrue( "costCutSet value", _param._costCutSet);
    }

    @Test( expected=IllegalArgumentException.class)
    public void testCostCutPercentileValueTooLarge() {
        _param.setCostCut( COSTCUT_PERCENTILE_VALUE_TOO_LARGE);
    }

    @Test( expected=IllegalArgumentException.class)
    public void testCostCutPercentileValueTooSmall() {
        _param.setCostCut( COSTCUT_PERCENTILE_VALUE_TOO_LARGE);
    }

    @Test
    public void testAbsoluateCostCutAfterCopy() {
        _param.setCostCut(  COSTCUT_ABSOLUTE_VALUE_STRING);
        PoolManagerParameter newParam  = new PoolManagerParameter( _param);
        assertEquals( "set/get mismatch after copy", COSTCUT_ABSOLUTE_VALUE_NUMBER, newParam.getCostCut(), 0.0);
        assertFalse( "check isCostCutPercentile after copy", newParam.isCostCutPercentile());
        assertTrue( "costCutSet value", newParam._costCutSet);
    }

    @Test
    public void testPercentileCostCutAfterCopy() {
        _param.setCostCut(  COSTCUT_PERCENTILE_VALUE_STRING);
        PoolManagerParameter newParam  = new PoolManagerParameter( _param);
        assertEquals( "set/get mismatch after copy", COSTCUT_PERCENTILE_VALUE_NUMBER, newParam.getCostCut(), 0.0);
        assertTrue( "check isCostCutPercentile after copy", newParam.isCostCutPercentile());
        assertTrue( "costCutSet value", newParam._costCutSet);
    }

    @Test
    public void testAbsoluteCostCutAfterMerge() {
        PoolManagerParameter newParam  = new PoolManagerParameter();
        newParam.setCostCut(  COSTCUT_ABSOLUTE_VALUE_STRING);

        _param.merge( newParam);

        assertEquals( "set/get mismatch after copy", COSTCUT_ABSOLUTE_VALUE_NUMBER, newParam.getCostCut(), 0.0);
        assertFalse( "check isCostCutPercentile after copy", newParam.isCostCutPercentile());
        assertTrue( "costCutSet value", newParam._costCutSet);
    }

    @Test
    public void testPercentileCostCutAfterMerge() {
        PoolManagerParameter newParam  = new PoolManagerParameter();
        newParam.setCostCut(  COSTCUT_PERCENTILE_VALUE_STRING);

        _param.merge( newParam);

        assertEquals( "set/get mismatch after copy", COSTCUT_PERCENTILE_VALUE_NUMBER, newParam.getCostCut(), 0.0);
        assertTrue( "check isCostCutPercentile after copy", newParam.isCostCutPercentile());
        assertTrue( "costCutSet value", newParam._costCutSet);
    }

    @Test
    public void testAbsoluteCostCutToString() {
        _param.setCostCut( COSTCUT_ABSOLUTE_VALUE_STRING);

        PoolManagerParameter newParam  = new PoolManagerParameter();
        newParam.setCostCut( _param.getCostCutString());

        assertEquals( "set/get mismatch after copy", COSTCUT_ABSOLUTE_VALUE_NUMBER, newParam.getCostCut(), 0.0);
        assertFalse( "check isCostCutPercentile after copy", newParam.isCostCutPercentile());
        assertTrue( "costCutSet value", newParam._costCutSet);
    }

    @Test
    public void testPercentileCostCutToString() {
        _param.setCostCut( COSTCUT_PERCENTILE_VALUE_STRING);

        PoolManagerParameter newParam  = new PoolManagerParameter();
        newParam.setCostCut( _param.getCostCutString());

        assertEquals( "set/get mismatch after copy", COSTCUT_PERCENTILE_VALUE_NUMBER, newParam.getCostCut(), 0.0);
        assertTrue( "check isCostCutPercentile after copy", newParam.isCostCutPercentile());
        assertTrue( "costCutSet value", newParam._costCutSet);
    }
}
