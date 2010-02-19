package org.dcache.webadmin.view.beans;

import org.dcache.webadmin.model.dataaccess.impl.XMLDataGathererHelper;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class PoolBeanTest {

    private static final int INITIAL_FREE_SPACE = 500;
    private static final int INITIAL_PRECIOUS_SPACE = 200;
    private static final int INITIAL_USED_SPACE = 300;
    private static final int INITIAL_TOTAL_SPACE = 1000;
    private static final float EXPECTED_PERCENTAGE_19_9 = 19.9F;
    private static final float EXPECTED_PERCENTAGE_ZERO = 0.0F;
    private static final float EXPECTED_PERCENTAGE_20 = 20.0F;
    private static final float EXPECTED_PERCENTAGE_30 = 30.0F;
    private static final float EXPECTED_PERCENTAGE_50 = 50.0F;
    private static final float EXPECTED_PERCENTAGE_100 = 100.0F;
    private PoolBean _poolBean;

    @Before
    public void setUp() {
        _poolBean = new PoolBean();
    }

    @Test
    public void testPercentageChangeFree() {
        _poolBean.setFreeSpace(INITIAL_FREE_SPACE);
        assertInitialValues();
    }

    @Test
    public void testPercentageInitial() {
        assertInitialValues();
    }

    @Test
    public void testPercentageChangePrecious() {
        _poolBean.setPreciousSpace(INITIAL_PRECIOUS_SPACE);
        assertInitialValues();
    }

    @Test
    public void testPercentageChangeFreeThenPrecious() {
        _poolBean.setFreeSpace(INITIAL_FREE_SPACE);
        _poolBean.setPreciousSpace(INITIAL_PRECIOUS_SPACE);
        assertInitialValues();
    }

    @Test
    public void testPercentageChangePreciousThenFree() {
        _poolBean.setPreciousSpace(INITIAL_PRECIOUS_SPACE);
        _poolBean.setFreeSpace(INITIAL_FREE_SPACE);
        assertInitialValues();
    }

    @Test
    public void testPercentageChangeUsed() {
        _poolBean.setUsedSpace(INITIAL_USED_SPACE);
        assertInitialValues();
    }

    @Test
    public void testPercentageResetTotalToZero() {
        setPoolBeanToStartingValues();
        _poolBean.setTotalSpace(0);
        assertInitialValues();
    }

    @Test
    public void testPercentageCalculationHistory() {
        setPoolBeanToStartingValues();
        assertEquals(EXPECTED_PERCENTAGE_20, _poolBean.getPercentagePrecious(), 0);
        assertEquals(EXPECTED_PERCENTAGE_30, _poolBean.getPercentageUsed(), 0);
        assertEquals(EXPECTED_PERCENTAGE_50, _poolBean.getPercentageFree(), 0);
        _poolBean.setTotalSpace(0);
        assertInitialValues();
        _poolBean.setPreciousSpace(199);
        assertInitialValues();
        _poolBean.setTotalSpace(INITIAL_TOTAL_SPACE);
        assertEquals(EXPECTED_PERCENTAGE_19_9, _poolBean.getPercentagePrecious(), 0);
        assertEquals(EXPECTED_PERCENTAGE_30, _poolBean.getPercentageUsed(), 0);
        assertEquals(EXPECTED_PERCENTAGE_50, _poolBean.getPercentageFree(), 0);
    }

    private void setPoolBeanToStartingValues() {
        _poolBean.setFreeSpace(INITIAL_FREE_SPACE);
        _poolBean.setPreciousSpace(INITIAL_PRECIOUS_SPACE);
        _poolBean.setUsedSpace(INITIAL_USED_SPACE);
        _poolBean.setTotalSpace(INITIAL_TOTAL_SPACE);
    }

    private void assertInitialValues() {
        assertEquals(EXPECTED_PERCENTAGE_ZERO, _poolBean.getPercentagePrecious(), 0);
        assertEquals(EXPECTED_PERCENTAGE_ZERO, _poolBean.getPercentageUsed(), 0);
        assertEquals(EXPECTED_PERCENTAGE_100, _poolBean.getPercentageFree(), 0);
    }

    @Test(expected = java.lang.NullPointerException.class)
    public void testCompareToWithNull() {
        _poolBean.compareTo(null);
    }

    @Test(expected = java.lang.ClassCastException.class)
    public void testCompareToWithWrongType() {
        Boolean otherPool = new Boolean(true);
        _poolBean.compareTo(otherPool);
    }

    @Test
    public void testCompareToWithEqual() {
        PoolBean otherPool = new PoolBean();
        assertEquals(_poolBean.compareTo(otherPool), 0);
    }

    @Test
    public void testCompareToWithSmaller() {
        PoolBean otherPool = new PoolBean();
        _poolBean.setName("A");
        otherPool.setName("B");
        assertEquals(_poolBean.compareTo(otherPool), -1);
    }

    @Test
    public void testCompareToWithBigger() {
        PoolBean otherPool = new PoolBean();
        _poolBean.setName("B");
        otherPool.setName("A");
        assertEquals(_poolBean.compareTo(otherPool), 1);
    }

    @Test
    public void testEqualsOnInitialState() {
        PoolBean otherPool = new PoolBean();
        assertEquals(_poolBean, otherPool);
        assertEquals(_poolBean.hashCode(), otherPool.hashCode());
    }

    @Test
    public void testNotEqualsByFree() {
        PoolBean otherPool = new PoolBean();
        _poolBean.setFreeSpace(10L);
        assertFalse(_poolBean.equals(otherPool));
    }

    @Test
    public void testEqualsWithDifferentEnable() {
        PoolBean otherPool = new PoolBean();
        otherPool.setEnabled(true);
        assertEquals(_poolBean, otherPool);
    }

    @Test
    public void testEqualsHistory() {
        PoolBean otherPool = new PoolBean();
        assertEquals(_poolBean, otherPool);
        assertEquals(_poolBean.hashCode(), otherPool.hashCode());

        _poolBean.setFreeSpace(10L);
        _poolBean.setName(XMLDataGathererHelper.POOL1_NAME);
        assertFalse(_poolBean.equals(otherPool));

        otherPool.setName(XMLDataGathererHelper.POOL1_NAME);
        assertFalse(_poolBean.equals(otherPool));

        otherPool.setFreeSpace(10L);
        assertEquals(_poolBean, otherPool);
    }
}
