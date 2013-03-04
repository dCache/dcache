package org.dcache.webadmin.view.beans;

import org.junit.Before;
import org.junit.Test;

import org.dcache.webadmin.model.dataaccess.impl.XMLDataGathererHelper;
import org.dcache.webadmin.view.util.DiskSpaceUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class PoolSpaceBeanTest {

    private static final int INITIAL_FREE_SPACE = 50000000;
    private static final int INITIAL_PRECIOUS_SPACE = 20000000;
    private static final int INITIAL_USED_SPACE = 50000000;
    private static final int INITIAL_REMOVABLE_SPACE = 0;
    private static final int INITIAL_TOTAL_SPACE = 100000000;
    private static final float EXPECTED_PERCENTAGE_19_9 = 19.9F;
    private static final float EXPECTED_PERCENTAGE_ZERO = 0.0F;
    private static final float EXPECTED_PERCENTAGE_20 = 20.0F;
    private static final float EXPECTED_PERCENTAGE_30 = 30.0F;
    private static final float EXPECTED_PERCENTAGE_50 = 50.0F;
    private static final float EXPECTED_PERCENTAGE_100 = 100.0F;
    private PoolSpaceBean _poolBean;

    @Before
    public void setUp() {
        _poolBean = new PoolSpaceBean();
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
        assertEquals(EXPECTED_PERCENTAGE_30, _poolBean.getPercentagePinned(), 0);
        assertEquals(EXPECTED_PERCENTAGE_50, _poolBean.getPercentageFree(), 0);
        _poolBean.setTotalSpace(0);
        assertInitialValues();
        _poolBean.setPreciousSpace(19900000);
        assertInitialValues();
        _poolBean.setTotalSpace(INITIAL_TOTAL_SPACE);
        assertEquals(EXPECTED_PERCENTAGE_19_9, _poolBean.getPercentagePrecious(), 0);
        assertEquals(EXPECTED_PERCENTAGE_50, _poolBean.getPercentageFree(), 0);
    }

    private void setPoolBeanToStartingValues() {
        setToStartingValues(_poolBean);
    }

    private void setToStartingValues(PoolSpaceBean pool) {
        pool.setFreeSpace(INITIAL_FREE_SPACE);
        pool.setPreciousSpace(INITIAL_PRECIOUS_SPACE);
        pool.setUsedSpace(INITIAL_USED_SPACE);
        pool.setRemovableSpace(INITIAL_REMOVABLE_SPACE);
        pool.setTotalSpace(INITIAL_TOTAL_SPACE);
    }

    private void assertInitialValues() {
        assertEquals(EXPECTED_PERCENTAGE_ZERO, _poolBean.getPercentagePrecious(), 0);
        assertEquals(EXPECTED_PERCENTAGE_ZERO, _poolBean.getPercentagePinned(), 0);
        assertEquals(EXPECTED_PERCENTAGE_ZERO, _poolBean.getPercentageRemovable(), 0);
        assertEquals(EXPECTED_PERCENTAGE_100, _poolBean.getPercentageFree(), 0);
    }

    @Test
    public void testAddPool() {
        setPoolBeanToStartingValues();
        PoolSpaceBean otherPool = new PoolSpaceBean();
        setToStartingValues(otherPool);
        otherPool.addPoolSpace(_poolBean);
        assertEquals(DiskSpaceUnit.BYTES.convert(INITIAL_FREE_SPACE * 2,
                DiskSpaceUnit.MIBIBYTES), otherPool.getFreeSpace(), 0);
        assertEquals(DiskSpaceUnit.BYTES.convert(INITIAL_PRECIOUS_SPACE * 2,
                DiskSpaceUnit.MIBIBYTES), otherPool.getPreciousSpace(), 0);
        assertEquals(DiskSpaceUnit.BYTES.convert(INITIAL_USED_SPACE * 2,
                DiskSpaceUnit.MIBIBYTES), otherPool.getUsedSpace(), 0);
        assertEquals(DiskSpaceUnit.BYTES.convert(INITIAL_TOTAL_SPACE * 2,
                DiskSpaceUnit.MIBIBYTES), otherPool.getTotalSpace(), 0);
    }

    @Test(expected = NullPointerException.class)
    public void testCompareToWithNull() {
        _poolBean.compareTo(null);
    }

    @Test
    public void testCompareToWithEqual() {
        PoolSpaceBean otherPool = new PoolSpaceBean();
        assertEquals(_poolBean.compareTo(otherPool), 0);
    }

    @Test
    public void testCompareToWithSmaller() {
        PoolSpaceBean otherPool = new PoolSpaceBean();
        _poolBean.setName("A");
        otherPool.setName("B");
        assertEquals(_poolBean.compareTo(otherPool), -1);
    }

    @Test
    public void testCompareToWithBigger() {
        PoolSpaceBean otherPool = new PoolSpaceBean();
        _poolBean.setName("B");
        otherPool.setName("A");
        assertEquals(_poolBean.compareTo(otherPool), 1);
    }

    @Test
    public void testEqualsOnInitialState() {
        PoolSpaceBean otherPool = new PoolSpaceBean();
        assertEquals(_poolBean, otherPool);
        assertEquals(_poolBean.hashCode(), otherPool.hashCode());
    }

    @Test
    public void testEqualsWithDifferentEnable() {
        PoolSpaceBean otherPool = new PoolSpaceBean();
        otherPool.setEnabled(true);
        assertEquals(_poolBean, otherPool);
    }

    @Test
    public void testEqualsHistory() {
        PoolSpaceBean otherPool = new PoolSpaceBean();
        assertEquals(_poolBean, otherPool);
        assertEquals(_poolBean.hashCode(), otherPool.hashCode());

        _poolBean.setName(XMLDataGathererHelper.POOL1_NAME);
        assertFalse(_poolBean.equals(otherPool));

        otherPool.setName(XMLDataGathererHelper.POOL1_NAME);
        assertEquals(_poolBean, otherPool);
    }
}
