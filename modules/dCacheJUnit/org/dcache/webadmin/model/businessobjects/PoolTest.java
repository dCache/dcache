package org.dcache.webadmin.model.businessobjects;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author jan schaefer 29-10-2009
 */
public class PoolTest {

    private final String TEST_NAME = "testName";
    private Pool pool = null;

    @Before
    public void setUp() {
        pool = new Pool();
    }

    @Test
    public void testPool() {
        pool = new Pool();
        assertNotNull("failed to instantiate a pool", pool);
    }

    @Test
    public void testSetName() {
        pool.setName(TEST_NAME);
        assertEquals("setName failed", TEST_NAME, pool.getName());
    }

    @Test
    public void testAttributes() {
        boolean enabled = true;
        long totalSpaceMB = 2147483648L;
        long freeSpaceMB = 2147467538L;
        long preciousSpaceMB = 16110L;

        pool.setName(TEST_NAME);
        pool.setEnabled(enabled);
        pool.setTotalSpace(totalSpaceMB);
        pool.setFreeSpace(freeSpaceMB);
        pool.setPreciousSpace(preciousSpaceMB);

        assertEquals("set-Method failed", TEST_NAME, pool.getName());
        assertEquals("set-Method failed", enabled, pool.isEnabled());
        assertEquals("set-Method failed", totalSpaceMB, pool.getTotalSpace());
        assertEquals("set-Method failed", freeSpaceMB, pool.getFreeSpace());
        assertEquals("set-Method failed", preciousSpaceMB, pool.getPreciousSpace());
    }

    @Test
    public void testHashCode() {
        Pool otherPool = new Pool();
        assertTrue(pool.hashCode() == otherPool.hashCode());

        pool.setFreeSpace(10L);
        pool.setName(TEST_NAME);
        assertFalse(pool.hashCode() == otherPool.hashCode());

        otherPool.setName(TEST_NAME);
        assertFalse(pool.hashCode() == otherPool.hashCode());

        otherPool.setFreeSpace(10L);
        assertTrue(pool.hashCode() == otherPool.hashCode());

        otherPool.setEnabled(true);
        assertTrue(pool.hashCode() == otherPool.hashCode());
    }

    @Test
    public void testEquals() {
        Pool otherPool = new Pool();
        assertTrue(pool.equals(otherPool));

        pool.setFreeSpace(10L);
        pool.setName(TEST_NAME);
        assertFalse(pool.equals(otherPool));

        otherPool.setName(TEST_NAME);
        assertFalse(pool.equals(otherPool));

        otherPool.setFreeSpace(10L);
        assertTrue(pool.hashCode() == otherPool.hashCode());

        otherPool.setEnabled(true);
        assertTrue(pool.equals(otherPool));
    }
}
