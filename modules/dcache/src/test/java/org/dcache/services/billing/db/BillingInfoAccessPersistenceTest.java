package org.dcache.services.billing.db;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.dcache.services.billing.db.data.DoorRequestData;
import org.dcache.services.billing.db.data.PnfsBaseInfo;
import org.dcache.services.billing.db.exceptions.BillingQueryException;

/**
 * Exercises basic put, get, remove.
 *
 * @author arossi
 */
public class BillingInfoAccessPersistenceTest extends BaseBillingInfoAccessTest {

    private long then;
    private long mod;

    @Override
    protected void setUp() throws Exception {
        if (getName().equals("testPutGetDelete")) {
            timeout = 1;
            maxBefore = 1;
        } else if (getName().equals("testDelayedCommit")) {
            timeout = Integer.MAX_VALUE;
            maxBefore = 1000;
        } else if (getName().equals("testSelect")) {
            timeout = 1;
            maxBefore = 1;
        }
        super.setUp();
        mod = TimeUnit.DAYS.toMillis(365);
        long now = System.currentTimeMillis();
        then = now - mod;
    }


    /**
     * Test simple inserts and retrievals on the 5 basic info objects. The
     * CommitTimer is set to 0.
     * @throws BillingQueryException
     */
    public void testPutGetDelete() throws BillingQueryException {
        long sleep = 1500L * timeout;
        for (int i = 0; i < 4; i++) {
            PnfsBaseInfo original = messageGenerator.newPnfsInfo(i);
            randomizeDate(original);
            getAccess().put(original);
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException ignored) {
            }
            Collection<?> retrieved = null;
            try {
                retrieved = getAccess().get(original.getClass());
            } catch (BillingQueryException t) {
                t.printStackTrace();
                assertNull(t);
            }
            assertNotNull(retrieved);
            assertEquals(1, retrieved.size());
            compare(original, retrieved.iterator().next());
            try {
                getAccess().remove(original.getClass());
                retrieved = getAccess().get(original.getClass());
            } catch (BillingQueryException t) {
                t.printStackTrace();
                assertNull(t);
            }
            assertNotNull(retrieved);
            assertEquals(0, retrieved.size());
        }
    }

    /**
     * Test the delayed commit mechanism. Timeout is infinite, so commit will
     * only take place when the insert margin has been reached;
     * @throws BillingQueryException
     */
    public void testDelayedCommit() throws BillingQueryException {
        int k = maxBefore / 2;
        for (int i = 0; i < k; i++) {
            getAccess().put(messageGenerator.newPnfsInfo(1));
        }
        Collection<?> retrieved = null;
        try {
            retrieved = getAccess().get(DoorRequestData.class);
        } catch (BillingQueryException t) {
            t.printStackTrace();
            assertNull(t);
        }
        assertNotNull(retrieved);
        assertEquals(0, retrieved.size());
        for (int i = 0; i < k; i++) {
            getAccess().put(messageGenerator.newPnfsInfo(1));
        }
        try {
            retrieved = getAccess().get(DoorRequestData.class);
        } catch (BillingQueryException t) {
            t.printStackTrace();
            assertNull(t);
        }
        assertNotNull(retrieved);
        assertEquals(maxBefore, retrieved.size());

        cleanup(DoorRequestData.class);
    }

    /**
     * Check that filter works.
     * @throws BillingQueryException
     */
    public void testSelect() throws BillingQueryException {
        PnfsBaseInfo p1 = messageGenerator.newPnfsInfo(1);
        PnfsBaseInfo p2 = messageGenerator.newPnfsInfo(1);
        p1.setAction("store");
        p2.setAction("restore");
        getAccess().put(p1);
        getAccess().put(p2);

        String filter = "action == val";
        String parameters = "java.lang.String val";
        Object[] value = new Object[] { "restore" };

        Collection<?> retrieved = null;
        try {
            retrieved = getAccess().get(p1.getClass(), filter, parameters,
                            value);
        } catch (BillingQueryException t) {
            t.printStackTrace();
            assertNull(t);
        }
        assertNotNull(retrieved);
        assertEquals(1, retrieved.size());
        compare(p2, retrieved.iterator().next());

        cleanup(DoorRequestData.class);
    }

    /**
     * @param original
     * @param next
     */
    private <T> void compare(T original, T next) {
        assertEquals(original.getClass(), next.getClass());
        Method[] methods = original.getClass().getMethods();
        for (Method m : methods) {
            if (m.getName().startsWith("get")) {
                try {
                    Object o1 = m.invoke(original, (Object[]) null);
                    Object o2 = m.invoke(next, (Object[]) null);
                    assertEquals(o1, o2);
                } catch (IllegalArgumentException | InvocationTargetException | IllegalAccessException t) {
                    t.printStackTrace();
                    assertNull(t);
                }
            }
        }
    }

    /**
     * @param o
     */
    private void randomizeDate(PnfsBaseInfo o) {
        long time = then + (Math.abs(r.nextLong()) % mod);
        time = (time / (1000 * 60 * 60)) * 1000 * 60 * 60;
        o.setDateStamp(new Date(time));
    }
}
