package org.dcache.services.billing.db;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.dcache.services.billing.db.data.DoorRequestData;
import org.dcache.services.billing.db.data.PnfsBaseInfo;
import org.dcache.services.billing.db.data.StorageData;
import org.dcache.services.billing.db.exceptions.BillingQueryException;
import org.dcache.services.billing.db.exceptions.BillingStorageException;

/**
 * Exercises basic put, get, remove.
 *
 * @author arossi
 */
public class BillingInfoAccessPersistenceTest extends BaseBillingInfoAccessTest {

    private long then;
    private long mod;
    private long sleep = 1000L;

    protected void setUp() throws Exception {
        super.setUp();
        mod = TimeUnit.DAYS.toMillis(365);
        long now = System.currentTimeMillis();
        then = now - mod;
    }

    public void test0PutGetDelete() throws BillingQueryException,
                    BillingStorageException {
        PnfsBaseInfo[] data = new PnfsBaseInfo[] {
                        messageGenerator.newPnfsInfo(0),
                        messageGenerator.newPnfsInfo(1),
                        messageGenerator.newPnfsInfo(2),
                        messageGenerator.newPnfsInfo(3) };

        for (PnfsBaseInfo d : data) {
            randomizeDate(d);
            getAccess().put(d);
        }

        try {
            Thread.sleep(sleep);
        } catch (InterruptedException ignored) {
        }

        for (PnfsBaseInfo d : data) {
            Collection<?> retrieved = getAccess().get(d.getClass());

            assertNotNull("class " + d.getClass(), retrieved);
            assertEquals("class " + d.getClass(), 1, retrieved.size());
        }

        for (PnfsBaseInfo d : data) {
            getAccess().remove(d.getClass());

            Collection<?> retrieved = getAccess().get(d.getClass());

            assertNotNull("class " + d.getClass(), retrieved);
            assertEquals("class " + d.getClass(), 0, retrieved.size());
        }
    }

    public void test1Select() throws BillingQueryException,
                    BillingStorageException {
        PnfsBaseInfo p1 = messageGenerator.newPnfsInfo(2);
        PnfsBaseInfo p2 = messageGenerator.newPnfsInfo(2);
        p1.setAction("store");
        p2.setAction("restore");
        getAccess().put(p1);
        getAccess().put(p2);

        try {
            Thread.sleep(sleep);
        } catch (InterruptedException ignored) {
        }

        String filter = "action == val";
        String parameters = "java.lang.String val";
        Object[] value = new Object[] { "restore" };

        Collection<?> retrieved = getAccess().get(p1.getClass(), filter,
                        parameters, value);
        assertNotNull(retrieved);
        assertEquals(1, retrieved.size());
        compare(p2, retrieved.iterator().next());

        cleanup(StorageData.class);
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
                } catch (IllegalArgumentException t) {
                    t.printStackTrace();
                    assertNull(t);
                } catch (IllegalAccessException t) {
                    t.printStackTrace();
                    assertNull(t);
                } catch (InvocationTargetException t) {
                    t.printStackTrace();
                    assertNull(t);
                }
            }
        }
    }

    private void randomizeDate(PnfsBaseInfo o) {
        long time = then + (Math.abs(r.nextLong()) % mod);
        time = (time / (1000 * 60 * 60)) * 1000 * 60 * 60;
        o.setDateStamp(new Date(time));
    }
}
