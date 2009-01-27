package org.dcache.tests.repository;

import static org.junit.Assert.*;

import java.util.MissingResourceException;
import java.util.Random;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.dcache.pool.repository.Account;
import org.junit.BeforeClass;
import org.junit.Test;

public class DiskSpaceAllocatorTest {


    @BeforeClass
    public static void setUp() {

      //  Logger.getLogger("logger.dev.org.dcache.poolspacemonitor").setLevel(Level.DEBUG);

    }

    @Test
    public void testNegativeTotalSpace() {

        try {
            final Account account = new Account();
            account.setTotal(-1);
            fail("IllegalArgumentException should be thrown in case of totalSpace < 0");
        }catch (final IllegalArgumentException e) {
            // OK
        }

    }


    @Test
    public void testTotalSpace() {

        final Random random = new Random();
        final long space = Math.abs(random.nextLong());

        final Account account = new Account();
        account.setTotal(space);
        assertEquals("set/getTotalSpace() miss match", space, account.getTotal());

    }

    @Test
    public void testFreeSpaceNoAllocations() {

        final Random random = new Random();
        final long space = Math.abs(random.nextLong());

        final Account account = new Account();
        account.setTotal(space);

        assertEquals("getFreeSpace() do not match getTotalSpace() without allocations", space, account.getFree());

    }


    @Test
    public void testUsedSpaceNoAllocations() {

        final Random random = new Random();
        final long space = Math.abs(random.nextLong());

        final Account account = new Account();
        account.setTotal(space);

        assertEquals("Non zero used space without allocations", 0, account.getUsed());

    }

    @Test
    public void testBadSize() throws Exception {

        final Account account = new Account();
        account.setTotal(2);

        try {
            account.allocate(-1);
            fail("IllegalArgumentException should be thrown in case of size < 0");
        }catch(final IllegalArgumentException iae) {
            //OK
        }

    }


    @Test
    public void testAllocateNonBlockOK() throws Exception {

        final Random random = new Random();
        final long space = Math.abs(random.nextLong());

        final Account account = new Account();
        account.setTotal(space);

        final long allocSize = space / 2;

        account.allocateNow(allocSize);

        assertEquals("used space do not match allocated space", allocSize, account.getUsed());
        assertEquals("free space do not match after alloc", space - allocSize, account.getFree());

    }


    @Test
    public void testAllocateMultipleNonBlockOK() throws Exception {

        final Random random = new Random();
        final long space = Math.abs(random.nextLong());

        final long allocSize = space / 4;

        final Account account = new Account();
        account.setTotal(space);

        account.allocateNow(allocSize);
        account.allocateNow(allocSize);
        account.allocateNow(allocSize);

        assertEquals("used space do not match allocated space", 3*allocSize, account.getUsed());
        assertEquals("free space do not match after alloc", space - 3*allocSize, account.getFree());

    }

    @Test
    public void testFree() throws Exception {

        final Random random = new Random();
        final long space = Math.abs(random.nextLong());

        final Account account = new Account();
        account.setTotal(space);

        final long allocSize = space / 2;

        account.allocateNow(allocSize);

        account.free(allocSize);

        assertEquals("used space do not match after free", 0, account.getUsed());
        assertEquals("free space do not match after free", space, account.getFree());

    }

    @Test
    public void testAllocateMoreThanFreeNonBlock() throws Exception {

        final Random random = new Random();
        final long space = Math.abs(random.nextLong());

        final Account account = new Account();
        account.setTotal(space);

        final long allocSize = space / 2;

        account.allocateNow(allocSize);
        assertFalse("allocateNow should return false if there is no space available", account.allocateNow(space));
    }

    @Test
    public void testAllocateWithFree() throws Exception {

        final Random random = new Random();
        final long space = Math.abs(random.nextLong());

        final long allocSize = space / 2;

        final Account account = new Account();
        account.setTotal(space);

        account.allocateNow(allocSize);

        final long timeout = 2000; // 2 seconds
        DiskSpaceAllocationTestHelper.freeEntry(account, allocSize, timeout);
        account.allocate(space);

        assertEquals("used space do not match allocated space", space, account.getUsed());
        assertEquals("free space do not match after alloc", 0, account.getFree());

    }

    @Test
    public void testSetTotalInc() throws Exception {

        final Random random = new Random();
        final long space = Math.abs(random.nextLong());
        final long newTotal = space + 20;

        final Account account = new Account();
        account.setTotal(space);

        final long allocSize = space / 4;

        account.allocateNow(allocSize);

        account.setTotal(newTotal);

        assertEquals("total space do not reflect to new value", newTotal, account.getTotal());
        assertEquals("used space should not change after total space increase", allocSize, account.getUsed());
        assertEquals("free space do not reflect new total space",newTotal  - allocSize, account.getFree());


    }

    @Test
    public void testSetTotalDec() throws Exception {

        final Random random = new Random();
        final long space = Math.abs(random.nextLong());
        final long newTotal = space / 2;

        final Account account = new Account();
        account.setTotal(space);

        final long allocSize = space / 4;

        account.allocateNow(allocSize);

        account.setTotal(newTotal);

        assertEquals("total space do not reflect to new value", newTotal, account.getTotal());
        assertEquals("used space should not change after total space increase", allocSize, account.getUsed());
        assertEquals("free space do not reflect new total space",newTotal  - allocSize, account.getFree());

    }

    @Test
    public void testSetTotalDecMissing() throws Exception {

        final Random random = new Random();
        final long space = Math.abs(random.nextLong());
        final long newTotal = space / 4;

        final Account account = new Account();
        account.setTotal(space);

        final long allocSize = space / 2;

        account.allocateNow(allocSize);

        try {
            account.setTotal(newTotal);
            fail("new space can't be smaller than used space");
        }catch(IllegalArgumentException e) {
            // OK
        }

    }


    @Test
    public void testAllocateWithSetTotalInc() throws Exception {

        final Random random = new Random();
        /*
         * while we use later on space+space we have to guarantee that
         * it still a positive number
         */
        final long space = Math.abs(random.nextLong())/2;

        final Account account = new Account();
        account.setTotal(space);

        final long allocSize = space;
        final long newTotal = (space + allocSize);

        account.allocateNow(allocSize);

        DiskSpaceAllocationTestHelper.addSpace(account, newTotal, 2000);
        final long newAllocSize = space/4;
        account.allocate(newAllocSize);
    }


    @Test(expected=IllegalArgumentException.class)
    public void testFreeMoreThanReserved() throws Exception {

        final Random random = new Random();
        final long space = Math.abs(random.nextLong());
        final Account account = new Account();
        account.setTotal(space);

        account.free(4);
    }
}
