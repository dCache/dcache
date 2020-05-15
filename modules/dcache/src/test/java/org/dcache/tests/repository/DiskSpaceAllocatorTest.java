package org.dcache.tests.repository;

import org.junit.Test;

import java.util.Random;

import diskCacheV111.util.PnfsId;

import org.dcache.pool.repository.Account;

import static org.junit.Assert.*;

public class DiskSpaceAllocatorTest {

    static final PnfsId ID = new PnfsId("000000000000000000000000000000000000");

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
            account.allocate(ID, -1);
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

        account.allocateNow(ID, allocSize);

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

        account.allocateNow(ID, allocSize);
        account.allocateNow(ID, allocSize);
        account.allocateNow(ID, allocSize);

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

        account.allocateNow(ID, allocSize);

        account.free(ID, allocSize);

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

        account.allocateNow(ID, allocSize);
        assertFalse("allocateNow should return false if there is no space available", account.allocateNow(ID, space));
    }


    @Test(timeout=60_000)
    public void testAllocateWithFree() throws Exception {

        final Random random = new Random();
        final long space = Math.abs(random.nextLong());

        final long allocSize = space / 2;

        final Account account = new Account();
        account.setTotal(space);

        account.allocateNow(ID, allocSize);

        assertEquals("used space do not match allocated space", allocSize, account.getUsed());
        assertEquals("free space do not match after alloc",space - allocSize, account.getFree());

        long delay = 200; // 0.2 seconds
        DiskSpaceAllocationTestHelper.delayedFreeEntry(account, allocSize, delay);

        // Should block until space is freed.
        account.allocate(ID, space);

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

        account.allocateNow(ID, allocSize);

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

        account.allocateNow(ID, allocSize);

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

        account.allocateNow(ID, allocSize);

        try {
            account.setTotal(newTotal);
            fail("new space can't be smaller than used space");
        }catch(IllegalArgumentException e) {
            // OK
        }

    }


    @Test(timeout=60_000)
    public void testAllocateWithSetTotalInc() throws Exception {

        final long initialTotalSize = 1000;

        final Account account = new Account();
        account.setTotal(initialTotalSize);

        final long newTotalSize = 2*initialTotalSize;
        final long delay = 100;

        // Allocate all the space
        account.allocateNow(ID, initialTotalSize);

        assertEquals( "Used size incorrect", initialTotalSize, account.getUsed());
        assertEquals( "Free size incorrect", 0, account.getFree());

        DiskSpaceAllocationTestHelper.delayedAddSpace(account, newTotalSize, delay);

        final long newAllocSize = initialTotalSize/4;
        account.allocate(ID, newAllocSize);

        final long newUsedSize = initialTotalSize + newAllocSize;

        assertEquals( "Total size incorrect", newTotalSize, account.getTotal());
        assertEquals( "Used size incorrect", newUsedSize, account.getUsed());
        assertEquals( "Free size incorrect", newTotalSize - newUsedSize, account.getFree());
    }


    @Test(expected=IllegalArgumentException.class)
    public void testFreeMoreThanReserved() throws Exception {

        final Random random = new Random();
        final long space = Math.abs(random.nextLong());
        final Account account = new Account();
        account.setTotal(space);

        account.free(ID, 4);
    }
}
