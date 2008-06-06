package org.dcache.tests.repository;

import static org.junit.Assert.*;

import java.util.MissingResourceException;
import java.util.Random;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.dcache.pool.repository.FairDiskSpaceAllocator;
import org.dcache.pool.repository.PoolSpaceAllocatable;
import org.junit.BeforeClass;
import org.junit.Test;

import diskCacheV111.repository.SpaceRequestable;


public class DiskSpaceAllocatorTest {


    @BeforeClass
    public static void setUp() {

      //  Logger.getLogger("logger.dev.org.dcache.poolspacemonitor").setLevel(Level.DEBUG);

    }

    @Test
    public void testNegativeTotalSpace() {

        try {
            final PoolSpaceAllocatable<Long> spaceAllocator = new FairDiskSpaceAllocator<Long>(-1);
            fail("IllegalArgumentException should be thrown in case of totalSpace < 0");
        }catch (final IllegalArgumentException e) {
            // OK
        }

    }


    @Test
    public void testTotalSpace() {

        final Random random = new Random();
        final long space = Math.abs(random.nextLong());

        final PoolSpaceAllocatable<Long> spaceAllocator = new FairDiskSpaceAllocator<Long>(space);

        assertEquals("set/getTotalSpace() miss match", space, spaceAllocator.getTotalSpace());

    }

    @Test
    public void testFreeSpaceNoAllocations() {

        final Random random = new Random();
        final long space = Math.abs(random.nextLong());

        final PoolSpaceAllocatable<Long> spaceAllocator = new FairDiskSpaceAllocator<Long>(space);

        assertEquals("getFreeSpace() do not match getTotalSpace() without allocations", space, spaceAllocator.getFreeSpace());

    }


    @Test
    public void testUsedSpaceNoAllocations() {

        final Random random = new Random();
        final long space = Math.abs(random.nextLong());

        final PoolSpaceAllocatable<Long> spaceAllocator = new FairDiskSpaceAllocator<Long>(space);

        assertEquals("Non zero used space without allocations", 0, spaceAllocator.getUsedSpace());

    }


    @Test
    public void testNullEntry() throws Exception {

        final Random random = new Random();
        final long space = Math.abs(random.nextLong());

        final PoolSpaceAllocatable<Long> spaceAllocator = new FairDiskSpaceAllocator<Long>(space);

        try {
            spaceAllocator.allocate(null, 1, 0);
            fail("NullPointerException should be thrown in case of entry is null");
        }catch(final NullPointerException npe) {
            //OK
        }

    }

    @Test
    public void testBadSize() throws Exception {

        final PoolSpaceAllocatable<Long> spaceAllocator = new FairDiskSpaceAllocator<Long>(2);

        try {
            final Long entry = Long.valueOf(1);
            spaceAllocator.allocate(entry, -1, 0);
            fail("IllegalArgumentException should be thrown in case of size < 0");
        }catch(final IllegalArgumentException iae) {
            //OK
        }

    }


    @Test
    public void testAllocateNonBlockOK() throws Exception {

        final Random random = new Random();
        final long space = Math.abs(random.nextLong());

        final PoolSpaceAllocatable<Long> spaceAllocator = new FairDiskSpaceAllocator<Long>(space);


        final long allocSize = space / 2;
        final Long entry = Long.valueOf(1);

        spaceAllocator.allocate(entry, allocSize, 0);

        assertEquals("used space do not match allocated space", allocSize, spaceAllocator.getUsedSpace());
        assertEquals("free space do not match after alloc", space - allocSize, spaceAllocator.getFreeSpace());

    }


    @Test
    public void testAllocateMultipleNonBlockOK() throws Exception {

        final Random random = new Random();
        final long space = Math.abs(random.nextLong());

        final PoolSpaceAllocatable<Long> spaceAllocator = new FairDiskSpaceAllocator<Long>(space);

        final long allocSize = space / 4;
        final Long entry = Long.valueOf(1);

        spaceAllocator.allocate(entry, allocSize, 0);
        spaceAllocator.allocate(entry, allocSize, 0);
        spaceAllocator.allocate(entry, allocSize, 0);

        assertEquals("used space do not match allocated space", 3*allocSize, spaceAllocator.getUsedSpace());
        assertEquals("free space do not match after alloc", space - 3*allocSize, spaceAllocator.getFreeSpace());

    }

    @Test
    public void testFreeNull() {

        final PoolSpaceAllocatable<Long> spaceAllocator = new FairDiskSpaceAllocator<Long>(1);

        try {
            spaceAllocator.free(null);
            fail("NullPointerException should be thrown in case of entry is null");
        }catch(final NullPointerException npe) {
            // OK
        }

    }


    @Test
    public void testFree() throws Exception {

        final Random random = new Random();
        final long space = Math.abs(random.nextLong());

        final PoolSpaceAllocatable<Long> spaceAllocator = new FairDiskSpaceAllocator<Long>(space);


        final long allocSize = space / 2;
        final Long entry = Long.valueOf(1);

        spaceAllocator.allocate(entry, allocSize, 0);

        spaceAllocator.free(entry);

        assertEquals("used space do not match after free", 0, spaceAllocator.getUsedSpace());
        assertEquals("free space do not match after free", space, spaceAllocator.getFreeSpace());

    }

    @Test
    public void testFreeMultiple() throws Exception {

        final Random random = new Random();
        final long space = Math.abs(random.nextLong());

        final PoolSpaceAllocatable<Long> spaceAllocator = new FairDiskSpaceAllocator<Long>(space);


        final long allocSize = space / 2;
        final Long entry = Long.valueOf(1);

        spaceAllocator.allocate(entry, allocSize, 0);

        spaceAllocator.free(entry);
        try {
            spaceAllocator.free(entry);
            fail("double free not allowed");
        }catch(final IllegalArgumentException iae) {
            // OK
        }

    }


    @Test
    public void testAllocateMoreThanFreeNonBlock() throws Exception {


        final Random random = new Random();
        final long space = Math.abs(random.nextLong());

        final PoolSpaceAllocatable<Long> spaceAllocator = new FairDiskSpaceAllocator<Long>(space);
        final long allocSize = space / 2;
        final Long entry = Long.valueOf(1);

        spaceAllocator.allocate(entry, allocSize, 0);
        try {
            spaceAllocator.allocate(entry, space, 0);
            fail("MissingResourceException should be thrown if there is no space available in specified time interval");
        }catch(final MissingResourceException mre) {
            // OK
        }

    }

    @Test
    public void testAllocateMoreThanFreeTimeout() throws Exception {


        final Random random = new Random();
        final long space = Math.abs(random.nextLong());

        final PoolSpaceAllocatable<Long> spaceAllocator = new FairDiskSpaceAllocator<Long>(space);
        final long allocSize = space / 2;
        final Long entry = Long.valueOf(1);

        spaceAllocator.allocate(entry, allocSize, 0);
        final long now = System.currentTimeMillis();
        final long timeout = 2000; // 2 seconds
        try {
            spaceAllocator.allocate(entry, space, timeout);
            fail("MissingResourceException should be thrown if there is no space available in specified time interval");
        }catch(final MissingResourceException mre) {
            // OK
            assertTrue("MissingResourceException thrown earlier that expected", System.currentTimeMillis() > now + timeout);
        }
    }

    @Test
    public void testAllocateWithFree() throws Exception {

        final Random random = new Random();
        final long space = Math.abs(random.nextLong());

        final PoolSpaceAllocatable<Long> spaceAllocator = new FairDiskSpaceAllocator<Long>(space);
        final long allocSize = space / 2;
        final Long entry1 = Long.valueOf(1);
        final Long entry2 = Long.valueOf(2);

        spaceAllocator.allocate(entry1, allocSize, 0);

        final long timeout = 2000; // 2 seconds
        try {
            DiskSpaceAllocationTestHelper.freeEntry(spaceAllocator, entry1, timeout);
            spaceAllocator.allocate(entry2, space, timeout*5);
        }catch(final MissingResourceException mre) {
            // OK
            fail("No space available after freeing");
        }

        assertEquals("used space do not match allocated space", space, spaceAllocator.getUsedSpace());
        assertEquals("free space do not match after alloc", 0, spaceAllocator.getFreeSpace());

    }


    @Test
    public void testReallocateInc() throws Exception {


        final Random random = new Random();
        final long space = Math.abs(random.nextLong());

        final PoolSpaceAllocatable<Long> spaceAllocator = new FairDiskSpaceAllocator<Long>(space);


        final long allocSize = space / 4;
        final Long entry = Long.valueOf(1);

        spaceAllocator.allocate(entry, allocSize, 0);

        spaceAllocator.reallocate(entry, allocSize*2);

        assertEquals("used space do not match allocated space", 2*allocSize, spaceAllocator.getUsedSpace());
        assertEquals("free space do not match after alloc", space - 2*allocSize, spaceAllocator.getFreeSpace());

    }


    @Test
    public void testReallocateDec() throws Exception {


        final Random random = new Random();
        final long space = Math.abs(random.nextLong());

        final PoolSpaceAllocatable<Long> spaceAllocator = new FairDiskSpaceAllocator<Long>(space);


        final long allocSize = space / 4;
        final Long entry = Long.valueOf(1);

        spaceAllocator.allocate(entry, 2*allocSize, 0);

        spaceAllocator.reallocate(entry, allocSize);

        assertEquals("used space do not match allocated space", allocSize, spaceAllocator.getUsedSpace());
        assertEquals("free space do not match after alloc", space - allocSize, spaceAllocator.getFreeSpace());

    }

    @Test
    public void testSetTotalInc() throws Exception {

        final Random random = new Random();
        final long space = Math.abs(random.nextLong());
        final long newTotal = space + 20;

        final PoolSpaceAllocatable<Long> spaceAllocator = new FairDiskSpaceAllocator<Long>(space);

        final long allocSize = space / 4;
        final Long entry = Long.valueOf(1);

        spaceAllocator.allocate(entry, allocSize, 0);

        spaceAllocator.setTotalSpace(newTotal);

        assertEquals("total space do not reflect to new value", newTotal, spaceAllocator.getTotalSpace());
        assertEquals("used space should not change after total space increase", allocSize, spaceAllocator.getUsedSpace());
        assertEquals("free space do not reflect new total space",newTotal  - allocSize, spaceAllocator.getFreeSpace());


    }

    @Test
    public void testSetTotalDec() throws Exception {

        final Random random = new Random();
        final long space = Math.abs(random.nextLong());
        final long newTotal = space / 2;

        final PoolSpaceAllocatable<Long> spaceAllocator = new FairDiskSpaceAllocator<Long>(space);

        final long allocSize = space / 4;
        final Long entry = Long.valueOf(1);

        spaceAllocator.allocate(entry, allocSize, 0);

        spaceAllocator.setTotalSpace(newTotal);

        assertEquals("total space do not reflect to new value", newTotal, spaceAllocator.getTotalSpace());
        assertEquals("used space should not change after total space increase", allocSize, spaceAllocator.getUsedSpace());
        assertEquals("free space do not reflect new total space",newTotal  - allocSize, spaceAllocator.getFreeSpace());

    }

    @Test
    public void testSetTotalDecMissing() throws Exception {

        final Random random = new Random();
        final long space = Math.abs(random.nextLong());
        final long newTotal = space / 4;

        final PoolSpaceAllocatable<Long> spaceAllocator = new FairDiskSpaceAllocator<Long>(space);

        final long allocSize = space / 2;
        final Long entry = Long.valueOf(1);

        spaceAllocator.allocate(entry, allocSize, 0);

        try {
            spaceAllocator.setTotalSpace(newTotal);
            fail("new space can't be smaller than used space");
        }catch(MissingResourceException mre) {
            // OK
        }

    }


    @Test
    public void testAllocateWithSetTotalInc() throws Exception {

        final Random random = new Random();
        /*
         * while we use later on space+space we hate to guarantee that it still positive number
         */
        final long space = Math.abs(random.nextLong())/2;

        final PoolSpaceAllocatable<Long> spaceAllocator = new FairDiskSpaceAllocator<Long>(space);

        final long allocSize = space;
        final long newTotal = (space + allocSize);
        final Long entry = Long.valueOf(1);

        spaceAllocator.allocate(entry, allocSize, 0);

        try {
            DiskSpaceAllocationTestHelper.addSpace(spaceAllocator, newTotal, 2000);
            final long newAllocSize = space/4;
            spaceAllocator.allocate(entry, newAllocSize, 10000);
        }catch(MissingResourceException mre) {
            fail("No space available after increasing");
        }

    }


    @Test
    public void testCallback() throws Exception {

        final Random random = new Random();
        final long space = Math.abs(random.nextLong());

        final PoolSpaceAllocatable<Long> spaceAllocator = new FairDiskSpaceAllocator<Long>(space);

        final long allocSize = space/4;

        final Long entry1 = Long.valueOf(1);
        final Long entry2 = Long.valueOf(2);


        spaceAllocator.allocate(entry1, allocSize, 0);
        SpaceRequestable sweeper = DiskSpaceAllocationTestHelper.cleanOnDemand(spaceAllocator, entry1);
        spaceAllocator.addSpaceRequestListener( sweeper );

        spaceAllocator.allocate(entry2, space, 1000);

        /*
         * on success, entry1 should  be removed.
         * double free will indicate it
         */

        try {
            spaceAllocator.free(entry1);
            fail("sweeper was not called");
        }catch(IllegalArgumentException iae) {
            // OK
        }

    }

    @Test
    public void testAllocationOrder() throws Exception {

        final Random random = new Random();
        final long allocSize = Math.abs(random.nextLong()) % (1L << 60) + 1;

        final PoolSpaceAllocatable<Long> spaceAllocator = new FairDiskSpaceAllocator<Long>(4 * allocSize);

        final Long entry = Long.valueOf(0);
        final Long entry1 = Long.valueOf(1);
        final Long entry2 = Long.valueOf(2);

        spaceAllocator.allocate(entry, allocSize *4 , 0);

        /*
         * too guarantee allocation order second allocation called only when first thread is running
         */
        Thread t = DiskSpaceAllocationTestHelper.allocateInThread(spaceAllocator, entry1, 10000, allocSize);
        while( !t.isAlive() ) {
            Thread.sleep(100);
        }
        DiskSpaceAllocationTestHelper.allocateInThread(spaceAllocator, entry2, 10000, allocSize);


        spaceAllocator.reallocate(entry, allocSize*3);


        try {
            /*
             * we will get IllegalArgumentException as long
             * as there is no space available for entry2.
             */
            spaceAllocator.getUsedSpace(entry2);
            fail("order is not respected");
        }catch(IllegalArgumentException iae) {
            // OK
        }

    }

    @Test
    public void testAccumulatedFree() throws Exception {

        final PoolSpaceAllocatable<Long> spaceAllocator =
            new FairDiskSpaceAllocator<Long>(1000);

        final Long entry1 = new Long(1);
        final Long entry2 = new Long(2);
        final Long entry3 = new Long(3);
        final Long entry4 = new Long(4);

        spaceAllocator.allocate(entry1, 880, 0);
        spaceAllocator.allocate(entry2, 60, 0);
        spaceAllocator.allocate(entry3, 60, 0);

        Thread t = DiskSpaceAllocationTestHelper.allocateInThread(spaceAllocator, entry4, -1, 100);

        Thread.sleep(100);

        spaceAllocator.free(entry2);
        spaceAllocator.free(entry3);

        t.join(100);
        assertFalse("Allocation did not succeed", t.isAlive());
    }

    @Test
    public void testAllocationsListNoAllocations() throws Exception {

        final PoolSpaceAllocatable<Long> spaceAllocator =
            new FairDiskSpaceAllocator<Long>(1000);

        assertTrue("Non empty allocations list without allocations", spaceAllocator.allocations().isEmpty());

    }


    @Test
    public void testAllocationsListAfterFree() throws Exception {

        final PoolSpaceAllocatable<Long> spaceAllocator =
            new FairDiskSpaceAllocator<Long>(1000);

        final Long entry1 = new Long(1);
        final Long entry2 = new Long(2);
        final Long entry3 = new Long(3);


        spaceAllocator.allocate(entry1, 880, 0);
        spaceAllocator.allocate(entry2, 60, 0);
        spaceAllocator.allocate(entry3, 60, 0);


        assertEquals("Allocations miss match", 3 , spaceAllocator.allocations().size());


        spaceAllocator.free(entry2);

        assertEquals("Allocations miss match after free", 2 , spaceAllocator.allocations().size());

        assertFalse("allocation still in the list after freeing", spaceAllocator.allocations().contains(entry2));
    }

}
