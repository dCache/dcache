package org.dcache.tests.repository;

import org.dcache.pool.repository.Account;

import static org.dcache.tests.repository.DiskSpaceAllocatorTest.ID;

public class DiskSpaceAllocationTestHelper {


    private DiskSpaceAllocationTestHelper() {
        // no instance allowed
    }


    /**
     * Execute the <code>spaceAllocator</code> object's <code>free()</code> method
     * for the given entry after a delay of at least milli milliseconds.
     * @param spaceAllocator The Account object to free space within
     * @param entry The size of data to remove
     * @param millis The minimum delay, in milliseconds, before executing spaceAllocator.free().
     */
    public static void delayedFreeEntry( final Account spaceAllocator, final long size, final long millis) {

        new Thread("DiskSpaceAllocationTestHelper") {
            @Override
            public void run() {
                try {
                    Thread.sleep(millis);
                    spaceAllocator.free(ID, size);
                }catch(Exception e) {
                    // ignore
                }
            }
        }.start();

    }


    public static void delayedAddSpace( final Account spaceAllocator,final long newSpace, final long millis) {

        new Thread("DiskSpaceAllocationTestHelper") {
            @Override
            public void run() {
                try {
                    Thread.sleep(millis);
                    spaceAllocator.setTotal(newSpace);
                }catch(Exception e) {
                    // ignore
                }
            }
        }.start();

    }

    public static Thread allocateInThread( final Account spaceAllocator, final long size) {


        Thread t = new Thread("DiskSpaceAllocationTestHelper") {
            @Override
            public void run() {
                try {
                    spaceAllocator.allocate(ID, size);
                }catch(Exception e) {
                    // ignore
                }
            }
            };
        t.start();
        return t;
    }
}
