package org.dcache.tests.repository;

import java.util.concurrent.CountDownLatch;

import org.dcache.pool.repository.Account;

public class DiskSpaceAllocationTestHelper {


    private DiskSpaceAllocationTestHelper() {
        // no instance allowed
    }



    public static void freeEntry( final Account spaceAllocator, final long size, final long millis) {

        new Thread("DiskSpaceAllocationTestHelper") {
            @Override
            public void run() {
                try {
                    Thread.sleep(millis);
                    spaceAllocator.free(size);
                }catch(Exception e) {
                    // ignore
                }
            }
        }.start();

    }


    public static void addSpace( final Account spaceAllocator,final long newSpace, final long millis) {

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
                    spaceAllocator.allocate(size);
                }catch(Exception e) {
                    // ignore
                }
            }
            };
        t.start();
        return t;
    }
}
