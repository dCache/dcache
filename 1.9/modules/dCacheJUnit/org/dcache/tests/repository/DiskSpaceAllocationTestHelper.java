package org.dcache.tests.repository;

import java.util.concurrent.CountDownLatch;

import org.dcache.pool.repository.PoolSpaceAllocatable;

import diskCacheV111.repository.SpaceRequestable;

public class DiskSpaceAllocationTestHelper {


    private DiskSpaceAllocationTestHelper() {
        // no instance allowed
    }



    public static void freeEntry( final PoolSpaceAllocatable<Long> spaceAllocator, final Long entry, final long millis) {

        new Thread("DiskSpaceAllocationTestHelper") {
            @Override
            public void run() {
                try {
                    Thread.sleep(millis);
                    spaceAllocator.free(entry);
                }catch(Exception e) {
                    // ignore
                }
            }
        }.start();

    }


    public static void addSpace( final PoolSpaceAllocatable<Long> spaceAllocator,final long newSpace, final long millis) {

        new Thread("DiskSpaceAllocationTestHelper") {
            @Override
            public void run() {
                try {
                    Thread.sleep(millis);
                    spaceAllocator.setTotalSpace(newSpace);
                }catch(Exception e) {
                    // ignore
                }
            }
        }.start();

    }

    public static SpaceRequestable cleanOnDemand( final PoolSpaceAllocatable<Long> spaceAllocator, final Long entry) {

        return new SpaceRequestable() {

            public void spaceNeeded(long space) {
                spaceAllocator.free(entry);

            }

        };

    }

    public static Thread allocateInThread( final PoolSpaceAllocatable<Long> spaceAllocator, final Long entry, final long millis, final long size) {


        Thread t = new Thread("DiskSpaceAllocationTestHelper") {
            @Override
            public void run() {
                try {
                    spaceAllocator.allocate(entry, size, millis);
                }catch(Exception e) {
                    // ignore
                }
            }
            };
        t.start();
        return t;
    }
}
