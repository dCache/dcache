package org.dcache.tests.repository;

import static org.junit.Assert.*;

import java.util.MissingResourceException;
import java.util.Random;

import org.junit.Ignore;
import org.junit.Test;

import diskCacheV111.repository.FairQueueAllocation;
import diskCacheV111.repository.SpaceMonitor;

public class PoolSpaceManagerTest {



    private SpaceMonitor _poolSpaceMonitor = null;
    private final Random _random = new Random();


    @Test
    @Ignore
    public void testOverbook() {


        long space = _random.nextLong();

        _poolSpaceMonitor = new FairQueueAllocation(space);

        try {
            _poolSpaceMonitor.allocateSpace(space + 2, 1000);
        } catch (MissingResourceException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail("Trying to allocate more than available");
        } catch (InterruptedException e) {
            fail("Trying to allocate more than available");
        }


    }

    @Test
    @Ignore
    public void testFreeSpaceCount() throws InterruptedException {
        // can't really use a random here
        long space = 2*1024*1024*1024; // 2GB
        long allocation = 512;
        _poolSpaceMonitor = new FairQueueAllocation(space);

        _poolSpaceMonitor.allocateSpace(allocation);

        assertEquals("free space miss calculation", space - allocation, _poolSpaceMonitor.getFreeSpace());

    }

    @Test
    @Ignore
    public void testFreeingSpace() throws InterruptedException {
        // can't really use a random here
        long space = 2*1024*1024*1024; // 2GB
        long allocation = 512;
        _poolSpaceMonitor = new FairQueueAllocation(space);

        _poolSpaceMonitor.allocateSpace(allocation);
        _poolSpaceMonitor.freeSpace(allocation);

        assertEquals("space freeing miss calculation", space, _poolSpaceMonitor.getFreeSpace());

    }

}
