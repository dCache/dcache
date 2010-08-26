package org.dcache.tests.util;

import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicInteger;

import org.dcache.tests.cells.CellAdapterHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import diskCacheV111.util.ThreadManager;

public class ThreadManagerTest {


    /*
     * make Cells happy
     */
    private static final CellAdapterHelper SYSTEM_CELL_HOLDER = new CellAdapterHelper("ThreadManagerTest", "");
    private ThreadManager _threadManager = null;


    @Test
    public void testMaxDefault() throws Exception {

        _threadManager = new ThreadManager("JUnitThreadManager*", "");

        final AtomicInteger runnedThreads = new  AtomicInteger(0);

        for( int i = 0; i < _threadManager.THREAD_COUNT + 5; i++) {

            ThreadManager.execute(
                    new Runnable() {

                        //@Override
                        public void run() {
                            runnedThreads.incrementAndGet();
                            try {
                                Thread.sleep(10000);
                            } catch (InterruptedException e) {
                                // take it easy
                            }
                        }
                        }
                    );

        }

        Thread.sleep( 2000);
        assertEquals("more threads executed than allowed", _threadManager.THREAD_COUNT, runnedThreads.get());

    }

    @Test
    public void testMaxExclusive() throws Exception {

        String args = "default -num-threads=10 -thread-timeout=15";
        _threadManager = new ThreadManager("JUnitThreadManager*", args);

        final AtomicInteger runnedThreads = new  AtomicInteger(0);
        _threadManager.setMaxThreadCount(7);

        for( int i = 0; i < 100; i++) {

            ThreadManager.execute(
                    new Runnable() {

                        //@Override
                        public void run() {
                            runnedThreads.incrementAndGet();
                            try {
                                Thread.sleep(10000);
                            } catch (InterruptedException e) {
                                // take it easy
                            }
                        }
                        }
                    );

        }

        Thread.sleep( 2000);
        assertEquals("more threads executed than allowed", 7, runnedThreads.get());

    }

    @Test
    public void testMaxExclusiveByConstructor() throws Exception {

        String args = "default -num-threads=28 -thread-timeout=15";
        _threadManager = new ThreadManager("JUnitThreadManager*", args);

        final AtomicInteger runnedThreads = new  AtomicInteger(0);


        for( int i = 0; i < 100; i++) {

            ThreadManager.execute(
                    new Runnable() {

                        //@Override
                        public void run() {
                            runnedThreads.incrementAndGet();
                            try {
                                Thread.sleep(10000);
                            } catch (InterruptedException e) {
                                // take it easy
                            }
                        }
                        }
                    );

        }

        Thread.sleep( 2000);
        assertEquals("more threads executed than allowed", 28, runnedThreads.get());

    }

    @Test
    public void testSetGetMax()throws Exception  {

        String args = "default -num-threads=10 -thread-timeout=15";
        _threadManager = new ThreadManager("JUnitThreadManager*", args);

        final int max = 17;

        _threadManager.setMaxThreadCount(max);
        assertEquals("more setMaxThreadCount() do not affects result provided by getMaxThreadCount()", max, _threadManager.getMaxThreadCount() );
    }

    @After
    public void tearDown() throws Exception {
        if( _threadManager != null ) {
            CellAdapterHelper.getSystem().getNucleus().kill(_threadManager.getCellName());
        }
    }
}
