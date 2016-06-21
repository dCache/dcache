package org.dcache.util;

import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class AtomicCounterTest
{
    private AtomicCounter counter;
    private CountDownLatch latch;

    @Before
    public void setup()
    {
        latch = new CountDownLatch(1);
        counter = new AtomicCounter() {
            @Override
            void inLock()
            {
                latch.countDown();
            }
        };
    }

    @Test
    public void startsAtZero()
    {
        assertEquals(0, counter.get());
    }

    @Test
    public void incrementByOne()
    {
        counter.increment();
        assertEquals(1, counter.get());
    }

    @Test
    public void incrementIsThreadSafe()
        throws InterruptedException
    {
        final int THREADS = 5;
        final int ITERATIONS = 100000;
        ExecutorService executor = Executors.newCachedThreadPool();

        for (int i = 0; i < THREADS; i++) {
            executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        for (int i = 0; i < ITERATIONS; i++) {
                            counter.increment();
                        }
                    }
                });
        }

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        assertEquals(THREADS * ITERATIONS, counter.get());
    }

    @Test
    public void awaitReturnsImmediately()
        throws InterruptedException
    {
        assertTrue(counter.awaitChangeUntil(1, new Date(System.currentTimeMillis() + 2000)));
    }

    @Test
    public void awaitTimesOutInFuture()
        throws InterruptedException
    {
        assertFalse(counter.awaitChangeUntil(0, new Date(System.currentTimeMillis() + 100)));
    }

    @Test
    public void awaitTimesOutImmediately()
        throws InterruptedException
    {
        assertFalse(counter.awaitChangeUntil(0, new Date(System.currentTimeMillis() - 100)));
    }

    @Test(expected=InterruptedException.class)
    public void awaitIsInterruptible()
        throws InterruptedException
    {
        Thread.currentThread().interrupt();
        counter.awaitChangeUntil(0, new Date(System.currentTimeMillis() + 200));
    }

    @Test
    public void incrementWakensAwait()
        throws InterruptedException
    {
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            latch.await();
                            counter.increment();
                        } catch (InterruptedException e) {
                            fail("Test was interrupted");
                        }
                    }
                });
            assertTrue(counter.awaitChangeUntil(0, new Date(System.currentTimeMillis() + 200)));
        } finally {
            executor.shutdownNow();
        }
    }
}
