package org.dcache.tests.util;

import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import diskCacheV111.util.JobScheduler;
import diskCacheV111.util.SimpleJobScheduler;

public class JobSchedulerTest {

    public static class ExampleJob implements Runnable {
        private final CountDownLatch _doneCounter;
        private final String _name;
        private final long _waitTime;

        public ExampleJob(String name, CountDownLatch doneCounter, long waitTime) {
            _name = name;
            _doneCounter = doneCounter;
            _waitTime = waitTime;
        }

        public void run() {
            try {
                _doneCounter.countDown();
                Thread.sleep(_waitTime);
            } catch (InterruptedException ie) {
                // ignore
            }
        }

        public String toString() {
            return _name;
        }
    }

    @Test
    public void testSimpleJobQueue() throws InvocationTargetException, InterruptedException {

        int jobsCount = 10;
        long waitTime = 1000;
        CountDownLatch doneCounter = new CountDownLatch(jobsCount);

        JobScheduler jobScheduler = new SimpleJobScheduler(null);
        jobScheduler.setMaxActiveJobs(1);

        for (int i = 0; i < jobsCount; i++) {
            jobScheduler.add(new ExampleJob("S-" + i, doneCounter, waitTime));
        }

        assertTrue("not all jobs are done", doneCounter.await(2 * waitTime * jobsCount, TimeUnit.MILLISECONDS));
        assertTrue("job queue is not empty", jobScheduler.getQueueSize() == 0);

    }

}
