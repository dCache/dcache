package org.dcache.tests.util;

import static org.junit.Assert.*;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import diskCacheV111.util.FJobScheduler;
import diskCacheV111.util.JobScheduler;

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
                Thread.sleep(_waitTime);
                if(_doneCounter != null ) {
                    _doneCounter.countDown();
                }
            } catch (InterruptedException ie) {
                // ignore
            }
        }

        @Override
        public String toString() {
            return _name;
        }
    }

    private JobScheduler _jobScheduler;

    @Before
    public void setUp() {
        _jobScheduler = new FJobScheduler(null);
    }

    @Test
    public void testSimpleJobQueue() throws InvocationTargetException, InterruptedException {

        int jobsCount = 10;
        long waitTime = 1000;
        CountDownLatch doneCounter = new CountDownLatch(jobsCount);

        _jobScheduler.setMaxActiveJobs(10);

        for (int i = 0; i < jobsCount; i++) {
            _jobScheduler.add(new ExampleJob("S-" + i, doneCounter, waitTime));
        }

        assertTrue("not all jobs are done", doneCounter.await(2 * waitTime * jobsCount, TimeUnit.MILLISECONDS));
        assertTrue("job queue is not empty", _jobScheduler.getQueueSize() == 0);

    }

}
