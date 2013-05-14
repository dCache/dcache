package diskCacheV111.util;

import com.google.common.base.Joiner;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import dmg.cells.nucleus.CDC;

import org.dcache.commons.util.NDC;
import org.dcache.util.CDCExecutorServiceDecorator;
import org.dcache.util.FireAndForgetTask;

public class SimpleJobScheduler implements JobScheduler, Runnable
{
    private static final Logger LOGGER =
        LoggerFactory.getLogger(SimpleJobScheduler.class);

    private static final int WAITING = 10;
    private static final int ACTIVE = 11;
    private static final int KILLED = 12;
    private static final int REMOVED = 13;

    private int _maxActiveJobs = 2;
    private int _activeJobs;
    private int _nextId = 1000;
    private final Object _lock = new Object();
    private final Thread _worker;
    private final Queue<SJob> _queue = new ArrayDeque<>();
    private final Map<Integer, SJob> _jobs = new HashMap<>();
    private final String _name;

    private String[] _st_string = { "W", "A", "K", "R" };

    private final ExecutorService _jobExecutor;

    private class SJob implements Job, Runnable {

        private final long _submitTime = System.currentTimeMillis();
        private long _startTime;
        private int _status = WAITING;
        private final Queable _runnable;
        private final int _id;
        private CDC _cdc;

        private SJob(Queable runnable, int id) {
            _runnable = runnable;
            _id = id;
            _cdc = new CDC();
        }

        @Override
        public int getJobId() {
            return _id;
        }

        @Override
        public String getStatusString() {
            return _st_string[_status - WAITING];
        }

        public int getId() {
            return _id;
        }

        @Override
        public synchronized long getStartTime() {
            return _startTime;
        }

        @Override
        public synchronized long getSubmitTime() {
            return _submitTime;
        }

        public synchronized void start() {
            _jobExecutor.submit(new FireAndForgetTask(this));
            _status = ACTIVE;
        }

        public synchronized void kill()
        {
            _runnable.kill();
        }

        @Override
        public void run() {
            synchronized (this) {
                _startTime = System.currentTimeMillis();
            }

            try {
                _cdc.restore();
                NDC.push("job=" + _id);
                _runnable.run();
            } finally {
                CDC.clear();
                synchronized (this) {
                    _status = REMOVED;
                }
                synchronized (_lock) {
                    _jobs.remove(_id);
                    _activeJobs--;
                    _lock.notifyAll();
                }
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(_id).append(" ").append(getStatusString()).append(" ");
            sb.append(_runnable.toString());
            return sb.toString();
        }

        @Override
        public int hashCode() {
            return _id;
        }

        @Override
        public boolean equals(Object o) {

            return (o instanceof SJob) && (((SJob) o)._id == _id);
        }
    }

    public SimpleJobScheduler(String name) {
        _name = name;
        _jobExecutor = new CDCExecutorServiceDecorator(Executors.newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat(_name + "-worker-%d").build()));
        _worker = new Thread(this);
        _worker.setName(_name);
        _worker.start();
    }

    @Override
    public int add(Queable runnable) throws InvocationTargetException {
        synchronized (_lock) {
            int id = _nextId++;

            try {
                runnable.queued(id);
            } catch (Throwable ee) {
                throw new InvocationTargetException(ee, "reported by queued");
            }

            if (_maxActiveJobs <= 0) {
                LOGGER.warn("A task was added to queue '{}', however the queue is not configured to execute any tasks.", _name);
            }

            SJob job = new SJob(runnable, id);
            _jobs.put(id, job);
            _queue.add(job);
            _lock.notifyAll();
            return id;

        }
    }

    @Override
    public String printJobQueue() {
        synchronized (_lock) {
            return Joiner.on('\n').join(_jobs.values());
        }
    }

    @Override
    public void kill(int jobId, boolean force)
        throws IllegalStateException, NoSuchElementException
    {
        synchronized (_lock) {
            SJob job = _jobs.get(jobId);
            if (job == null) {
                throw new NoSuchElementException("Job "+ jobId + " not found");
            }
            switch (job._status) {
            case WAITING:
                _queue.remove(job);
                _jobs.remove(job._id);
                job._runnable.unqueued();
                break;
            case ACTIVE:
                if (!force) {
                    throw new IllegalStateException("Job is active. Use force to remove.");
                }
                job.kill();
                break;
            default:
                throw new IllegalStateException("Job is " + job.getStatusString());
            }
        }
    }

    @Override
    public int getQueueSize() {
        return _queue.size();
    }

    @Override
    public int getActiveJobs() {
        return _activeJobs;
    }

    @Override
    public int getMaxActiveJobs() {
        return _maxActiveJobs;
    }

    @Override
    public void setMaxActiveJobs(int max) {
        if( max < 0) {
            throw new IllegalArgumentException( "new maximum value must be zero or greater");
        }

        synchronized (_lock) {
            _maxActiveJobs = max;
            _lock.notifyAll();
        }
    }

    @Override
    public void shutdown() {
        _worker.interrupt();
    }

    @Override
    public void run() {
        synchronized (_lock) {
            try {
                try {
                    while (true) {
                        while (_activeJobs < _maxActiveJobs && !_queue.isEmpty()) {
                            SJob job = _queue.poll();
                            _activeJobs++;
                            job.start();
                        }
                        _lock.wait();
                    }
                } catch (InterruptedException e) {
                }

                //
                // shutdown
                //

                for (SJob job : _jobs.values()) {
                    if (job._status == WAITING) {
                        job._runnable.unqueued();
                    } else if (job._status == ACTIVE) {
                        job.kill();
                    }
                    long start = System.currentTimeMillis();
                    while ((_activeJobs > 0)
                           && ((System.currentTimeMillis() - start) > 10000)) {
                        try {
                            _lock.wait(1000);
                        } catch (InterruptedException ee) {
                            // for 10 seconds we simply ignore the interruptions
                            // after that we quit anyway

                        }
                    }
                }
            } finally {
                _jobExecutor.shutdownNow();
            }
        }
    }

}
