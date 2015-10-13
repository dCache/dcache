package org.dcache.pool.classic;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Ordering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.InterruptedIOException;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.IoJobInfo;
import diskCacheV111.vehicles.JobInfo;

import dmg.cells.nucleus.CDC;

import org.dcache.pool.movers.Mover;
import org.dcache.util.AdjustableSemaphore;
import org.dcache.util.IoPrioritizable;
import org.dcache.util.IoPriority;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static org.dcache.pool.classic.IoRequestState.*;

/**
 *
 * @since 1.9.11
 */
public class SimpleIoScheduler implements IoScheduler, Runnable {

    private final static Logger _log =
            LoggerFactory.getLogger(SimpleIoScheduler.class);

    /**
     * A worker thread for request queue processing.
     */
    private final Thread _worker;

    /**
     * The name of IoScheduler.
     */
    private final String _name;

    /**
     * request queue.
     */
    private final BlockingQueue<PrioritizedRequest> _queue;

    private final Map<Integer, PrioritizedRequest> _jobs =
        new ConcurrentHashMap<>();

    /**
     * ID of the current queue. Used to identify queue in {@link
     * IoQueueManager}.
     */
    private final int _queueId;

    /**
     * job id generator
     */
    private int _nextId;

    /**
     * are we need to shutdown.
     */
    private volatile boolean _shutdown;

    private final AdjustableSemaphore _semaphore = new AdjustableSemaphore();

    public SimpleIoScheduler(String name, int queueId, boolean fifo)
    {
        _name = name;
        _queueId = queueId;

        /* PriorityBlockinQueue returns the least elements first, that is, the
         * the highest priority requests have to be first in the ordering.
         */
        Function<IoPrioritizable, Comparable> getPriority =
                new Function<IoPrioritizable, Comparable>()
                {
                    @Nullable
                    @Override
                    public Comparable apply(IoPrioritizable input)
                    {
                        return input.getPriority();
                    }
                };
        Function<IoPrioritizable, Comparable> getCreationTime =
                new Function<IoPrioritizable, Comparable>()
                {
                    @Nullable
                    @Override
                    public Comparable apply(IoPrioritizable input)
                    {
                        return input.getCreateTime();
                    }
                };
        Ordering<IoPrioritizable> ordering =
                fifo
                ? Ordering.natural()
                        .onResultOf(getPriority)
                        .reverse()
                        .compound(Ordering.natural().onResultOf(getCreationTime))
                : Ordering.natural()
                        .onResultOf(getPriority)
                        .compound(Ordering.natural().onResultOf(getCreationTime))
                        .reverse();

        _queue = new PriorityBlockingQueue<>(16, ordering);

        _semaphore.setMaxPermits(2);

        _worker = new Thread(this);
        _worker.setName(_name);
        _worker.start();
    }

    /**
     * Add a request into the queue. The returned id is composed from queue id
     * and internal counter:
     *   | 31- queue id -24|23- job id -0|
     *
     * @param mover
     * @param priority
     * @return mover id
     */
    @Override
    public synchronized int add(Mover<?> mover, IoPriority priority) {
        checkState(!_shutdown);

        int id = _queueId << 24 | nextId();

        if (_semaphore.getMaxPermits() <= 0) {
            _log.warn("A task was added to queue '{}', however the queue is not configured to execute any tasks.", _name);
        }

        final PrioritizedRequest wrapper = new PrioritizedRequest(id, mover, priority);

        if (_semaphore.tryAcquire()) {
            /*
             * there is a free slot in the queue - use it!
             */
            sendToExecution(wrapper);
        } else {
            _queue.add(wrapper);
        }

        _jobs.put(id, wrapper);

        return id;
    }

    private synchronized int nextId() {
        if(_nextId == 0x00FFFFFF) {
            _nextId = 0;
        }else{
            _nextId++;
        }
        return _nextId;
    }

    @Override
    public synchronized int getActiveJobs() {
        return _jobs.size() - _queue.size();
    }

    @Override
    public JobInfo getJobInfo(int id) {
        PrioritizedRequest request = _jobs.get(id);
        if(request == null) {
            throw new NoSuchElementException("Job not found : Job-" + id);
        }
        return request.toJobInfo();
    }

    @Override
    public List<JobInfo> getJobInfos() {
        List<JobInfo> jobs = new ArrayList<>();
        for (PrioritizedRequest request : _jobs.values()) {
            jobs.add(request.toJobInfo());
        }
        return Collections.unmodifiableList(jobs);
    }

    @Override
    public int getMaxActiveJobs() {
        return _semaphore.getMaxPermits();
    }

    @Override
    public int getQueueSize() {
        return _queue.size();
    }

    @Override
    public int getCountByPriority(IoPriority priority)
    {
        int count = 0;
        for (PrioritizedRequest request: _queue) {
            if (request.getPriority() == priority) {
                count++;
            }
        }
        return count;
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public synchronized void cancel(int id) throws NoSuchElementException {
        PrioritizedRequest wrapper;
        wrapper = _jobs.get(id);
        if (wrapper == null) {
            throw new NoSuchElementException("Job " + id + " not found");
        }
        cancel(wrapper);
    }

    private void cancel(final PrioritizedRequest request)
    {
        try (CDC ignored = request.getCdc().restore()) {
            if (_queue.remove(request)) {
                /* The request was still in the queue. Post processing is still applied to close
             * the transfer and to notify billing and door.
             */
                request.kill();
                request.getMover().postprocess(
                        new CompletionHandler<Void,Void>()
                        {
                            @Override
                            public void completed(Void result, Void attachment)
                            {
                                release();
                            }

                            @Override
                            public void failed(Throwable exc, Void attachment)
                            {
                                release();
                            }

                            private void release()
                            {
                                request.done();
                                _jobs.remove(request.getId());
                            }

                        });
            } else {
                request.kill();
            }
        }
    }

    @Override
    public StringBuffer printJobQueue(StringBuffer sb) {
        for (Map.Entry<Integer, PrioritizedRequest> job : _jobs.entrySet()) {
            sb.append(job.getKey()).append(" : ").append(job.getValue()).append('\n');
        }
        return sb;
    }

    @Override
    public void setMaxActiveJobs(int maxJobs) {
        _semaphore.setMaxPermits(maxJobs);
    }

    @Override
    public synchronized void shutdown() throws InterruptedException
    {
        if (!_shutdown) {
            _shutdown = true;
            _worker.interrupt();
            for (PrioritizedRequest request : _jobs.values()) {
                cancel(request);
            }
            _log.info("Waiting for movers on queue '{}' to finish", _name);
            if (!_semaphore.tryAcquire(_semaphore.getMaxPermits(), 2000L, TimeUnit.MILLISECONDS)) {
                // This is often due to a mover not reacting to interrupt or the transfer
                // doing a lengthy checksum calculation during post processing.
                _log.warn("Failed to terminate some movers prior to shutdown: {}",
                        Joiner.on(",").join(transform(_jobs.values(), new Function<PrioritizedRequest, String>()
                        {
                            @Override
                            public String apply(PrioritizedRequest input)
                            {
                                return input.getMover().getProtocolInfo().getVersionString();
                            }
                        })));
            }
        }
    }

    @Override
    public void run() {
        try {
            while (!_shutdown) {
                _semaphore.acquire();
                try {
                    final PrioritizedRequest request = _queue.take();
                    sendToExecution(request);
                } catch (RuntimeException | Error | InterruptedException e) {
                    _semaphore.release();
                    throw e;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void sendToExecution(final PrioritizedRequest request) {
        try (CDC ignore = request.getCdc().restore()) {
            request.transfer(
                    new CompletionHandler<Void, Void>() {
                        @Override
                        public void completed(Void result, Void attachment) {
                            postprocess();
                        }

                        @Override
                        public void failed(Throwable exc, Void attachment) {
                            if (exc instanceof InterruptedException || exc instanceof InterruptedIOException) {
                                request.getMover().setTransferStatus(CacheException.DEFAULT_ERROR_CODE, "Transfer was killed");
                            }
                            postprocess();
                        }

                        private void postprocess() {
                            request.getMover().postprocess(
                                    new CompletionHandler<Void, Void>() {
                                        @Override
                                        public void completed(Void result, Void attachment) {
                                            release();
                                        }

                                        @Override
                                        public void failed(Throwable exc, Void attachment) {
                                            release();
                                        }

                                        private void release() {
                                            request.done();
                                            _jobs.remove(request.getId());
                                            _semaphore.release();
                                        }
                                    });
                        }
                    });
        }
    }

    private static class PrioritizedRequest implements IoPrioritizable  {
        private final Mover<?> _mover;
        private final IoPriority _priority;
        private final long _ctime;
        private final int _id;
        private final CDC _cdc;
        private IoRequestState _state;

        /**
         * Request creation time.
         */
        private final long _submitTime;

        /**
         * Transfer start time.
         */
        private long _startTime;
        private Cancellable _cancellable;

        PrioritizedRequest(int id, Mover<?> mover, IoPriority p) {
            _id = id;
            _mover = mover;
            _priority = p;
            _ctime = System.nanoTime();
            _submitTime = System.currentTimeMillis();
            _state = QUEUED;
            _cdc = new CDC();
        }

        public Mover<?> getMover() {
            return _mover;
        }

        public CDC getCdc() {
            return _cdc;
        }

        public int getId() {
            return _id;
        }

        @Override
        public IoPriority getPriority() {
            return _priority;
        }

        @Override
        public long getCreateTime() {
            return _ctime;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof PrioritizedRequest)) {
                return false;
            }

            final PrioritizedRequest other = (PrioritizedRequest) o;
            return _id == other._id;
        }

        @Override
        public int hashCode() {
            return _id;
        }

        @Override
        public synchronized String toString() {
            return _state + " : " + _mover.toString();
        }

        public synchronized JobInfo toJobInfo() {
            return new IoJobInfo(_submitTime, _startTime, _state, _id, _mover);
        }

        public synchronized void transfer(CompletionHandler<Void,Void> completionHandler) {
            try {
                if (_state != QUEUED) {
                    completionHandler.failed(new InterruptedException("Transfer cancelled"), null);
                }
                _state = RUNNING;
                _startTime = System.currentTimeMillis();
                _cancellable = _mover.execute(completionHandler);
            } catch (RuntimeException e) {
                completionHandler.failed(e, null);
            }
        }

        public synchronized void kill()
        {
            if (_state == CANCELED || _state == DONE) {
                return;
            }

            if (_cancellable != null) {
                _cancellable.cancel();
            } else {
                _mover.setTransferStatus(CacheException.DEFAULT_ERROR_CODE, "Transfer cancelled");
            }
            _state = CANCELED;
        }

        public synchronized void done()
        {
            _state = DONE;
        }
    }
}
