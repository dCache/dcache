package org.dcache.pool.classic;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import org.dcache.pool.FaultAction;
import org.dcache.pool.FaultEvent;
import org.dcache.util.AdjustableSemaphore;
import org.dcache.util.FifoPriorityComparator;
import org.dcache.util.IoPrioritizable;
import org.dcache.util.IoPriority;
import org.dcache.util.LifoPriorityComparator;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;

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

    private final MoverExecutorServices _executorServices;

    public SimpleIoScheduler(String name,
                             MoverExecutorServices executorServices,
                             int queueId)
    {
        this(name, executorServices, queueId, true);
    }

    public SimpleIoScheduler(String name,
                             MoverExecutorServices executorServices,
                             int queueId,
                             boolean fifo)
    {
        _name = name;
        _executorServices = executorServices;
        _queueId = queueId;

        Comparator<IoPrioritizable> comparator =
            fifo
            ? new FifoPriorityComparator()
            : new LifoPriorityComparator();

        _queue = new PriorityBlockingQueue<>(16, comparator);

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
     * @param request
     * @param priority
     * @return mover id
     */
    @Override
    public synchronized int add(PoolIORequest request, IoPriority priority) {
        checkState(!_shutdown);

        int id = _queueId << 24 | nextId();

        if (_semaphore.getMaxPermits() <= 0) {
            _log.warn("A task was added to queue '{}', however the queue is not configured to execute any tasks.", _name);
        }

        request.setState(IoRequestState.QUEUED);
        PrioritizedRequest wrapper = new PrioritizedRequest(id, request, priority);
        _queue.add(wrapper);
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
        PrioritizedRequest pRequest = _jobs.get(id);
        if(pRequest == null) {
            throw new NoSuchElementException("Job not found : Job-" + id);
        }
        return toJobInfo(pRequest.getRequest(), id);
    }

    @Override
    public List<JobInfo> getJobInfos() {
        List<JobInfo> jobs = new ArrayList<>();

        for (Map.Entry<Integer, PrioritizedRequest> job : _jobs.entrySet()) {
            jobs.add(toJobInfo(job.getValue().getRequest(), job.getKey()));
        }

        /*
         * return unmodifiable list to 'kill' every one who wants to
         * change it (in other words, for bug tracking).
         */
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

    private void cancel(PrioritizedRequest wrapper)
    {
        if (_queue.remove(wrapper)) {
            /*
             * as request is still in the queue, we can cancel it right away.
             */
            _jobs.remove(wrapper.getId());
            PoolIORequest request = wrapper.getRequest();
            request.setState(IoRequestState.DONE);
            request.setTransferStatus(CacheException.DEFAULT_ERROR_CODE, "Transfer canceled");

            /*
             * go through the standard procedure to update billing and notify door.
             */
            String protocolName = protocolNameOf(request);
            _executorServices.getPostExecutorService(protocolName).execute(request);
        } else {
            wrapper.getRequest().kill();
        }
    }

    @Override
    public StringBuffer printJobQueue(StringBuffer sb) {
        for (Map.Entry<Integer, PrioritizedRequest> job : _jobs.entrySet()) {
            sb.append(job.getKey()).append(" : ").append(job.getValue().getRequest()).append('\n');
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
                                return input.getRequest().getProtocolInfo().getVersionString();
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
                    final PrioritizedRequest wrapp = _queue.take();
                    wrapp.getCdc().restore();

                    final PoolIORequest request = wrapp.getRequest();
                    final String protocolName = protocolNameOf(request);
                    request.transfer(_executorServices.getExecutorService(protocolName),
                        new CompletionHandler<Object,Object>() {
                        @Override
                        public void completed(Object result, Object attachment)
                        {
                            postProcess();
                        }

                        @Override
                        public void failed(Throwable e, Object attachment)
                        {
                            int rc;
                            String msg;
                            if (e instanceof InterruptedException) {
                                rc = CacheException.DEFAULT_ERROR_CODE;
                                msg = "Transfer was killed";
                            } else if (e instanceof CacheException) {
                                rc = ((CacheException) e).getRc();
                                msg = e.getMessage();
                                if (rc == CacheException.ERROR_IO_DISK) {
                                    request.getFaultListener().faultOccurred(new FaultEvent("repository", FaultAction.DISABLED, msg, e));
                                }
                            } else {
                                rc = CacheException.UNEXPECTED_SYSTEM_EXCEPTION;
                                msg = "Transfer failed due to unexpected exception: " + e;
                            }
                            request.setTransferStatus(rc, msg);
                            postProcess();
                        }

                        private void postProcess() {
                            _executorServices
                                    .getPostExecutorService(protocolName)
                                    .execute(request)
                                    .addListener(new Runnable()
                                    {
                                        @Override
                                        public void run()
                                        {
                                            request.setState(IoRequestState.DONE);
                                            _jobs.remove(wrapp.getId());
                                            _semaphore.release();
                                        }
                                    }, MoreExecutors.sameThreadExecutor());
                        }
                    });
                } catch (RuntimeException | Error | InterruptedException e) {
                    _semaphore.release();
                    throw e;
                } finally {
                    CDC.clear();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private String protocolNameOf(PoolIORequest request) {
        return request.getProtocolInfo().getProtocol() + "-"
                        + request.getProtocolInfo().getMajorVersion();
    }
    /*
     * wrapper for priority queue
     */
    private static class PrioritizedRequest implements IoPrioritizable {

        private final PoolIORequest _request;
        private final IoPriority _priority;
        private final long _ctime;
        private final int _id;
        private final CDC _cdc;

        PrioritizedRequest(int id, PoolIORequest o, IoPriority p) {
            _id = id;
            _request = o;
            _priority = p;
            _ctime = System.nanoTime();
            _cdc = new CDC();
        }

        public PoolIORequest getRequest() {
            return _request;
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
    }

    private static JobInfo toJobInfo(final PoolIORequest request, final int id) {
        return new IoJobInfo(request, id);
    }
}
