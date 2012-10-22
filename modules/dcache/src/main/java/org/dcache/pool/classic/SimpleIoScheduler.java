package org.dcache.pool.classic;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.IoJobInfo;
import diskCacheV111.vehicles.JobInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Comparator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import org.dcache.pool.FaultAction;
import org.dcache.pool.FaultEvent;
import org.dcache.util.AdjustableSemaphore;
import org.dcache.util.FifoPriorityComparator;
import org.dcache.util.IoPrioritizable;
import org.dcache.util.IoPriority;
import org.dcache.util.LifoPriorityComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import dmg.cells.nucleus.CDC;

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
    boolean _shutdown;

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
    public  int add(PoolIORequest request, IoPriority priority) {

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
    public int getActiveJobs() {
        return _semaphore.getUsedPermits();
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
    public void cancel(int id) throws NoSuchElementException {

        PrioritizedRequest wrapper;
        wrapper = _jobs.get(id);

        if (wrapper == null) {
            throw new NoSuchElementException("Job " + id + " not found");
        }

        if(_queue.remove(wrapper)) {
            /*
             * if request still in the queue, then we can cancel it right now.
             */
            _jobs.remove(id);
            final PoolIORequest request = wrapper.getRequest();
            request.setState(IoRequestState.DONE);
            request.setTransferStatus(CacheException.DEFAULT_ERROR_CODE, "Transfer canceled");

            /*
             * go though standart procedure to update billing and notity door.
             */
            final String protocolName = protocolNameOf(request);
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
    public void shutdown() {
        // NOP for now
    }

    @Override
    public void run() {
            while (!_shutdown) {

                try {
                    final PrioritizedRequest wrapp = _queue.take();
                    wrapp.getCdc().restore();
                    _semaphore.acquire();

                    final PoolIORequest request = wrapp.getRequest();
                    final String protocolName = protocolNameOf(request);
                    request.transfer(_executorServices.getExecutorService(protocolName),
                        new CompletionHandler() {

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
                            _semaphore.release();
                            _jobs.remove(wrapp.getId());
                            request.setState(IoRequestState.DONE);
                            _executorServices.getPostExecutorService(protocolName).execute(request);
                        }
                    });
                } catch(InterruptedException e) {
                    _shutdown = true;
                } finally {
                    CDC.clear();
                }
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
