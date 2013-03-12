package org.dcache.pool.classic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import diskCacheV111.vehicles.JobInfo;

import org.dcache.util.IoPriority;

public class IoQueueManager {

    private final static Logger _log = LoggerFactory.getLogger(IoQueueManager.class);

    public static final String DEFAULT_QUEUE = "regular";
    private List<IoScheduler> _list = new ArrayList<>();
    private final Map<String, IoScheduler> _hash = new HashMap<>();

    public IoQueueManager(JobTimeoutManager jobTimeoutManager, String[] queues,
            MoverExecutorServices executorServices) {

        if(queues == null) {
            throw new IllegalArgumentException("queue names can't be null");
        }

        addQueue(DEFAULT_QUEUE, jobTimeoutManager, executorServices);
        for (String queueName : queues) {
            queueName = queueName.trim();
            if(queueName.isEmpty()) {
                continue;
            }

            addQueue(queueName, jobTimeoutManager, executorServices);
        }

        _log.info("Defined IO queues {}: " + _hash.keySet());
    }

    private void addQueue(String queueName, JobTimeoutManager jobTimeoutManager,
            MoverExecutorServices executorServices ) {
        boolean fifo = !queueName.startsWith("-");
        if (!fifo) {
            queueName = queueName.substring(1);
        }
        if (_hash.get(queueName) == null) {
            _log.info("adding queue: {}", queueName);
            int id = _list.size();
            IoScheduler job = new SimpleIoScheduler(queueName, executorServices, id, fifo);
            _list.add(job);
            _hash.put(queueName, job);
            jobTimeoutManager.addScheduler(queueName, job);
        }else{
            _log.warn("Queue not created, name already exists: " + queueName);
        }
    }

    public synchronized IoScheduler getDefaultScheduler() {
        return _list.get(0);
    }

    public synchronized Collection<IoScheduler> getSchedulers() {
        return Collections.unmodifiableCollection(_list);
    }

    public synchronized IoScheduler getQueue(String queueName) {
        return _hash.get(queueName);
    }

    /**
     * Get {@link List} of defined {@link JobScheduler}s.
     * @return schedulers.
     */
    public synchronized List<IoScheduler> getQueues() {
        return new ArrayList<>(_list);
    }

    public synchronized IoScheduler getQueueByJobId(int id) {
        int pos = id >> 24;
        if (pos >= _list.size()) {
            throw new IllegalArgumentException("Invalid id (doesn't below to any known scheduler)");
        }
        return _list.get(pos);
    }

    public synchronized int add(String queueName, PoolIORequest request, IoPriority priority)
    {
        IoScheduler js = (queueName == null) ? null : _hash.get(queueName);
        return (js == null) ? add(request, priority) : js.add(request, priority);
    }

    public synchronized int add(PoolIORequest request, IoPriority priority)
    {
        return getDefaultScheduler().add(request, priority);
    }

    public synchronized void cancel(int jobId) throws NoSuchElementException {
        getQueueByJobId(jobId).cancel(jobId);
    }

    public synchronized int getMaxActiveJobs() {
        int sum = 0;
        for (IoScheduler s : _list) {
            sum += s.getMaxActiveJobs();
        }
        return sum;
    }

    public synchronized int getActiveJobs() {
        int sum = 0;
        for (IoScheduler s : _list) {
            sum += s.getActiveJobs();
        }
        return sum;
    }

    public synchronized int getQueueSize() {
        int sum = 0;
        for (IoScheduler s : _list) {
            sum += s.getQueueSize();
        }
        return sum;
    }

    public synchronized List<JobInfo> getJobInfos() {
        List<JobInfo> list = new ArrayList<>();
        for (IoScheduler s : _list) {
            list.addAll(s.getJobInfos());
        }
        return list;
    }

    public synchronized void printSetup(PrintWriter pw) {
        for (IoScheduler s : _list) {
            pw.println("mover set max active -queue=" + s.getName() + " " + s.getMaxActiveJobs());
        }
    }

    public synchronized JobInfo findJob(String client, long id) {
        for (JobInfo info : getJobInfos()) {
            if (client.equals(info.getClientName()) && id == info.getClientId()) {
                return info;
            }
        }
        return null;
    }

    public synchronized void shutdown() {
        for (IoScheduler s : _list) {
            s.shutdown();
        }
    }
}
