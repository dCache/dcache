package org.dcache.pool.classic;

import diskCacheV111.util.JobScheduler;
import diskCacheV111.util.SimpleJobScheduler;
import diskCacheV111.vehicles.JobInfo;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IoQueueManager {

    private final static Logger _log = LoggerFactory.getLogger(IoQueueManager.class);

    public static final String DEFAULT_QUEUE = "regular";
    private List<JobScheduler> _list = new ArrayList<JobScheduler>();
    private Map<String, JobScheduler> _hash = new HashMap<String, JobScheduler>();

    public IoQueueManager(JobTimeoutManager jobTimeoutManager, String[] queues) {

        if(queues == null) {
            throw new IllegalArgumentException("queue names can't be null");
        }

        addQueue(DEFAULT_QUEUE, jobTimeoutManager);
        for (String queueName : queues) {
            queueName = queueName.trim();
            if(queueName.isEmpty()) continue;

            addQueue(queueName, jobTimeoutManager);
        }

        _log.info("Defined IO queues {}: " + _hash.keySet());
    }

    private void addQueue(String queueName, JobTimeoutManager jobTimeoutManager) {
        boolean fifo = !queueName.startsWith("-");
        if (!fifo) {
            queueName = queueName.substring(1);
        }
        if (_hash.get(queueName) == null) {
            _log.info("adding queue: {}", queueName);
            int id = _list.size();
            JobScheduler job = new SimpleJobScheduler(queueName, fifo);
            _list.add(job);
            _hash.put(queueName, job);
            job.setSchedulerId(id);
            jobTimeoutManager.addScheduler(queueName, job);
        }else{
            _log.warn("Queue not created, name already exists: " + queueName);
        }
    }

    public synchronized JobScheduler getDefaultScheduler() {
        return _list.get(0);
    }

    public synchronized Collection<JobScheduler> getSchedulers() {
        return Collections.unmodifiableCollection(_list);
    }

    public synchronized JobScheduler getQueue(String queueName) {
        return _hash.get(queueName);
    }

    /**
     * Get {@link List} of defined {@link JobScheduler}s.
     * @return schedulers.
     */
    public synchronized List<JobScheduler> getQueues() {
        return new ArrayList<JobScheduler>(_list);
    }

    public synchronized JobScheduler getQueueByJobId(int id) {
        int pos = id % 10;
        if (pos >= _list.size()) {
            throw new IllegalArgumentException("Invalid id (doesn't below to any known scheduler)");
        }
        return _list.get(pos);
    }

    public synchronized JobInfo getJobInfo(int id) {
        return getQueueByJobId(id).getJobInfo(id);
    }

    public synchronized int add(String queueName, PoolIORequest request, JobScheduler.Priority priority) throws InvocationTargetException {
        JobScheduler js = (queueName == null) ? null : _hash.get(queueName);
        return (js == null) ? add(request, priority) : js.add(request, priority);
    }

    public synchronized int add(PoolIORequest request, JobScheduler.Priority priority) throws InvocationTargetException {
        return getDefaultScheduler().add(request, priority);
    }

    public synchronized void kill(int jobId, boolean force) throws NoSuchElementException {
        getQueueByJobId(jobId).kill(jobId, force);
    }

    public synchronized void remove(int jobId) throws NoSuchElementException {
        getQueueByJobId(jobId).remove(jobId);
    }

    public synchronized int getMaxActiveJobs() {
        int sum = 0;
        for (JobScheduler s : _list) {
            sum += s.getMaxActiveJobs();
        }
        return sum;
    }

    public synchronized int getActiveJobs() {
        int sum = 0;
        for (JobScheduler s : _list) {
            sum += s.getActiveJobs();
        }
        return sum;
    }

    public synchronized int getQueueSize() {
        int sum = 0;
        for (JobScheduler s : _list) {
            sum += s.getQueueSize();
        }
        return sum;
    }

    public synchronized List<JobInfo> getJobInfos() {
        List<JobInfo> list = new ArrayList<JobInfo>();
        for (JobScheduler s : _list) {
            list.addAll(s.getJobInfos());
        }
        return list;
    }

    public synchronized void printSetup(PrintWriter pw) {
        for (JobScheduler s : _list) {
            pw.println("mover set max active -queue=" + s.getSchedulerName() + " " + s.getMaxActiveJobs());
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
        for (JobScheduler s : _list) {
            s.shutdown();
        }
    }
}
