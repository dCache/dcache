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

class IoQueueManager implements JobScheduler {

    private final static Logger _log = LoggerFactory.getLogger(IoQueueManager.class);

    private List<JobScheduler> _list = new ArrayList<JobScheduler>();
    private Map<String, JobScheduler> _hash = new HashMap<String, JobScheduler>();
    private boolean _isConfigured = false;

    public IoQueueManager(String ioQueueList, JobTimeoutManager jobTimeoutManager) {
        _isConfigured = (ioQueueList != null) && (ioQueueList.length() > 0);
        if (!_isConfigured) {
            ioQueueList = "regular";
        }
        for (String queueName : ioQueueList.split(",")) {
            boolean fifo = !queueName.startsWith("-");
            if (!fifo) {
                queueName = queueName.substring(1);
            }
            if (_hash.get(queueName) != null) {
                _log.error("Queue not created, name already exists: " + queueName);
                continue;
            }
            int id = _list.size();
            JobScheduler job = new SimpleJobScheduler(queueName, fifo);
            _list.add(job);
            _hash.put(queueName, job);
            job.setSchedulerId(id);
            jobTimeoutManager.addScheduler(queueName, job);
        }
        if (!_isConfigured) {
            _log.info("No custom mover queues defined");
        } else {
            _log.info("Mover queues defined: " + _hash.toString());
        }
    }

    public synchronized boolean isConfigured() {
        return _isConfigured;
    }

    public synchronized JobScheduler getDefaultScheduler() {
        return _list.get(0);
    }

    public synchronized Collection<JobScheduler> getSchedulers() {
        return Collections.unmodifiableCollection(_list);
    }

    public synchronized JobScheduler getSchedulerByName(String queueName) {
        return _hash.get(queueName);
    }

    public synchronized JobScheduler getSchedulerById(int id) {
        int pos = id % 10;
        if (pos >= _list.size()) {
            throw new IllegalArgumentException("Invalid id (doesn't below to any known scheduler)");
        }
        return _list.get(pos);
    }

    public synchronized JobInfo getJobInfo(int id) {
        return getSchedulerById(id).getJobInfo(id);
    }

    public synchronized int add(String queueName, Runnable runnable, int priority) throws InvocationTargetException {
        JobScheduler js = (queueName == null) ? null : (JobScheduler) _hash.get(queueName);
        return (js == null) ? add(runnable, priority) : js.add(runnable, priority);
    }

    public synchronized int add(Runnable runnable) throws InvocationTargetException {
        return getDefaultScheduler().add(runnable);
    }

    public synchronized int add(Runnable runnable, int priority) throws InvocationTargetException {
        return getDefaultScheduler().add(runnable, priority);
    }

    public synchronized void kill(int jobId, boolean force) throws NoSuchElementException {
        getSchedulerById(jobId).kill(jobId, force);
    }

    public synchronized void remove(int jobId) throws NoSuchElementException {
        getSchedulerById(jobId).remove(jobId);
    }

    public synchronized StringBuffer printJobQueue(StringBuffer sb) {
        if (sb == null) {
            sb = new StringBuffer();
        }
        for (JobScheduler s : _list) {
            s.printJobQueue(sb);
        }
        return sb;
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

    public synchronized void setMaxActiveJobs(int maxJobs) {
    }

    public synchronized List<JobInfo> getJobInfos() {
        List<JobInfo> list = new ArrayList<JobInfo>();
        for (JobScheduler s : _list) {
            list.addAll(s.getJobInfos());
        }
        return list;
    }

    public void setSchedulerId(int id) {
    }

    public String getSchedulerName() {
        return "Manager";
    }

    public int getSchedulerId() {
        return -1;
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
