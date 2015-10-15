package org.dcache.pool.classic;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import diskCacheV111.vehicles.JobInfo;

import org.dcache.pool.movers.Mover;
import org.dcache.util.IoPriority;

import static com.google.common.collect.Iterables.concat;
import static java.util.Arrays.asList;

public class IoQueueManager {

    private final static Logger _log = LoggerFactory.getLogger(IoQueueManager.class);

    public static final String DEFAULT_QUEUE = "regular";
    private final ImmutableList<MoverRequestScheduler> _queues;
    private final ImmutableMap<String, MoverRequestScheduler> _queuesByName;

    public IoQueueManager(JobTimeoutManager jobTimeoutManager, String[] names) {
        Map<String,MoverRequestScheduler> queuesByName = new HashMap<>();
        List<MoverRequestScheduler> queues = new ArrayList<>();
        for (String name : concat(asList(DEFAULT_QUEUE), asList(names))) {
            name = name.trim();
            if (!name.isEmpty()) {
                boolean fifo = name.startsWith("-");
                if (fifo) {
                    name = name.substring(1);
                }
                if (!queuesByName.containsKey(name)) {
                    _log.debug("Creating queue: {}", name);
                    MoverRequestScheduler job = new MoverRequestScheduler(name, queues.size(), fifo);
                    queues.add(job);
                    queuesByName.put(name, job);
                    jobTimeoutManager.addScheduler(name, job);
                } else {
                    _log.warn("Queue not created, name already exists: {}", name);
                }
            }
        }
        _queues = ImmutableList.copyOf(queues);
        _queuesByName = ImmutableMap.copyOf(queuesByName);
        _log.debug("Defined IO queues: {}", _queuesByName.keySet());
    }

    public MoverRequestScheduler getDefaultQueue() {
        return _queues.get(0);
    }

    public ImmutableCollection<MoverRequestScheduler> getQueues() {
        return _queues;
    }

    public MoverRequestScheduler getQueue(String queueName) {
        return _queuesByName.get(queueName);
    }

    public MoverRequestScheduler getQueueByJobId(int id) {
        int pos = id >> 24;
        if (pos >= _queues.size()) {
            throw new IllegalArgumentException("Invalid id (doesn't belong to any known scheduler)");
        }
        return _queues.get(pos);
    }

    public int add(String queueName, Mover<?> transfer, IoPriority priority)
    {
        MoverRequestScheduler js = (queueName == null) ? null : _queuesByName.get(queueName);
        return (js == null) ? add(transfer, priority) : js.add(transfer, priority);
    }

    public int add(Mover<?> transfer, IoPriority priority)
    {
        return getDefaultQueue().add(transfer, priority);
    }

    public void cancel(int jobId) throws NoSuchElementException {
        getQueueByJobId(jobId).cancel(jobId);
    }

    public int getMaxActiveJobs() {
        int sum = 0;
        for (MoverRequestScheduler s : _queues) {
            sum += s.getMaxActiveJobs();
        }
        return sum;
    }

    public int getActiveJobs() {
        int sum = 0;
        for (MoverRequestScheduler s : _queues) {
            sum += s.getActiveJobs();
        }
        return sum;
    }

    public int getQueueSize() {
        int sum = 0;
        for (MoverRequestScheduler s : _queues) {
            sum += s.getQueueSize();
        }
        return sum;
    }

    public List<JobInfo> getJobInfos() {
        List<JobInfo> list = new ArrayList<>();
        for (MoverRequestScheduler s : _queues) {
            list.addAll(s.getJobInfos());
        }
        return list;
    }

    public void printSetup(PrintWriter pw) {
        for (MoverRequestScheduler s : _queues) {
            pw.println("mover set max active -queue=" + s.getName() + " " + s.getMaxActiveJobs());
        }
    }

    public JobInfo findJob(String client, long id) {
        for (JobInfo info : getJobInfos()) {
            if (client.equals(info.getClientName()) && id == info.getClientId()) {
                return info;
            }
        }
        return null;
    }

    public synchronized void shutdown() throws InterruptedException {
        for (MoverRequestScheduler queue : _queues) {
            queue.shutdown();
        }
    }
}
