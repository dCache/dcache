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
import java.util.Collection;
import java.util.stream.Collectors;

import diskCacheV111.vehicles.PoolIoFileMessage;
import org.dcache.util.IoPriority;

import static java.util.Arrays.asList;
import diskCacheV111.util.CacheException;
import dmg.cells.nucleus.CellMessage;
import static com.google.common.collect.Iterables.concat;

public class IoQueueManager {

    private static final Logger _log = LoggerFactory.getLogger(IoQueueManager.class);

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

    private MoverRequestScheduler getQueueByNameOrDefault(String queueName) {
        if (queueName == null) {
            return getDefaultQueue();
        }
        MoverRequestScheduler queue = getQueue(queueName);
        return queue == null ? getDefaultQueue() : queue;
    }

    public MoverRequestScheduler getQueueByJobId(int id) {
        int pos = id >> 24;
        if (pos >= _queues.size()) {
            throw new IllegalArgumentException("Invalid id (doesn't belong to any known scheduler)");
        }
        return _queues.get(pos);
    }

    public int getOrCreateMover(String queueName, String doorUniqueId, MoverSupplier moverSupplier, IoPriority priority) throws CacheException
    {
        return getQueueByNameOrDefault(queueName).getOrCreateMover(moverSupplier, doorUniqueId, priority);
    }

    public void cancel(int jobId) throws NoSuchElementException {
        getQueueByJobId(jobId).cancel(jobId);
    }

    public int getMaxActiveJobs() {
        return _queues.stream()
                .mapToInt(MoverRequestScheduler::getMaxActiveJobs).sum();
    }

    public int getActiveJobs() {
        return _queues.stream()
                .mapToInt(MoverRequestScheduler::getActiveJobs).sum();
    }

    public int getQueueSize() {
        return _queues.stream()
                .mapToInt(MoverRequestScheduler::getQueueSize).sum();
    }

    public List<JobInfo> getJobInfos() {

        return _queues.stream()
                .map(MoverRequestScheduler::getJobInfos)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public void printSetup(PrintWriter pw) {
        _queues.forEach(s -> {
            pw.println("mover set max active -queue=" + s.getName() + " " + s.getMaxActiveJobs());
        });
    }

    public JobInfo findJob(String client, long id) {
        return _queues.stream()
                .map(MoverRequestScheduler::getJobInfos)
                .flatMap(Collection::stream)
                .filter(i -> client.equals(i.getClientName()))
                .filter(i -> id == i.getClientId())
                .findFirst().orElse(null);
    }

    public synchronized void shutdown() throws InterruptedException {
        for (MoverRequestScheduler queue : _queues) {
            queue.shutdown();
        }
    }
}
