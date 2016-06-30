package org.dcache.pool.classic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.IoJobInfo;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellSetupProvider;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.DelayedCommand;
import dmg.util.command.Option;

import org.dcache.pool.classic.MoverRequestScheduler.Order;
import org.dcache.util.IoPriority;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.joining;

public class IoQueueManager
        implements CellCommandListener, CellSetupProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IoQueueManager.class);

    /**
     * The name of the default queue.
     */
    public static final String DEFAULT_QUEUE = "regular";

    /**
     * The name of a queue used by pool-to-pool transfers.
     */
    public static final String P2P_QUEUE_NAME = "p2p";

    /**
     * Queues by queue id.
     */
    private final ConcurrentMap<Integer, MoverRequestScheduler> queuesById = new ConcurrentHashMap<>();

    /**
     * Queues by name.
     */
    private final ConcurrentMap<String, MoverRequestScheduler> queuesByName = new ConcurrentHashMap<>();

    /**
     * Generator for queue ids.
     */
    private final AtomicInteger counter = new AtomicInteger();

    /**
     * Default queue used when named queue does not exist.
     */
    private final MoverRequestScheduler defaultQueue;

    /**
     * Queue for pool to pool transfers.
     */
    private final MoverRequestScheduler p2pQueue;

    public IoQueueManager()
    {
        defaultQueue = createQueue(DEFAULT_QUEUE, Order.LIFO);
        p2pQueue = createQueue(P2P_QUEUE_NAME, Order.LIFO);
    }

    public void setQueues(String[] queues)
    {
        for (String queue : queues) {
            queue = queue.trim();
            if (queue.startsWith("-")) {
                createQueue(queue.substring(1), Order.FIFO);
            } else if (!queue.isEmpty()) {
                createQueue(queue, Order.LIFO);
            }
        }
    }

    public Collection<MoverRequestScheduler> queues()
    {
        return queuesById.values();
    }

    public MoverRequestScheduler getPoolToPoolQueue()
    {
        return p2pQueue;
    }

    @Nonnull
    private MoverRequestScheduler getQueueByNameOrDefault(String queueName)
    {
        return (queueName == null) ? defaultQueue : queuesByName.getOrDefault(queueName, defaultQueue);
    }

    @Nonnull
    public MoverRequestScheduler getQueueByJobId(int jobId) throws NoSuchElementException
    {
        MoverRequestScheduler queue = queuesById.get(jobId >> 24);
        if (queue == null) {
            throw new NoSuchElementException("Id doesn't belong to any known scheduler.");
        }
        return queue;
    }

    public int getOrCreateMover(String queueName, String doorUniqueId, MoverSupplier moverSupplier,
                                IoPriority priority) throws CacheException
    {
        return getQueueByNameOrDefault(queueName).getOrCreateMover(moverSupplier, doorUniqueId, priority);
    }

    public void printSetup(PrintWriter pw)
    {
        queues().forEach(q -> pw.println("mover queue create " + q.getName() + " -order=" + q.getOrder()));
        queues().forEach(q -> pw.println("mover set max active -queue=" + q.getName() + " " + q.getMaxActiveJobs()));
        queues().forEach(q -> pw.println("jtm set timeout -queue=" + q.getName() + " -lastAccess=" +
                                         (q.getLastAccessed() / 1000L) + " -total=" + (q.getTotal() / 1000L)));
    }

    public synchronized void shutdown() throws InterruptedException
    {
        for (MoverRequestScheduler queue : queuesById.values()) {
            queue.shutdown();
        }
        queuesById.clear();
        queuesByName.clear();
    }

    private synchronized MoverRequestScheduler createQueue(String name, Order order)
    {
        MoverRequestScheduler queue = queuesByName.get(name);
        if (queue != null) {
            queue.setOrder(order);
        } else {
            LOGGER.info("Creating queue: {}", name);

            int id = counter.getAndIncrement();
            queue = new MoverRequestScheduler(name, id, order);
            queuesById.put(id, queue);
            queuesByName.put(name, queue);
        }
        return queue;
    }

    private synchronized MoverRequestScheduler deleteQueue(String name)
    {
        checkArgument(!name.equals(DEFAULT_QUEUE), "Cannot delete the default queue.");
        checkArgument(!name.equals(P2P_QUEUE_NAME), "Cannot delete the pool to pool queue.");

        MoverRequestScheduler queue = queuesByName.remove(name);
        if (queue != null) {
            queuesById.remove(queue.getId(), queue);
            queuesByName.remove(queue.getName(), queue);
        }
        return queue;
    }

    private String moverSetMaxActive(MoverRequestScheduler js, int active)
            throws IllegalArgumentException
    {
        checkArgument(active >= 0, "<maxActiveMovers> must be >= 0");
        js.setMaxActiveJobs(active);

        return "Max Active Io Movers set to " + active;
    }

    private Serializable list(MoverRequestScheduler js, boolean binary)
    {
        return list(Collections.singleton(js), binary);
    }

    private Serializable list(Collection<MoverRequestScheduler> jobSchedulers, boolean isBinary)
    {
        if (isBinary) {
            return jobSchedulers.stream().flatMap(s -> s.getJobInfos().stream()).toArray(IoJobInfo[]::new);
        } else {
            StringBuffer sb = new StringBuffer();
            for (MoverRequestScheduler js : jobSchedulers) {
                js.printJobQueue(sb);
            }
            return sb.toString();
        }
    }

    @AffectsSetup
    @Command(name = "mover set max active",
            hint = "set the maximum number of active client transfers",
            description = "Set the maximum number of allowed concurrent transfers. " +
                          "If any further requests are send after the set maximum value is " +
                          "reach, these requests will be queued. A classic usage will be " +
                          "to set the maximum number of client concurrent transfer request " +
                          "allowed.\n\n" +
                          "Note that, this set maximum value will also be used by the cost " +
                          "module for calculating the performance cost.")
    public class MoverSetMaxActiveCommand implements Callable<String>
    {
        @Argument(metaVar = "maxActiveMovers",
                usage = "Specify the maximum number of active client transfers.")
        int maxActiveIoMovers;

        @Option(name = "queue", metaVar = "queueName",
                usage = "Specify the mover queue name to operate on. If unspecified, " +
                        "the default mover queue is assumed.")
        String queueName;

        @Override
        public String call() throws IllegalArgumentException
        {
            if (queueName == null) {
                return moverSetMaxActive(defaultQueue, maxActiveIoMovers);
            }

            MoverRequestScheduler js = queuesByName.get(queueName);
            if (js == null) {
                return "Not found : " + queueName;
            }

            return moverSetMaxActive(js, maxActiveIoMovers);
        }
    }

    @AffectsSetup
    @Command(name = "p2p set max active",
            hint = "set maximum number of active pool-to-pool transfers",
            description = "Set the maximum number of concurrent active pool-to-pool " +
                          "source transfers allowed. Any further requests will be queued. " +
                          "This value will also be used by the cost module for calculating " +
                          "the performance cost.")
    public class P2pSetMaxActiveCommand implements Callable<String>
    {
        @Argument(usage = "The maximum number of active pool-to-pool source transfers.")
        int maxActiveP2PTransfers;

        @Override
        public String call() throws IllegalArgumentException
        {
            return moverSetMaxActive(p2pQueue, maxActiveP2PTransfers);
        }
    }

    @AffectsSetup
    @Command(name = "mover queue create", hint = "create mover queue",
            description= "Creates a new mover queue. If the queue already exists, the command changes " +
                         "the queue order if it differs from the current value.\n\n" +
                         "Doors have to be explicitly configured to submit to a particular queue. The " +
                         "queue called 'regular' is the default queue. The queue called 'p2p' is used for " +
                         "the source movers of pool to pool transfers.")
    public class MoverCreateQueueCommand extends DelayedCommand<String>
    {
        @Argument(usage = "Name of the queue to create.")
        String name;

        @Option(name = "order",
                usage = "Ordering of the queue. Although last in first out is " +
                        "unfair, it tends to be more robust in overload situations.")
        Order order = Order.LIFO;

        @Override
        public String execute() throws InterruptedException
        {
            createQueue(name, order);
            return "";
        }
    }

    @AffectsSetup
    @Command(name = "mover queue delete", hint = "delete mover queue",
            description = "Deletes a mover queue. The 'regular' and 'p2p' queues cannot be deleted.")
    public class MoverDeleteQueueCommand extends DelayedCommand<String>
    {
        @Argument(index = 0)
        String name;

        @Override
        public String execute() throws InterruptedException
        {
            MoverRequestScheduler oldQueue = deleteQueue(name);
            if (oldQueue != null) {
                oldQueue.shutdown();
            }
            return "";
        }
    }

    @Command(name = "mover queue ls",
            hint = "list all mover queues in this pool",
            description = "List information about the mover queues in this pool. " +
                          "Only the names of the mover queues are listed if the option '-l' " +
                          "is not specified.")
    public class MoverQueueLsCommand implements Callable<Serializable>
    {
        @Option(name = "l",
                usage = "Get additional information on the mover queues. " +
                        "The returned information comprises of: the name of the " +
                        "mover queue, number of active transfer, maximum number " +
                        "of allowed transfer and the length of the queued transfer.")
        boolean verbose;

        @Override
        public Serializable call()
        {
            Function<MoverRequestScheduler, String> f;
            if (verbose) {
                f = q -> q.getName() + " " + q.getActiveJobs() + " " + q.getMaxActiveJobs() + " " + q.getQueueSize() + " " + q.getOrder();
            } else {
                f = MoverRequestScheduler::getName;
            }
            return queues().stream().map(f).collect(joining("\n"));
        }
    }

    @AffectsSetup
    @Command(name = "jtm set timeout",
            hint = "set transfer inactivity limits",
            description = "Set the transfer timeout for a specified queue or all queues in " +
                          "this pool. The timeout is a time limit after which a job is considered " +
                          "expired and it will be terminated by the job timeout manager. There are " +
                          "two timeout values needed to be set:\n" +
                          "\t1. The last access timeout is the time durations after which a job is " +
                          "deemed expired based on the time since the last block was transferred.\n" +
                          "\t2. The total timeout is the time duration after which a job is deemed " +
                          "expired based on the start time of the job.\n\n" +
                          "One of these two or both timeout duration must be surpassed before a job " +
                          "is terminated.")
    public class JtmSetTimeoutCommand implements Callable<String>
    {
        @Option(name = "queue", valueSpec = "NAME",
                usage = "Specify the queue name. If no queue is specified, " +
                        "the setting will be applied to all queues.")
        String name;

        @Option(name = "lastAccess", valueSpec = "TIMEOUT",
                usage = "Set the lassAccessed timeout limit in seconds.")
        long lastAccessed;

        @Option(name = "total", valueSpec = "TIMEOUT",
                usage = "Set the total timeout limit in seconds.")
        long total;

        @Override
        public synchronized String call() throws IllegalArgumentException, NoSuchElementException
        {
            if (name == null) {
                for (MoverRequestScheduler queue : queues()) {
                    queue.setLastAccessed(lastAccessed * 1000L);
                    queue.setTotal(total * 1000L);
                }
            } else {
                MoverRequestScheduler queue = queuesByName.get(this.name);
                if (queue == null) {
                    throw new NoSuchElementException("No such queue: " + name);
                }
                queue.setLastAccessed(lastAccessed * 1000L);
                queue.setTotal(total * 1000L);
            }
            return "";
        }
    }

    @Command(name = "mover ls", hint = "list movers",
            description = "List movers on this pool.")
    public class MoverLsCommand implements Callable<Serializable>
    {
        @Argument(required = false, usage = "Limit output to mover with this job id.")
        Integer id;

        @Option(name = "queue", metaVar = "name", usage = "Limit output to this queue.")
        String queueName;

        @Option(name = "binary", usage = "Use binary output format.")
        boolean isBinary;

        @Override
        public Serializable call() throws NoSuchElementException
        {
            if (id != null) {
                return getQueueByJobId(id).getJobInfo(id);
            }

            if (queueName == null) {
                return list(queuesById.values(), isBinary);
            }

            if (queueName.isEmpty()) {
                StringBuilder sb = new StringBuilder();
                for (MoverRequestScheduler js : queues()) {
                    sb.append("[").append(js.getName()).append("]\n");
                    sb.append(list(js, isBinary).toString());
                }
                return sb.toString();
            }

            MoverRequestScheduler js = queuesByName.get(queueName);
            if (js == null) {
                throw new NoSuchElementException("Not found : " + queueName);
            }

            return list(js, isBinary);
        }
    }

    @Command(name = "p2p ls", hint = "list pool to pool source movers",
            description = "List movers that serve files for pool to pool transfers.")
    public class PoolToPoolListCommand implements Callable<Serializable>
    {
        @Argument(required = false, usage = "Limit output to mover with this job id.")
        Integer id;

        @Option(name = "binary", usage = "Use binary output format.")
        boolean isBinary;

        @Override
        public Serializable call() throws NoSuchElementException
        {
            if (id != null) {
                return p2pQueue.getJobInfo(id);
            }
            return list(p2pQueue, isBinary);
        }
    }

    @Command(name = "mover kill",
            hint = "terminate a file transfer connection",
            description = "Interrupt a specified file transfer in progress by " +
                          "terminating the request. This is particularly useful when " +
                          "the transfer request is stuck and blocking other requests.")
    public class MoverKillCommand implements Callable<String>
    {
        @Argument(metaVar = "jobId",
                usage = "Specify the job number of the transfer request to kill.")
        int id;

        @Override
        public String call() throws NoSuchElementException, IllegalArgumentException
        {
            MoverRequestScheduler js = getQueueByJobId(id);
            LOGGER.info("Killing mover {}", id);
            js.cancel(id);
            return "Kill initialized.";
        }
    }
}
