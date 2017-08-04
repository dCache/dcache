package org.dcache.pool.classic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.IoJobInfo;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellSetupProvider;
import dmg.util.CommandException;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.DelayedCommand;
import dmg.util.command.Option;

import org.dcache.pool.FaultEvent;
import org.dcache.pool.FaultListener;
import org.dcache.pool.classic.MoverRequestScheduler.Order;
import org.dcache.util.IoPriority;

import static com.google.common.base.Preconditions.checkArgument;
import static dmg.util.CommandException.checkCommand;
import static java.util.stream.Collectors.joining;

public class IoQueueManager
        implements FaultListener, CellCommandListener, CellSetupProvider
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
     * Listeners notified when any queue generates a fatal fault.
     */
    private final List<FaultListener> faultListeners =
            new CopyOnWriteArrayList<>();

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

    public void addFaultListener(FaultListener listener)
    {
        faultListeners.add(listener);
    }

    public void removeFaultListener(FaultListener listener)
    {
        faultListeners.remove(listener);
    }

    @Override
    public void faultOccurred(FaultEvent event)
    {
        faultListeners.forEach(l -> l.faultOccurred(event));
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
    public MoverRequestScheduler getQueueByNameOrDefault(String queueName)
    {
        return (queueName == null) ? defaultQueue : queuesByName.getOrDefault(queueName, defaultQueue);
    }

    @Nonnull
    public Optional<MoverRequestScheduler> getQueueByJobId(int jobId)
    {
        return Optional.ofNullable(queuesById.get(jobId >> 24));
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
            queue.addFaultListener(this);
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
            throws CommandException
    {
        checkCommand(active >= 0, "<maxActiveMovers> must be >= 0");
        js.setMaxActiveJobs(active);

        return "Max Active Io Movers set to " + active;
    }

    private static void toMoverString(MoverRequestScheduler.PrioritizedRequest j, StringBuilder sb) {
        sb.append(j.getId()).append(" : ").append(j).append('\n');
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
        public String call() throws CommandException
        {
            if (queueName == null) {
                return moverSetMaxActive(defaultQueue, maxActiveIoMovers);
            }

            MoverRequestScheduler js = queuesByName.get(queueName);
            checkCommand(js != null, "Not found : %s", queueName);

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
        public String call() throws CommandException
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
        public synchronized String call() throws CommandException
        {
            checkCommand(lastAccessed >= 0L, "invalid lastAccess value %d: must be >= 0", lastAccessed);
            checkCommand(total >= 0L, "invalid total value %d: must be >= 0", total);

            if (name == null) {
                for (MoverRequestScheduler queue : queues()) {
                    queue.setLastAccessed(lastAccessed * 1000L);
                    queue.setTotal(total * 1000L);
                }
            } else {
                MoverRequestScheduler queue = queuesByName.get(name);
                checkCommand(queue != null, "No such queue: %s", name);
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

        @Option(name = "t", usage = "Sort output by last access time.")
        boolean sortByTime;

        @Option(name = "S", usage = "Sort output by transfer size.")
        boolean sortBySize;

        @Option(name = "r", usage = "Sort output in reverse order.")
        boolean reverseSort;

        @Override
        public Serializable call() throws CommandException
        {
            if (id != null) {
                return getQueueByJobId(id).orElseThrow(() -> new CommandException("Id doesn't belong to any known scheduler."))
                        .getJobInfo(id).orElseThrow(() -> new CommandException("Job not found : Job-" + id));
            }

            boolean groupByQueue;
            Collection<MoverRequestScheduler> queues;
            if (queueName != null && !queueName.isEmpty()) {
                MoverRequestScheduler js = queuesByName.get(queueName);
                checkCommand(js != null, "Not found : %s", queueName);
                queues = Collections.singleton(js);
                groupByQueue = false;
            } else {
                groupByQueue = queueName != null && queueName.isEmpty();
                queues = queuesById.values();
            }

            if (isBinary) {
                // ignore sortin and grouping by queue name if binnary
                return queues.stream().flatMap(s -> s.getJobInfos().stream()).toArray(IoJobInfo[]::new);
            } else {

                Comparator<MoverRequestScheduler.PrioritizedRequest> comparator;
                if (sortBySize) {
                    comparator = (b, a) -> Long.compare(
                            a.getMover().getBytesTransferred(), b.getMover().getBytesTransferred()
                    );
                } else if (sortByTime) {
                    comparator = (b, a) -> Long.compare(
                            a.getMover().getLastTransferred(), b.getMover().getLastTransferred()
                    );
                } else {
                    comparator = (b, a) -> Integer.compare(
                            a.getId(), b.getId()
                    );
                }

                if (reverseSort) {
                    comparator = comparator.reversed();
                }

                StringBuilder sb = new StringBuilder();
                if (groupByQueue) {
                    queues.stream().forEach(q -> {
                        sb.append("[").append(q.getName()).append("]\n");
                        q.getJobs()
                                .sorted()
                                .forEach(j -> IoQueueManager.toMoverString(j, sb));
                    });
                } else {
                    queues.stream().flatMap(s -> s.getJobs())
                            .sorted(comparator)
                            .forEach(j -> IoQueueManager.toMoverString(j, sb));
                }
                return sb.toString();
            }
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
        public Serializable call() throws CommandException
        {
            if (id != null) {
                return p2pQueue.getJobInfo(id).orElseThrow(() -> new CommandException("Job not found : Job-" + id));
            }
            if (isBinary) {
                return p2pQueue.getJobs().toArray(IoJobInfo[]::new);
            } else {
                StringBuilder sb = new StringBuilder();
                p2pQueue.getJobs()
                        .forEach(j -> IoQueueManager.toMoverString(j, sb));
                return sb.toString();
            }
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
        public String call() throws CommandException
        {
            MoverRequestScheduler js = getQueueByJobId(id).orElseThrow(() -> new CommandException("Id doesn't belong to any known scheduler."));
            LOGGER.info("Killing mover {}", id);
            if (!js.cancel(id, "killed through admin interface")) {
                throw new CommandException("Unknown id: " + id);
            }
            return "Kill initialized.";
        }
    }
}
