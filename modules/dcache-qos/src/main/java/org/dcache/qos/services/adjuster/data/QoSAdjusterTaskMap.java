/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.qos.services.adjuster.data;

import com.google.common.annotations.VisibleForTesting;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellInfoProvider;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import org.dcache.pool.migration.PoolMigrationCopyFinishedMessage;
import org.dcache.qos.services.adjuster.adjusters.QoSAdjusterFactory;
import org.dcache.qos.services.adjuster.handlers.QoSAdjusterTaskHandler;
import org.dcache.qos.services.adjuster.util.QoSAdjusterCounters;
import org.dcache.qos.services.adjuster.util.QoSAdjusterTask;
import org.dcache.qos.util.QoSHistory;
import org.dcache.qos.vehicles.QoSAdjustmentRequest;
import org.dcache.util.RunnableModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Manages adjuster tasks that are submitted from the verification service.
 */
public final class QoSAdjusterTaskMap extends RunnableModule implements CellInfoProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(QoSAdjusterTaskMap.class);
    private static final String MISSING_ENTRY = "Entry for {} was removed from map before "
        + "completion of outstanding task.";

    private final ReadWriteLock lock   = new ReentrantReadWriteLock(true);
    private final Lock          write  = lock.writeLock();
    private final Lock          read   = lock.readLock();

    private final Map<String, QoSAdjusterTask> index = new ConcurrentHashMap<>();
    private final Deque<QoSAdjusterTask> runningQueue = new LinkedBlockingDeque<>();
    private final Deque<QoSAdjusterTask> readyQueue = new LinkedBlockingDeque<>();
    private final Deque<QoSAdjusterTask> waitingQueue = new LinkedBlockingDeque<>();
    private final AtomicLong signalled = new AtomicLong(0L);

    private QoSAdjusterFactory factory;
    private QoSAdjusterCounters counters;
    private QoSHistory history;

    private ScheduledExecutorService executorService;

    /*
     *  A callback.  Note that this creates a cyclical dependency in the spring context.
     *  The rationale here is that the map controls the terminal logic for adjustment
     *  tasks on a single thread, and thus needs to notify other components (such
     *  as the verifier) of termination.  It makes sense that only the
     *  handler would communicate with the "outside", and that the map should
     *  be internal to this service.
     */
    private QoSAdjusterTaskHandler taskHandler;

    /*
     *  Note that this throttle is necessary.  Staging adjustments and migration tasks
     *  relinquish the thread, so we cannot rely on the thread pool to put a barrier
     *  on the number of concurrent (or "waiting") jobs.
     */
    private int maxRunning = 200;

    /**
     *   Meaning for a given source-target pair (i.e., this task).  The verifier
     *   will retry new pairings if possible and resubmit the task request.
     */
    private int maxRetries = 1;

    public void cancel(PnfsId pnfsId) {
        write.lock();
        try {
            QoSAdjusterTask task = index.get(pnfsId.toString());
            if (task != null) {
                LOGGER.debug("TaskMap cancel {}", pnfsId);
                task.cancel("Cancelled by admin/user.");
            }
        } finally {
            write.unlock();
        }

        signalAll();
    }

    /**
     *  Used by admin command.
     */
    public void cancel(Predicate<QoSAdjusterTask> filter) {
        write.lock();
        try {
            index.values().stream().filter(filter).forEach(task -> {
                if (task != null) {
                    task.cancel("Cancelled by admin/user.");
                }
            });
        } finally {
            write.unlock();
        }

        signalAll();
    }

    /**
     *  Used by admin command.
     */
    public long count(Predicate<QoSAdjusterTask> filter) {
        read.lock();
        try {
            return index.values().stream().filter(filter).count();
        } finally {
            read.unlock();
        }
    }

    @Override
    public void getInfo(PrintWriter pw) {
        StringBuilder builder = new StringBuilder();
        getInfo(builder);
        pw.println(builder.toString());
    }

    /**
     *  Used by admin command.
     */
    public void getInfo(StringBuilder builder) {
        counters.appendRunning(builder);
        counters.appendSweep(builder);
        builder.append("maximum running tasks:     ").append(maxRunning).append("\n");
        builder.append("maximum number of retries: ").append(maxRetries).append("\n\n");
        counters.appendCounts(builder);
    }

    public void initialize() {
        super.initialize();
        LOGGER.info("Adjuster task map initialized.");
    }

    /*
     *  Testing only.
     */
    @VisibleForTesting
    public List<QoSAdjusterTask> getTasks(Predicate<QoSAdjusterTask> filter, int limit) {
        read.lock();
        try {
            return index.values().stream()
                                 .filter(filter)
                                 .limit(limit)
                                 .collect(Collectors.toList());
        } finally {
            read.unlock();
        }
    }

    /**
     *  Used by admin command.
     */
    public String list(Predicate<QoSAdjusterTask> filter, int limit) {
        StringBuilder builder = new StringBuilder();
        AtomicInteger total = new AtomicInteger(0);

        read.lock();
        try {
            index.values().stream()
                .filter(filter)
                .limit(limit)
                .forEach(t -> {
                    builder.append(t).append("\n");
                    total.incrementAndGet();
                });
        } finally {
            read.unlock();
        }

        if (total.get() == 0) {
            builder.append("NO (MATCHING) TASKS.\n");
        } else {
            builder.append("TOTAL TASKS:\t\t").append(total.get()).append("\n");
        }
        return builder.toString();
    }

    public void register(QoSAdjustmentRequest request) {
        QoSAdjusterTask task = new QoSAdjusterTask(request, factory);
        LOGGER.debug("register task {}: {}", request.getPnfsId(), task);
        write.lock();
        try {
            if (!add(task.getPnfsId(), task)) {
                LOGGER.error("received request for {} but a task for this file is already registered.",
                                task.getPnfsId());
            }
        } finally {
            write.unlock();
        }

        signalAll();
    }

    /**
     *  The consumer thread. When notified or times out, polls any waiting tasks and
     *  then runs scan.
     *  <p/>
     *  Note that since the scan takes place outside of the monitor, the
     *  signals sent by various update methods will not be caught before
     *  the current thread is inside {@link #await}; for this reason, a
     *  counter is used and reset to 0 before each scan. No wait occurs if
     *  the counter is non-zero after the scan.
     */
    public void run() {
        try {
            while (!Thread.interrupted()) {
                LOGGER.trace("Calling scan.");

                signalled.set(0);
                long start = System.currentTimeMillis();

                pollWaiting();

                scan();

                long end = System.currentTimeMillis();
                counters.recordSweep(end, end-start);

                if (signalled.get() > 0) {
                    /*
                     *  Give the operations completed during the scan a chance
                     *  to free slots immediately, if possible, by rescanning now.
                     */
                    LOGGER.trace("Scan complete, received {} signals; "
                                    + "rechecking for requeued operations ...",
                                    signalled.get());
                    continue;
                }

                LOGGER.trace("Scan complete, waiting ...");
                await();
            }
        } catch (InterruptedException e) {
            LOGGER.trace("Consumer was interrupted.");
        }

        LOGGER.trace("Exiting file operation consumer.");
        clear();

        LOGGER.trace("File operation queues and index cleared.");
    }

    /**
     *  Iterates over the queues to check the state of running tasks and to submit
     *  waiting tasks if there are open slots.  Removes completed tasks.
     */
    public void scan() {
        write.lock();
        try {
            final List<QoSAdjusterTask> toRemove = new ArrayList<>();
            /*
             *  Check all queues in case of cancellation.
             *  Also move tasks off running queue to waiting queue
             *  that have gone to the WAITING state.
             */
            runningQueue.forEach(t -> {
                if (t.isDone()) {
                    toRemove.add(t);
                    handleTerminatedTask(t);
                } else if (t.isWaiting()) {
                    toRemove.add(t);
                    waitingQueue.add(t);
                }
            });

            toRemove.forEach(runningQueue::remove);
            toRemove.clear();

            waitingQueue.stream().filter(QoSAdjusterTask::isDone)
                .forEach(t -> {
                    toRemove.add(t);
                    handleTerminatedTask(t);
                });

            toRemove.forEach(waitingQueue::remove);
            toRemove.clear();

            readyQueue.stream().filter(QoSAdjusterTask::isDone)
                .forEach(t -> {
                    toRemove.add(t);
                    handleTerminatedTask(t);
                });

            toRemove.forEach(readyQueue::remove);
            toRemove.clear();

            int available = maxRunning - runningQueue.size();

            QoSAdjusterTask task;

            while ((task = readyQueue.poll()) != null && available > 0) {
                runningQueue.add(task);
                submit(task);
                --available;
            }
        } finally {
            write.unlock();
        }
    }

    public void setCounters(QoSAdjusterCounters counters) {
        this.counters = counters;
    }

    public void setFactory(QoSAdjusterFactory factory) {
        this.factory = factory;
    }

    public void setExecutorService(ScheduledExecutorService executorService) {
        this.executorService = executorService;
    }

    public void setHistory(QoSHistory history) {
        this.history = history;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public void setMaxRunning(int maxRunning) {
        this.maxRunning = maxRunning;
    }

    public void setHandler(QoSAdjusterTaskHandler taskHandler) {
        this.taskHandler = taskHandler;
    }

    @Override
    public void shutdown() {
        LOGGER.info("Adjuster task map shutting down ...");
        super.shutdown();
    }

    /**
     *  <p>Also used by admin command.</p>
     */
    public synchronized void signalAll() {
        signalled.incrementAndGet();
        notifyAll();
    }

    public int size() {
        read.lock();
        try {
            return index.size();
        } finally {
            read.unlock();
        }
    }

    /**
     *  Migration task termination.
     *  <p/>
     *  This call will bottom out in the completion handler finally calling
     *  #updateTask(PnfsId, CacheException).  It is there that the callback
     *  message is triggered.
     */
    public void updateTask(PoolMigrationCopyFinishedMessage message) {
        PnfsId pnfsId = message.getPnfsId();
        read.lock();
        try {
            QoSAdjusterTask task = index.get(pnfsId.toString());
            if (task == null) {
                /*
                 *  Treat the missing entry benignly,
                 *  as it is possible to have a race between removal
                 *  from forced cancellation and the arrival of the task
                 *  callback.
                 */
                LOGGER.warn(MISSING_ENTRY, pnfsId);
            } else {
                task.relayMessage(message);
            }
        } finally {
            read.unlock();
        }
    }

    /**
     *  Calls task terminated.
     *
     *  @param pnfsId of the file undergoing adjustment.
     *  @param target an optional target update (this is used only by staging, where the pool
     *               manager selects the pool via the pin manager).
     *  @param exception
     */
    public void updateTask(PnfsId pnfsId, Optional<String> target, CacheException exception) {
        QoSAdjusterTask task;

        read.lock();
        try {
            task = index.get(pnfsId.toString());
            if (task == null) {
                /*
                 *  Treat the missing entry benignly,
                 *  as it is possible to have a race between removal
                 *  from forced cancellation and the arrival of the task
                 *  callback.
                 */
                LOGGER.warn(MISSING_ENTRY, pnfsId);
            } else {
                task.taskTerminated(target, exception);
            }
        } finally {
            read.unlock();
        }

        signalAll();
    }

    @GuardedBy("write")
    private boolean add(PnfsId pnfsId, QoSAdjusterTask task) {
        String key = pnfsId.toString();
        if (index.containsKey(key)) {
            return false;
        }
        index.put(key, task);
        readyQueue.add(task);
        return true;
    }

    private synchronized void await() throws InterruptedException {
        wait(timeoutUnit.toMillis(timeout));
    }

    private synchronized void clear() {
        runningQueue.clear();
        readyQueue.clear();
        index.clear();
    }

    private void handleTerminatedTask(QoSAdjusterTask task) {
        int retry = task.getRetry();
        if (task.getException() != null && retry < maxRetries) {
            LOGGER.debug("TaskMap handleTerminatedTask, readding {}", task.getPnfsId());
            task = new QoSAdjusterTask(task, retry + 1);
            index.put(task.getPnfsId().toString(), task);
            readyQueue.add(task);
        } else {
            LOGGER.debug("TaskMap handleTerminatedTask, removing {}", task.getPnfsId());
            index.remove(task.getPnfsId().toString());
            history.add(task.getPnfsId(), task.toHistoryString(), task.getException() != null);
            counters.recordTask(task);
            taskHandler.notifyAdjustmentCompleted(task);
        }
    }

    private void pollWaiting() {
        read.lock();
        try {
            waitingQueue.stream().forEach(QoSAdjusterTask::poll);
        } finally {
            read.unlock();
        }
    }

    private void submit(QoSAdjusterTask task) {
        task.setFuture(executorService.submit(task.toFireAndForgetTask()));
    }
}