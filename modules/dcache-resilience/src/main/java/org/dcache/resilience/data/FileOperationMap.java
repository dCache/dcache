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
package org.dcache.resilience.data;

import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.ws.rs.HEAD;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import org.dcache.pool.migration.PoolMigrationCopyFinishedMessage;
import org.dcache.resilience.handlers.FileOperationHandler;
import org.dcache.resilience.handlers.FileTaskCompletionHandler;
import org.dcache.resilience.handlers.PoolTaskCompletionHandler;
import org.dcache.resilience.util.CacheExceptionUtils;
import org.dcache.resilience.util.CacheExceptionUtils.FailureType;
import org.dcache.resilience.util.CheckpointUtils;
import org.dcache.resilience.util.Operation;
import org.dcache.resilience.util.OperationHistory;
import org.dcache.resilience.util.OperationStatistics;
import org.dcache.resilience.util.PoolSelectionUnitDecorator.SelectionAction;
import org.dcache.resilience.util.ResilientFileTask;
import org.dcache.util.RunnableModule;
import org.dcache.vehicles.FileAttributes;

/**
 * <p>The main locus of operations for resilience.</p>
 *
 * <p>Tracks all operations on individual files via an instance of
 *      {@link FileOperation}, whose lifecycle is initiated by the the arrival
 *      of a message or scan update, and which terminates when all work on
 *      the associated pnfsid has completed or has been aborted/cancelled.</p>
 *
 * <p>When more than one event references a pnfsid which has a current
 *      entry in the map, the entry's operation count is simply incremented.
 *      This indicates that when the current task finishes, another pass
 *      should be made.  For each pass, file attributes, and hence location
 *      information, is refreshed directly against the namespace. In the case
 *      where there are > 1 additional passes to be made, but the current
 *      state of the pnfsid locations satisfies the requirements, the
 *      operation is aborted (count is set to 0, to enable removal from the
 *      map).</p>
 *
 * <p>The runnable logic entails scanning the queues in order to post-process
 *      terminated tasks, and to launch new tasks for eligible operations if
 *      there are slots available.</p>
 *
 * <p>Fairness is defined here as the availability of the first copy.
 *      This means that operations are processed FIFO, but those requiring more
 *      than one copy or remove task are requeued after each task has completed
 *      successfully.</p>
 *
 * <p>The map distinguishes between foreground (new files) and background
 *      (files on a pool being scanned) operations. The number of foreground
 *      vs background operations allowed to run at each pass of the scanner
 *      is balanced in proportion to the number of waiting operations on each
 *      queue.</p>
 *
 * <p>A periodic checkpointer, if on, writes out selected data from each
 *      operation entry.  In the case of crash and restart of this domain,
 *      the checkpoint file is reloaded into memory.</p>
 *
 * <p>Access to the index map is not synchronized, because
 *      it is implemented using a ConcurrentHashMap.  This is the most
 *      efficient solution for allowing multiple inserts and reads to take
 *      place concurrently with any consumer thread removes.  All
 *      updating of operation state or settings in fact is done through
 *      an index read, since the necessary synchronization of those
 *      values is handled inside the operation object.  Only the initial
 *      queueing and cancellation requests require additional synchronization.</p>
 *
 * <p>However, since index reads are not blocked, the list and count methods,
 *      which filter against the index (and not the queues), along with
 *      the checkpointer, which attempts to persist live operations, will return
 *      or write out a dirty snapshot which is only an approximation of state,
 *      so as not to block consumer processing.</p>
 *
 * <p>Class is not marked final for stubbing/mocking purposes.</p>
 */
public class FileOperationMap extends RunnableModule {
    private static final String MISSING_ENTRY =
                    "Entry for %s + was removed from map before completion of "
                                    + "outstanding operation.";

    private static final String COUNTS_FORMAT = "    %-24s %15s\n";

    final class Checkpointer implements Runnable {
        long     last;
        long     expiry;
        TimeUnit expiryUnit;
        String   path;
        Thread   thread;

        volatile boolean running        = false;
        volatile boolean resetInterrupt = false;
        volatile boolean runInterrupt   = false;

        public void run() {
            running = true;

            while (running) {
                try {
                    synchronized (this) {
                        wait(expiryUnit.toMillis(expiry));
                    }
                } catch (InterruptedException e) {
                    if (resetInterrupt) {
                        LOGGER.trace("Checkpoint reset: expiry {} {}.",
                                     expiry, expiryUnit);
                        resetInterrupt = false;
                        continue;
                    }

                    if (!runInterrupt) {
                        LOGGER.info("Checkpointer wait was interrupted; exiting.");
                        break;
                    }
                    runInterrupt = false;
                }

                if (running) {
                    save();
                }
            }
        }

        /**
         * Writes out data from the operation map to the checkpoint file.
         */
        @VisibleForTesting
        void save() {
            long start = System.currentTimeMillis();
            long count = CheckpointUtils.save(path, poolInfoMap,
                                              index.values().iterator());
            last = System.currentTimeMillis();
            counters.recordCheckpoint(last, last - start, count);
        }
    }

    /**
     * <p>Handles canceled operations.</p>
     *
     * <p>Searches the running queue to see which operations have completed.
     *      Merges these with any cancelled operations.  It then appends
     *      the incoming operations to the foreground/background queues.</p>
     *
     * <p>Post-processing determines whether the operation can be permanently
     *      removed or needs to be requeued.</p>
     */
    class TerminalOperationProcessor {
        private Collection<FileOperation> toProcess = new ArrayList<>();

        void processTerminated() {
            appendIncoming();
            gatherTerminated();
            gatherCanceled();

            LOGGER.trace("Found {} terminated operations.", toProcess.size());

            /*
             *  Only operations whose counts are > 0 will
             *  remain in the map after this call.  If the non-zero count
             *  is subsequent to a failed operation, the operation
             *  goes to the head of the queue; else it is appended.
             */
            toProcess.stream().forEach(this::postProcess);
            toProcess.clear();
        }

        private void appendIncoming() {
            synchronized (incoming) {
                while (true) {
                    try {
                        FileOperation operation = incoming.remove();
                        if (operation.isBackground()) {
                            background.addLast(operation);
                        } else {
                            foreground.addLast(operation);
                        }
                    } catch (NoSuchElementException e) {
                        break;
                    }
                }
            }
        }

        /**
         *  <p>This is a potentially expensive operation (O[n] in the
         *     queue size), but should be called relatively infrequently.</p>
         */
        private void cancel(Queue<FileOperation> queue,
                            Collection<FileMatcher> filters,
                            Collection<FileOperation> toProcess) {
            for (Iterator<FileOperation> i = queue.iterator(); i.hasNext();) {
                FileOperation operation = i.next();
                for (FileMatcher filter : filters) {
                    if (filter.matches(operation, poolInfoMap)
                            && cancel(operation, filter.isForceRemoval())) {
                        i.remove();
                        toProcess.add(operation);
                        break;
                    }
                }
            }
        }

        private boolean cancel(FileOperation operation, boolean remove) {
            if (operation.cancelCurrent()) {
                if (remove) {
                    operation.setOpCount(0); // force removal
                }
                return true;
            }
            return false;
        }

        private void gatherCanceled() {
            Collection<FileMatcher> filters = new ArrayList<>();

            synchronized (cancelFilters) {
                filters.addAll(cancelFilters);
                cancelFilters.clear();
            }

            cancel(running, filters, toProcess);
            cancel(foreground, filters, toProcess);
            cancel(background, filters, toProcess);
        }

        private void gatherTerminated() {
            for (Iterator<FileOperation> i = running.iterator(); i.hasNext(); ) {
                FileOperation operation = i.next();
                if (operation.getState() == FileOperation.RUNNING) {
                    continue;
                }
                i.remove();
                toProcess.add(operation);
            }
        }

        /**
         * <p> Exceptions are analyzed to determine if any more work can be done.
         *      In the case of fatal errors, an alarm is sent.  Operations with
         *      counts > 0 are reset to waiting; otherwise, the operation record
         *      will be removed when this method returns.</p>
         */
        private void postProcess(FileOperation operation) {
            operation.setLastType();
            String pool = operation.getPrincipalPool(poolInfoMap);
            Integer sindex = operation.getSource();
            Integer tindex = operation.getTarget();
            String source = sindex == null ? null : poolInfoMap.getPool(sindex);
            String target = tindex == null ? null : poolInfoMap.getPool(tindex);

            boolean retry = false;
            boolean abort = false;

            if (operation.getState() == FileOperation.FAILED) {
                FailureType type =
                    CacheExceptionUtils.getFailureType(operation.getException(),
                                                       source != null);

                switch (type) {
                    case BROKEN:
                        if (source != null) {
                            pool = poolInfoMap.getPool(operation.getSource());
                            operationHandler.handleBrokenFileLocation(operation.getPnfsId(),
                                                                      pool);
                        }
                        // fall through - possibly retriable with another source
                    case NEWSOURCE:
                        operation.addSourceToTriedLocations();
                        operation.resetSourceAndTarget();
                        retry = true;
                        break;
                    case NEWTARGET:
                        operation.addTargetToTriedLocations();
                        operation.resetSourceAndTarget();
                        retry = true;
                        break;
                    case RETRIABLE:
                        operation.incrementRetried();
                        if (operation.getRetried() < maxRetries) {
                            retry = true;
                            break;
                        }
                        /*
                         *  We don't really know here whether the source
                         *  is bad or the target.  The best we can do is
                         *  retry until we have exhausted all the possible
                         *  pool group members.
                         */
                        operation.addTargetToTriedLocations();
                        if (source != null) {
                            operation.addSourceToTriedLocations();
                        }

                        /*
                         *  All readable pools in the pool group.
                         */
                        int groupMembers = poolInfoMap.getMemberPools
                                        (operation.getPoolGroup(), false).size();

                        if (groupMembers > operation.getTried().size()) {
                            operation.resetSourceAndTarget();
                            retry = true;
                            break;
                        }
                        // fall through; no more possibilities left
                    case FATAL:
                        operation.addTargetToTriedLocations();
                        if (source != null) {
                            operation.addSourceToTriedLocations();
                        }
                        Set<String> tried = operation.getTried().stream()
                                        .map(poolInfoMap::getPool)
                                        .collect(Collectors.toSet());
                        completionHandler.taskAborted(operation.getPnfsId(),
                                                      tried,
                                                      operation.getRetried(),
                                                      maxRetries,
                                                      operation.getException());
                        operation.abortOperation();
                        abort = true;

                        /*
                         * Only fatal errors count as operation failures.
                         */
                        counters.incrementOperationFailed(Operation.FILE.name());
                        counters.incrementFailed(pool, operation.getType());
                        break;
                    default:
                        operation.abortOperation();
                        abort = true;
                        LOGGER.error("{}: No such failure type: {}.",
                                        operation.getPnfsId(), type);
                }
                counters.recordTaskStatistics(operation.getTask(),
                                operation.getStateName(), type,
                                operation.getParent() == null ? null : pool,
                                source, target);
            } else if (operation.getState() == FileOperation.DONE) {
                counters.incrementOperation(Operation.FILE.name());
                counters.increment(source, target, operation.getType(),
                                operation.getSize());
                counters.recordTaskStatistics(operation.getTask(),
                                operation.getStateName(), null,
                                operation.getParent() == null ? null : pool,
                                source, target);
                operation.setSource(null);
                operation.setTarget(null);
            }

            if (operation.getOpCount() > 0) {
                operation.resetOperation();
                restore(operation, retry);
            } else {
                /*
                 *  If abort is not true, this is being called either
                 *  because all operations have completed, the task
                 *  has been VOIDed, or CANCEL was called with forcible
                 *  removal.
                 */
                remove(operation.getPnfsId(), abort);
            }
        }

        private void restore(FileOperation operation, boolean retry) {
            if (operation.isBackground()) {
                if (retry) {
                    background.addFirst(operation);
                } else {
                    background.addLast(operation);
                }
            } else {
                if (retry) {
                    foreground.addFirst(operation);
                } else {
                    foreground.addLast(operation);
                }
            }
        }
    }

    /**
     * <p>Handles dequeueing waiting operations and submitting them for
     *    task execution.</p>
     */
    class WaitingOperationProcessor {
        long fgAvailable;
        long bgAvailable;

        void processWaiting() {
            computeAvailable();

            LOGGER.trace("After computing available: {} foreground, "
                            + "{} background.",
                            fgAvailable, bgAvailable);

            long remainder = promoteToRunning(foreground, fgAvailable);
            promoteToRunning(background, bgAvailable + remainder);

            reset();
        }

        /**
         * <p>The number of waiting operations which can be promoted
         *    to running is based on the available slots (maximum minus
         *    the number of operations still running).</p>
         *
         * <p>The proportion allotted to foreground vs background is based
         *    on the proportion of operations in the queues.</p>
         *
         * <p>The maximum allocation percentage places both lower and upper
         *    bounds on the apportioned number of operations per queue.</p>
         *
         * <p>If after application of the bound, the proportion rounds to 0,
         *    but the relevant queue is not empty, one of its waiting tasks
         *    will be guaranteed to run.</p>
         */
        private void computeAvailable() {
            long available = copyThreads - running.size();

            if (available == 0) {
                fgAvailable = 0;
                bgAvailable = 0;
                return;
            }

            double fgsize = foreground.size();
            double bgsize = background.size();

            double size = fgsize+bgsize;

            if (size == 0.0) {
                fgAvailable = 0;
                bgAvailable = 0;
                return;
            }

            double fgweight = Math.min(Math.max(fgsize/size, 100-maxAllocation),
                                       maxAllocation);

            fgAvailable = Math.round(available * fgweight);
            bgAvailable = available - fgAvailable;

            if (fgAvailable == 0 && fgsize > 0) {
                fgAvailable = 1;
                --bgAvailable;
            } else if (bgAvailable == 0 && bgsize > 0) {
                bgAvailable = 1;
                --fgAvailable;
            }
        }

        /**
         * <p>Dequeues up to the indicated number of operations and submits
         *      them.</p>
         */
        private long promoteToRunning(Queue<FileOperation> queue, long limit) {
            for (int i = 0; i < limit; i++) {
                FileOperation operation = queue.poll();
                if (operation == null) {
                    return limit - i;
                }
                submit(operation);
            }
            return 0;
        }

        private void reset() {
            fgAvailable = 0;
            bgAvailable = 0;
        }

        /**
         * <p>Task is submitted to an appropriate executor service.</p>
         */
        private void submit(FileOperation operation) {
            int remove = SelectionAction.REMOVE.ordinal();
            operation.setTask(new ResilientFileTask(operation.getPnfsId(),
                            operation.getSelectionAction() == remove,
                            operation.getRetried(),
                            operationHandler));
            operation.setState(FileOperation.RUNNING);
            running.add(operation);
            operation.submit();
        }
    }

    /**
     *  <p>Accessed mostly for retrieval of the operation.  Writes occur on
     *      the handler threads adding operations and removes occur
     *      on the consumer thread.</p>
     *
     *  <p>Default sharding is probably OK for the present purposes,
     *      even with a large copyThreads value, so we have not specified the
     *      constructor parameters.</p>
     */
    final Map<PnfsId, FileOperation> index = new ConcurrentHashMap<>();

    /**
     *  <p>These queues are entirely used by the consumer thread. Hence
     *      there is no need for synchronization on any of them.</p>
     *
     *  <p>The order for election to run is FIFO.  The operation is
     *      removed from these waiting queues and added to running;
     *      an attempt at fairness is made by appending it back to
     *      these queues when it successfully terminates, if more work
     *      is to be done, but to restoring it to the head of the
     *      queue if there is a retriable failure.</p>
     */
    final Deque<FileOperation> foreground = new LinkedList<>();
    final Deque<FileOperation> background = new LinkedList<>();
    final Queue<FileOperation> running    = new LinkedList<>();

    /**
     *  <p>Queue of incoming/ready operations.  This buffer is
     *       shared between the handler and consumer threads, to avoid
     *       synchronizing the internal queues.  The incoming operations
     *       are appended to the latter during the consumer scan.</p>
     */
    final Queue<FileOperation> incoming = new LinkedList<>();

    /**
     *  <p>List of filters for cancelling operations.  This buffer is
     *       shared between the caller and the consumer thread.</p>
     *       Processing of cancellation is done during the consumer scan,
     *       as it would have to be atomic anyway.  This avoids once again any
     *       extra locking on the internal queues.</p>
     */
    final Collection<FileMatcher> cancelFilters = new ArrayList<>();

    /**
     * <p>For recovery.</p>
     */
    @VisibleForTesting
    final Checkpointer checkpointer = new Checkpointer();

    /**
     * <p>The consumer thread logic is encapsulated in these two processors.</p>
     */
    final TerminalOperationProcessor terminalOperationProcessor =
                    new TerminalOperationProcessor();
    final WaitingOperationProcessor waitingOperationProcessor =
                    new WaitingOperationProcessor();

    /**
     *  <p>For reporting operations terminated or canceled while the
     *      consumer thread is doing work outside the wait monitor.</p>
     */
    final AtomicInteger               signalled = new AtomicInteger(0);

    /**
     *  <p>Maximum proportion to allocate to either foreground or
     *      background operations.</p>
     */
    double maxAllocation = 0.8;

    private PoolInfoMap               poolInfoMap;
    private FileOperationHandler      operationHandler;
    private FileTaskCompletionHandler completionHandler;
    private PoolTaskCompletionHandler poolTaskCompletionHandler;

    /**
     *  <p>This number serves as the upper limit on the number of tasks
     *          which can be run at a given time. This ensures the tasks
     *          will not block on the executor queue waiting for a thread.
     *          To ensure the task does not block in general, the number of
     *          database connections must be at least equal to this, but should
     *          probably be 1 1/2 to 2 times greater to allow other operations
     *          to run as well.  See the default settings.</p>
     */
    private int copyThreads = 200;

    /**
     *  <p>Meaning for a given source-target pair. When a source or target
     *      is changed, the retry count is reset to 0.</p>
     */
    private int maxRetries  = 2;

    /**
     *  <p>Statistics collection.</p>
     */
    private OperationStatistics counters;
    private OperationHistory    history;

    /**
     * <p>Only used by admin command.</p>
     *
     * <p>Degenerate call to {@link #cancel(FileMatcher)}.</p>
     *
     * @param pnfsId single operation to cancel.
     * @param remove true if the entire entry is to be removed from the
     *               map at the next scan.  Otherwise, cancellation pertains
     *               only to the current (running) operation.
     */
    public void cancel(PnfsId pnfsId, boolean remove) {
        FileFilter filter = new FileFilter();
        filter.setPnfsIds(pnfsId.toString());
        filter.setForceRemoval(remove);
        cancel(filter);
    }

    /**
     * <p>Batch version of cancel.  In this case, the filter will
     *      indicate whether the operation should be cancelled <i>in toto</i>
     *      or only the current task.</p>
     *
     * <p>The actual scan is conducted by the consumer thread.</p>
     */
    public void cancel(FileMatcher filter) {
        synchronized (cancelFilters) {
            cancelFilters.add(filter);
        }
        signalAll();
    }

    /**
     * @return number of operations matching the filter.
     */
    public long count(FileMatcher filter, StringBuilder builder) {
        long total = 0;
        Iterator<FileOperation> iterator = index.values().iterator();

        Map<String, AtomicLong> summary =
                        builder == null ? null : new HashMap<>();

        while (iterator.hasNext()) {
            FileOperation operation = iterator.next();
            if (filter.matches(operation, poolInfoMap)) {
                ++total;

                if (summary == null) {
                    continue;
                }

                String pool = operation.getPrincipalPool(poolInfoMap);
                AtomicLong count = summary.get(pool);
                if (count == null) {
                    count = new AtomicLong(0);
                    summary.put(pool, count);
                }
                count.addAndGet(operation.getOpCount());
            }
        }

        if (summary != null) {
            summary.entrySet()
                   .stream()
                   .forEach((e) -> builder.append(String.format(COUNTS_FORMAT,
                                   e.getKey(), e.getValue())));
        }
        return total;
    }

    public long getCheckpointExpiry() {
        return checkpointer.expiry;
    }

    public TimeUnit getCheckpointExpiryUnit() {
        return checkpointer.expiryUnit;
    }

    public String getCheckpointFilePath() {
        return checkpointer.path;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public int getMaxRunning() {
        return copyThreads;
    }

    public FileOperation getOperation(PnfsId pnfsId) {
        return index.get(pnfsId);
    }

    @Override
    public void initialize() {
        super.initialize();
        startCheckpointer();
    }

    public boolean isCheckpointingOn() {
        return checkpointer.running;
    }

    /**
     *  <p>Used by the admin command.</p>
     */
    public String list(FileMatcher filter, int limit) {
        StringBuilder builder = new StringBuilder();
        Iterator<FileOperation> iterator = index.values().iterator();

        int total = 0;

        while (iterator.hasNext()) {
            FileOperation operation = iterator.next();
            if (filter.matches(operation, poolInfoMap)) {
                ++total;
                builder.append(operation).append("\n");
            }

            if (total >= limit) {
                break;
            }
        }

        if (total == 0) {
            builder.append("NO (MATCHING) OPERATIONS.\n");
        } else {
            builder.append("TOTAL OPERATIONS:\t\t").append(total).append("\n");
        }
        return builder.toString();
    }

    /**
     * <p>Called by the {@link FileOperationHandler}.
     *      Adds essential information to a new entry.</p>
     *
     * @return true if add returns true.
     */
    public boolean register(FileUpdate data) {
        FileOperation operation = new FileOperation(data.pnfsId,
                                                    data.getGroup(),
                                                    data.getUnitIndex(),
                                                    data.getSelectionAction(),
                                                    data.getCount(),
                                                    data.getSize());
        operation.setParentOrSource(data.getPoolIndex(), data.isParent);
        operation.setVerifySticky(data.shouldVerifySticky());
        FileAttributes attributes = data.getAttributes();
        operation.setRetentionPolicy(attributes.getRetentionPolicy().toString());
        operation.resetOperation();
        return add(data.pnfsId, operation);
    }

    /**
     * <p>Reads in the checkpoint file.  Creates one if it does not exist.
     *      For each entry read, creates a {@link FileUpdate} and calls
     *      {@link FileOperationHandler#handleLocationUpdate(FileUpdate)}.</p>
     */
    public void reload() {
        CheckpointUtils.load(checkpointer.path, poolInfoMap, this, operationHandler);
    }

    /**
     * <p>Called after a change to the checkpoint path and/or interval.
     *    Interrupts the thread so that it resumes with the new settings.</p>
     */
    public void reset() {
        if (isCheckpointingOn()) {
            checkpointer.resetInterrupt = true;
            checkpointer.thread.interrupt();
        }
    }

    /**
     * <p>The consumer thread. When notified or times out, iterates over
     *      the queues to check the state of running tasks and
     *      to submit waiting tasks if there are open slots.  Removes
     *      completed operations.</p>
     *
     * <p>Note that since the scan takes place outside of the monitor, the
     *      signals sent by various update methods will not be caught before
     *      the current thread is inside {@link #await}; for this reason, a
     *      counter is used and reset to 0 before each scan.  No wait occurs if
     *      the counter is non-zero after the scan.</p>
     */
    public void run() {
        try {
            while (!Thread.interrupted()) {
                LOGGER.trace("Calling scan.");

                signalled.set(0);

                scan();

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

                if (Thread.interrupted()) {
                    break;
                }

                LOGGER.trace("Scan complete, waiting ...");
                await();
            }
        } catch (InterruptedException e) {
            LOGGER.trace("Consumer was interrupted.");
        }

        LOGGER.info("Exiting file operation consumer.");
        clear();

        LOGGER.info("File operation queues and index cleared.");
    }

    /**
     * <p>If the checkpointing thread is running, interrupts the wait
     *      so that it calls save immediately.  If it is off, it just calls
     *      save. (Note:  the latter is done on the caller thread;
     *      this is used mainly for testing.  For the admin command,
     *      this method is disallowed if checkpointing is off.)</p>
     */
    public void runCheckpointNow() {
        if (isCheckpointingOn()) {
            checkpointer.runInterrupt = true;
            checkpointer.thread.interrupt();
        } else {
            checkpointer.save();
        }
    }

    /**
     *  <p>"File operation sweep:" the main queue update sequence (run by the consumer).</p>
     */
    @VisibleForTesting
    public void scan() {
        long start = System.currentTimeMillis();
        terminalOperationProcessor.processTerminated();
        waitingOperationProcessor.processWaiting();
        long end = System.currentTimeMillis();
        counters.recordFileOpSweep(end, end - start);
    }

    public void setCheckpointExpiry(long checkpointExpiry) {
        checkpointer.expiry = checkpointExpiry;
    }

    public void setCheckpointExpiryUnit(TimeUnit checkpointExpiryUnit) {
        checkpointer.expiryUnit = checkpointExpiryUnit;
    }

    public void setCheckpointFilePath(String checkpointFilePath) {
        checkpointer.path = checkpointFilePath;
    }

    public void setCompletionHandler(
                    FileTaskCompletionHandler completionHandler) {
        this.completionHandler = completionHandler;
    }

    public void setCopyThreads(int copyThreads) {
        this.copyThreads = copyThreads;
    }

    public void setCounters(OperationStatistics counters) {
        this.counters = counters;
    }

    public void setHistory(OperationHistory history) {
        this.history = history;
    }

    public void setMaxAllocation(double maxAllocation) {
        this.maxAllocation = maxAllocation;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public void setOperationHandler(FileOperationHandler operationHandler) {
        this.operationHandler = operationHandler;
    }

    public void setPoolInfoMap(PoolInfoMap poolInfoMap) {
        this.poolInfoMap = poolInfoMap;
    }

    public void setPoolTaskCompletionHandler(
                    PoolTaskCompletionHandler poolTaskCompletionHandler) {
        this.poolTaskCompletionHandler = poolTaskCompletionHandler;
    }

    @Override
    public void shutdown() {
        stopCheckpointer();
        super.shutdown();
    }

    public long size() {
        return index.size();
    }

    public void startCheckpointer() {
        checkpointer.thread = new Thread(checkpointer, "Checkpointing");
        checkpointer.thread.start();
    }

    public void stopCheckpointer() {
        checkpointer.running = false;
        if (checkpointer.thread != null) {
            checkpointer.thread.interrupt();
        }
    }

    /**
     * <p>Records the selected source and/or target. No state change is involved.</p>
     */
    public void updateOperation(PnfsId pnfsId, String source, String target) {
        FileOperation operation = index.get(pnfsId);

        if (operation == null) {
            throw new IllegalStateException(
                            String.format(MISSING_ENTRY, pnfsId));
        }

        if (source != null) {
            operation.setSource(poolInfoMap.getPoolIndex(source));
        }

        if (target != null) {
            operation.setTarget(poolInfoMap.getPoolIndex(target));
        }
    }

    /**
     * <p>Terminal update.</p>
     *
     * <p>Unlike with cancellation, this method should only
     *      be called in reference to submitted/running tasks; hence,
     *      there is no need to remove them from a queue at this point,
     *      as the consumer sweeps the running list during its scan.</p>
     */
    public void updateOperation(PnfsId pnfsId, CacheException error) {
        FileOperation operation = index.get(pnfsId);

        if (operation == null) {
            throw new IllegalStateException(
                            String.format(MISSING_ENTRY, pnfsId));
        }

        if (operation.updateOperation(error)) {
            signalAll();
        }
    }

    /**
     * <p>Migration task termination.</p>
     */
    public void updateOperation(PoolMigrationCopyFinishedMessage message) {
        PnfsId pnfsId = message.getPnfsId();

        FileOperation operation = index.get(pnfsId);

        if (operation == null) {
            throw new IllegalStateException(
                            String.format(MISSING_ENTRY, pnfsId));
        }

        operation.relay(message);
    }

    /**
     * <p>Terminal update. OpCount is set to 0 so consumer will remove.</p>
     *
     * <p>Unlike with cancellation, this method should only
     *      be called in reference to submitted/running tasks; hence,
     *      there is no need to remove them from a queue at this point,
     *      as the consumer sweeps the running list during its scan.</p>
     */
    public void voidOperation(PnfsId pnfsId) {
        FileOperation operation = index.get(pnfsId);

        if (operation == null) {
            return;
        }

        if (operation.voidOperation()) {
            signalAll();
        }
    }

    private boolean add(PnfsId pnfsId, FileOperation operation) {

        synchronized (incoming) {
            FileOperation present = index.get(pnfsId);
            if (present != null) {
                present.updateOperation(operation);
                return false;
            }

            index.put(pnfsId, operation);
            incoming.add(operation);
        }

        signalAll();

        return true;
    }

    private synchronized void await() throws InterruptedException {
        wait(timeoutUnit.toMillis(timeout));
    }

    private void clear() {
        foreground.clear();
        background.clear();
        running.clear();
        cancelFilters.clear();
        synchronized (incoming) {
            incoming.clear();
        }
        index.clear();
    }

    private void remove(PnfsId pnfsId, boolean failed) {
        FileOperation operation = index.remove(pnfsId);

        if (operation == null) {
            return;
        }

        if (operation.isBackground()) {
            String parent = poolInfoMap.getPool(operation.getParent());
            if (parent == null) {
                LOGGER.error("Operation on background "
                                + "queue has no parent: {}; "
                                + "this is a bug.", operation);
            }
            poolTaskCompletionHandler.childTerminated(parent, pnfsId);
        }

        history.add(operation.toHistoryString(), failed);
    }

    private synchronized void signalAll() {
        signalled.incrementAndGet();
        notifyAll();
    }
}