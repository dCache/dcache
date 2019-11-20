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
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.resilience.data.PoolOperation.NextAction;
import org.dcache.resilience.data.PoolOperation.State;
import org.dcache.resilience.db.ScanSummary;
import org.dcache.resilience.handlers.PoolOperationHandler;
import org.dcache.resilience.util.CheckpointUtils;
import org.dcache.resilience.util.ExceptionMessage;
import org.dcache.resilience.util.MapInitializer;
import org.dcache.resilience.util.Operation;
import org.dcache.resilience.util.OperationStatistics;
import org.dcache.resilience.util.PoolScanTask;
import org.dcache.util.RunnableModule;

/**
 * <p>Maintains three queues corresponding to the IDLE, WAITING, and RUNNING
 *    pool operation states.  The runnable method periodically scans the
 *    queues, promoting tasks as slots become available, and returning
 *    pool operation placeholders to IDLE when the related scan completes.</p>
 *
 * <p>When a pool status DOWN update is received, a certain grace period
 *    is observed before actually launching the associated task.</p>
 *
 * <p>Subsequent duplicate messages are handled according to a transition
 *    table (see {@link PoolOperation#getNextAction(PoolStatusForResilience)})
 *    which defines whether the current operation should be kept,
 *    replaced or cancelled (see {@link #update}).</p>
 *
 * <p>The map is swept every period (which should be less than or equal to that
 *    defined by the grace interval).  Idle pools are first checked
 *    for expired "last scan" timestamps; those eligible are promoted to
 *    the waiting queue (this is the "watchdog" component of the map).  Next,
 *    the waiting queue is scanned for grace interval expiration; the
 *    eligible operations are then promoted to running, with a scan task
 *    being launched.</p>
 *
 * <p>When a scan terminates, the update of the task records whether it
 *    completed successfully, was cancelled or failed, and the task is
 *    returned to the idle queue.</p>
 *
 * <p>Map provides methods for cancellation of running pool scans, and for
 *    ad hoc submission of a scan.</p>
 *
 * <p>If pools are added or removed from the {@link PoolInfoMap} via the
 *    arrival of a PoolMonitor message, the corresponding
 *    pool operation will also be added or removed here.</p>
 *
 * <p>Class is not marked final for stubbing/mocking purposes.</p>
 */
public class PoolOperationMap extends RunnableModule {
    class Watchdog {
        Integer  rescanWindow;
        TimeUnit rescanWindowUnit;
        volatile boolean running        = true;
        volatile boolean resetInterrupt = false;
        volatile boolean runInterrupt   = false;

        long getExpiry() {
            if (!running) {
                return Long.MAX_VALUE;
            }
            return rescanWindowUnit.toMillis(rescanWindow);
        }
    }

    final Map<String, PoolOperation> idle    = new LinkedHashMap<>();
    final Map<String, PoolOperation> waiting = new LinkedHashMap<>();
    final Map<String, PoolOperation> running = new HashMap<>();

    private final Lock      lock      = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private final Watchdog  watchdog  = new Watchdog();

    private PoolInfoMap             poolInfoMap;
    private PoolOperationHandler    handler;
    private FileOperationMap        fileOperationMap; // needed for cancellation

    private String                  excludedPoolsFile;

    private int                 downGracePeriod;
    private TimeUnit            downGracePeriodUnit;
    private int                 restartGracePeriod;
    private TimeUnit            restartGracePeriodUnit;
    private int                 maxConcurrentRunning;
    private OperationStatistics counters;

    public void saveExcluded() {
        lock.lock();
        try {
            CheckpointUtils.save(excludedPoolsFile, idle);
        } finally {
            lock.unlock();
        }
    }

    /**
     * <p>Called by the admin interface.</p>
     *
     * <p>Sets pool operation state to either included or
     *      excluded.  If the latter, it will not be susceptible to pool scans
     *      or status change messages, though it will continue to be checked for
     *      status when other pools are scanned.  The arrival of a new
     *      mode update will change the state but trigger nothing.</p>
     *
     * @param filter used only with regular expression for pools.
     * @param included whether to include or not.
     * @return the number of pool operations which have been included or
     *          excluded.
     */
    public long setIncluded(PoolMatcher filter, boolean included) {
        lock.lock();

        Set<String> visited = new HashSet<>();

        try {
            update(filter, running, included, visited);
            update(filter, waiting, included, visited);
            update(filter, idle, included, visited);
            condition.signalAll();
        } finally {
            lock.unlock();
        }

        return visited.size();
    }

    public void add(String pool) {
        lock.lock();
        try {
            addPool(pool);
        } finally {
            lock.unlock();
        }
    }

    /**
     * <p>Called by the admin interface.</p>
     *
     * @return the number of pool operations which have been cancelled.
     */
    public long cancel(PoolMatcher filter) {
        lock.lock();

        long cancelled = 0;
        try {
            if (filter.matchesRunning()) {
                cancelled += cancel(running, filter);
            }

            if (filter.matchesWaiting()) {
                cancelled += cancel(waiting, filter);
            }

            condition.signalAll();
        } finally {
            lock.unlock();
        }

        return cancelled;
    }

    @VisibleForTesting
    public PoolStatusForResilience getCurrentStatus(String pool) {
        /*
         *  Used only in testing, will not return null.
         */
        return get(pool).currStatus;
    }

    public int getDownGracePeriod() {
        return downGracePeriod;
    }

    public TimeUnit getDownGracePeriodUnit() {
        return downGracePeriodUnit;
    }

    public int getMaxConcurrentRunning() {
        return maxConcurrentRunning;
    }

    public int getRestartGracePeriod() {
        return restartGracePeriod;
    }

    public TimeUnit getRestartGracePeriodUnit() {
        return restartGracePeriodUnit;
    }

    public int getScanWindow() {
        return watchdog.rescanWindow;
    }

    public TimeUnit getScanWindowUnit() {
        return watchdog.rescanWindowUnit;
    }

    public String getState(String pool) {
        PoolOperation operation = get(pool);
        if (operation == null) {
            return null;
        }
        return operation.state.name();
    }

    public boolean isWatchdogOn() {
        return watchdog.running;
    }

    /**
     * Called from admin interface.
     */
    public String list(PoolMatcher filter) {
        StringBuilder builder = new StringBuilder();
        TreeMap<String, PoolOperation>[] tmp =  new TreeMap[]{new TreeMap(),
                                                              new TreeMap<>(),
                                                              new TreeMap<>()};
        lock.lock();

        try {
            if (filter.matchesRunning()) {
                tmp[0].putAll(running);
            }

            if (filter.matchesWaiting()) {
                tmp[1].putAll(waiting);
            }

            if (filter.matchesIdle()) {
                tmp[2].putAll(idle);
            }
        } finally {
            lock.unlock();
        }

        int total = 0;

        for (TreeMap<String, PoolOperation> map: tmp) {
            for (Entry<String, PoolOperation> entry: map.entrySet()) {
                String key = entry.getKey();
                PoolOperation op = entry.getValue();
                if (filter.matches(key, op)) {
                    builder.append(key).append("\t").append(op).append("\n");
                    ++total;
                }
            }
        }

        if (total == 0) {
            builder.setLength(0);
            builder.append("NO (MATCHING) OPERATIONS.\n");
        } else {
            builder.append("TOTAL OPERATIONS:\t\t").append(total).append("\n");
        }

        return builder.toString();
    }

    /**
     * @return list of pools that have been removed.
     *
     * @see MapInitializer#initialize()
     */
    public List<String> loadPools() {
        List<String> removed = new ArrayList<>();
        lock.lock();
        try {
            Set<String> pools = poolInfoMap.getResilientPools();

            for (String pool : running.keySet()) {
                if (!pools.contains(pool)) {
                    removed.add(pool);
                }
            }
            for (String pool : waiting.keySet()) {
                if (!pools.contains(pool)) {
                    removed.add(pool);
                }
            }
            for (String pool : idle.keySet()) {
                if (!pools.contains(pool)) {
                    removed.add(pool);
                }
            }
            /*
             *  Will not overwrite existing placeholder.
             */
            pools.stream().forEach(this::addPool);
        } finally {
            lock.unlock();
        }

        CheckpointUtils.load(excludedPoolsFile).stream().forEach((p) -> {
            PoolFilter filter = new PoolFilter();
            filter.setPools(p);
            setIncluded(filter, false);
        });

        return removed;
    }

    public void remove(String pool) {
        lock.lock();
        try {
            if (running.containsKey(pool)) {
                /*
                 *  NB:  we cannot do anything about child pnfsid tasks here.
                 *  This must be handled by the caller.
                 */
                running.remove(pool).task.cancel("pool no longer resilient");
            } else if (waiting.remove(pool) == null) {
                idle.remove(pool);
            }
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void reset() {
        watchdog.resetInterrupt = true;
        threadInterrupt();
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            lock.lock();
            try {
                condition.await(timeout, timeoutUnit);
            } catch (InterruptedException e) {
                if (watchdog.resetInterrupt) {
                    LOGGER.trace("Pool watchdog reset, returning to wait: "
                                                 + "timeout {} {}.", timeout,
                                 timeoutUnit);
                    watchdog.resetInterrupt = false;
                    continue;
                }
                if (!watchdog.runInterrupt) {
                    LOGGER.trace("Pool watchdog wait was interrupted; exiting.");
                    break;
                }
                watchdog.runInterrupt = false;
            } finally {
                lock.unlock();
            }

            if (Thread.interrupted()) {
                break;
            }

            LOGGER.trace("Pool watchdog initiating scan.");
            scan();
            LOGGER.trace("Pool watchdog scan completed.");
        }

        LOGGER.info("Exiting pool operation consumer.");
        clear();

        LOGGER.info("Pool operation queues cleared.");
    }

    public void runNow() {
        watchdog.runInterrupt = true;
        threadInterrupt();
    }

    /**
     * <p>Called by {@link org.dcache.resilience.handlers.PoolInfoChangeHandler)</p>
     *
     * See documentation at {@link #doScan}.
     */
    public boolean scan(PoolStateUpdate update, boolean bypassStateCheck) {
        lock.lock();
        try {
            return doScan(update, bypassStateCheck);
        } finally {
            lock.unlock();
        }
    }

    /**
     * <p>Called by admin command.</p>
     *
     * <p>Tries to match the filter against pool operation on the
     *      WAITING or IDLE queue.  As in the auxiliary method called,
     *      WAITING operations have their forceScan flag set to true,
     *      but are not promoted to RUNNING here.</p>
     *
     * See documentation at {@link #doScan}.
     */
    public void scan(PoolFilter filter, StringBuilder reply, StringBuilder errors) {
        lock.lock();
        try {
            Set<String> pools = poolInfoMap.getResilientPools();
            for (String pool : pools) {
                PoolOperation operation = null;
                if (waiting.containsKey(pool)) {
                    operation = waiting.get(pool);
                } else if (idle.containsKey(pool)) {
                    operation = idle.get(pool);
                }

                if (operation == null) {
                    continue;
                }

                if (filter.matches(pool, operation)) {
                    try {
                        /*
                         *  true overrides considerations of whether
                         *  the pool has already been scanned because
                         *  it is down, or has been excluded.
                         *
                         *  Since this is an admin command, we force the
                         *  check on partitions as well.
                         */
                        operation.unit = ScanSummary.ALL_UNITS;

                        if (doScan(poolInfoMap.getPoolState(pool), true)) {
                            reply.append("\t").append(pool).append("\n");
                        }
                    } catch (IllegalArgumentException e) {
                        errors.append("\t")
                              .append(String.format("%s, %s", pool,
                                                    new ExceptionMessage(e)))
                              .append("\n");
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public void setCounters(OperationStatistics counters) {
        this.counters = counters;
    }

    public void setDownGracePeriod(int downGracePeriod) {
        this.downGracePeriod = downGracePeriod;
    }

    public void setDownGracePeriodUnit(TimeUnit downGracePeriodUnit) {
        this.downGracePeriodUnit = downGracePeriodUnit;
    }

    public void setExcludedPoolsFile(String excludedPoolsFile) {
        this.excludedPoolsFile = excludedPoolsFile;
    }

    public void setHandler(PoolOperationHandler handler) {
        this.handler = handler;
    }

    public void setMaxConcurrentRunning(int maxConcurrentRunning) {
        this.maxConcurrentRunning = maxConcurrentRunning;
    }

    public void setFileOperationMap(FileOperationMap fileOperationMap) {
        this.fileOperationMap = fileOperationMap;
    }

    public void setPoolInfoMap(PoolInfoMap poolInfoMap) {
        this.poolInfoMap = poolInfoMap;
    }

    public void setRescanWindow(int rescanWindow) {
        watchdog.rescanWindow = rescanWindow;
    }

    public void setRescanWindowUnit(TimeUnit rescanWindowUnit) {
        watchdog.rescanWindowUnit = rescanWindowUnit;
    }

    public void setRestartGracePeriod(int restartGracePeriod) {
        this.restartGracePeriod = restartGracePeriod;
    }

    public void setRestartGracePeriodUnit(TimeUnit restartGracePeriodUnit) {
        this.restartGracePeriodUnit = restartGracePeriodUnit;
    }

    public void setWatchdog(boolean on) {
        watchdog.running = on;
    }

    public void updateInitialized() {
        poolInfoMap.getResilientPools().stream()
                   .filter(poolInfoMap::isInitialized)
                   .map(poolInfoMap::getPoolState)
                   .forEach(this::update);
    }

    /**
     * <p>Called upon receipt of a pool status update (generated via
     *      comparison of PoolMonitor data).</p>
     */
    public void update(PoolStateUpdate update) {
        /*
         *  If the pool is not mapped, this will return false.
         */
        if (!poolInfoMap.isResilientPool(update.pool)) {
            return;
        }

        lock.lock();
        try {
            Map<String, PoolOperation> queue = running;
            PoolOperation operation = queue.get(update.pool);

            if (operation == null) {
                queue = waiting;
                operation = queue.get(update.pool);
                if (operation == null) {
                    queue = idle;
                    operation = queue.get(update.pool);
                }
            }

            NextAction nextAction = operation.getNextAction(update.getStatus());
            if (nextAction == NextAction.NOP) {
                operation.lastUpdate = System.currentTimeMillis();
                return;
            }

            if (operation.state == State.RUNNING) {
                /*
                 *  NOTE:  there is a need to cancel children here,
                 *  even though the task is being canceled because
                 *  of a change of status, so that all waiting file tasks
                 *  would eventually find the available replica count changed
                 *  and act accordingly.  The problem is with possible
                 *  inconsistencies in the child counts for the operation,
                 *  which need to be zeroed out in order to guarantee
                 *  the second operation will complete successfully.
                 */
                operation.task.cancel("pool " + update.pool + " changed");
                FileFilter fileFilter = new FileCancelFilter();
                fileFilter.setForceRemoval(true);
                fileFilter.setParent(update.pool);
                fileOperationMap.cancel(fileFilter);
            }

            switch (nextAction) {
                case DOWN_TO_UP:
                case UP_TO_DOWN:
                    if (operation.state == State.WAITING) {
                        LOGGER.trace("Update, {} already on WAITING queue, {}.",
                                     update.pool, operation);
                        break;
                    }

                    LOGGER.trace("Update, putting {} on WAITING queue, {}.",
                                    update.pool, operation);
                    queue.remove(update.pool);
                    operation.resetChildren();
                    operation.resetFailed();
                    operation.lastUpdate = System.currentTimeMillis();
                    operation.state = State.WAITING;
                    operation.group = update.group;
                    if (update.storageUnit != null) {
                        operation.unit = poolInfoMap.getUnitIndex(update.storageUnit);
                    }
                    operation.exception = null;
                    operation.task = null;
                    waiting.put(update.pool, operation);
                    break;
            }
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * <p>Called by the {@link PoolOperationHandler) when scan completes.</p>
     */
    public void update(String pool, int children) {
        update(pool, children, null);
    }

    /**
     * <p>Called by the {@link FileOperationMap ) when a child operation completes.</p>
     */
    public void update(String pool, PnfsId pnfsId, boolean failed) {
        LOGGER.debug("Parent {}, child operation for {} has completed.", pool,
                     pnfsId);
        lock.lock();
        try {
            PoolOperation operation = get(pool);
            operation.incrementCompleted(failed);
            if (operation.isComplete()) {
                terminate(pool, operation);
                condition.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * <p>Called by the {@link PoolOperationHandler)
     *      when scan completes or fails.</p>
     */
    public void update(String pool,
                       int children,
                       CacheException exception) {
        LOGGER.debug("Pool {}, operation update, children {}.", pool,
                     children);
        lock.lock();
        try {
            PoolOperation operation = get(pool);
            operation.exception = exception;
            operation.setChildren(children);
            operation.lastScan = System.currentTimeMillis();
            operation.lastUpdate = operation.lastScan;

            if (children == 0 || operation.isComplete()) {
                terminate(pool, operation);
                condition.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    @VisibleForTesting
    public void scan() {
        scanIdle();
        scanWaiting();
    }

    private void addPool(String pool) {
        /*
         *  Idempotency.  Should not fail.
         */
        if (idle.containsKey(pool)
                        || waiting.containsKey(pool)
                        || running.containsKey(pool)) {
            return;
        }

        if (!poolInfoMap.isResilientPool(pool)) {
            LOGGER.debug("skipping operation for non-resilient pool {}", pool);
            return;
        }
        idle.put(pool, new PoolOperation());
        counters.registerPool(pool);
    }

    private long cancel(Map<String, PoolOperation> queue, PoolMatcher filter) {
        AtomicLong canceled = new AtomicLong(0);

        ImmutableSet.copyOf(queue.keySet()).stream().forEach((k) -> {
            PoolOperation operation = queue.get(k);
            if (filter.matches(k, operation)) {
                cancel(k, operation, queue);
                canceled.incrementAndGet();
            }
        });

        return canceled.get();
    }

    private void cancel(String pool, PoolOperation operation,
                    Map<String, PoolOperation> queue) {
        if (operation.task != null) {
            operation.task.cancel("resilient admin command");
            operation.task = null;
        }

        queue.remove(pool);
        operation.state = State.CANCELED;
        reset(pool, operation);
    }

    private void clear() {
        lock.lock();

        try {
            running.clear();
            waiting.clear();
            idle.clear();
        } finally {
            lock.unlock();
        }
    }

    /**
     * <p>Serves ad hoc scans. Ignores the grace period timeouts (this
     *    corresponds to setting the <code>forceScan</code>
     *    flag on the operation).</p>
     *
     * <p>Will <i>not</i> override the behavior of normal task submission by
     *      cancelling any outstanding task for this pool.</p>
     *
     * <p>If indicated, bypasses the transition checking of pool status.</p>
     *
     * <p>Called after lock has been acquired.</p>
     *
     * @param bypassStateCheck if false, will not bypass considerations
     *                              of whether the pool has been previously
     *                              scanned because it went down.  NOTE:
     *                              an excluded pool will not be scanned
     *                              under any circumstances until it is
     *                              included.
     * @return true only if operation has been promoted from idle to waiting.
     */
    private  boolean doScan(PoolStateUpdate update, boolean bypassStateCheck) {
        if (running.containsKey(update.pool)) {
            LOGGER.info("Scan of {} is already in progress", update.pool);
            return false;
        }

        PoolOperation operation;

        if (waiting.containsKey(update.pool)) {
            LOGGER.info("Scan of {} is already in waiting state, setting its "
                                        + "force flag to true.",
                        update.pool);
            waiting.get(update.pool).forceScan = true;
            return false;
        }

        operation = idle.remove(update.pool);
        if (operation == null) {
            LOGGER.warn("No entry for {} in any queues; "
                                        + "pool is not (yet) registered.",
                        update.pool);
            return false;
        }

        if (operation.currStatus == PoolStatusForResilience.UNINITIALIZED) {
            LOGGER.info("Cannot scan {} –– uninitialized", update.pool);
            reset(update.pool, operation);
            return false;
        }

        if (operation.state == State.EXCLUDED) {
            LOGGER.info("Skipping scan {} –– pool is excluded", update.pool);
            reset(update.pool, operation);
            return false;
        }

        if (!bypassStateCheck) {
            if (operation.currStatus == PoolStatusForResilience.DOWN &&
                operation.lastStatus == PoolStatusForResilience.DOWN) {
                LOGGER.info("Skipping scan {} –– pool is down and was already "
                                + "scanned", update.pool);
                reset(update.pool, operation);
                return false;
            }
        }

        operation.getNextAction(update.getStatus());
        operation.forceScan = true;
        operation.lastUpdate = System.currentTimeMillis();
        operation.state = State.WAITING;
        operation.group = update.group;

        if (update.storageUnit != null) {
            operation.unit = poolInfoMap.getUnitIndex(update.storageUnit);
        }

        operation.exception = null;
        operation.resetFailed();
        operation.task = null;
        waiting.put(update.pool, operation);
        return true;
    }

    /**
     * @return operation, or <code>null</code> if not mapped.
     */
    private PoolOperation get(String pool) {
        PoolOperation operation = running.get(pool);

        if (operation == null) {
            operation = waiting.get(pool);
        }

        if (operation == null) {
            operation = idle.get(pool);
        }

        return operation;
    }

    private void reset(String pool, PoolOperation operation) {
        operation.lastUpdate = System.currentTimeMillis();
        operation.group = null;
        operation.unit = null;
        operation.forceScan = false;
        operation.resetChildren();
        if (poolInfoMap.isResilientPool(pool)) {
            idle.put(pool, operation);
        } else if (operation.state == State.FAILED || operation.failedChildren() > 0) {
            String message = operation.exception == null ? "" : "exception: " +
                            new ExceptionMessage(operation.exception);
            LOGGER.error(AlarmMarkerFactory.getMarker(
                            PredefinedAlarm.FAILED_REPLICATION, pool),
                            "{} was removed from resilient group but final scan "
                                            + "{}; {} failed file operations.",
                         pool, message, operation.failedChildren());
        }
    }

    /**
     * <p>Handles the periodic scan/watchdog function.
     *      The scan uses the implicit temporal ordering of puts to the linked
     *      hash map to find all expired pools (they will be at the head of the
     *      list maintained by the map).</p>
     */
    private void scanIdle() {
        lock.lock();
        try {
            long now = System.currentTimeMillis();
            long expiry = watchdog.getExpiry();

            for (Iterator<Entry<String, PoolOperation>> i
                    = idle.entrySet().iterator(); i.hasNext(); ) {
                Entry<String, PoolOperation> entry = i.next();
                String pool = entry.getKey();
                PoolOperation operation = entry.getValue();

                if (operation.state == State.EXCLUDED) {
                    continue;
                }

                if (operation.currStatus == PoolStatusForResilience.UNINITIALIZED) {
                    continue;
                }

                if (operation.lastStatus == PoolStatusForResilience.DOWN &&
                    operation.currStatus == PoolStatusForResilience.DOWN) {
                    /*
                     *  When a scan completes or the operation is reset,
                     *  the lastStatus is set to the current status.
                     *  If both of these are DOWN, then the pool has
                     *  not changed state since the last scan.  We avoid
                     *  rescanning DOWN pools that have already been scanned.
                     */
                    continue;
                }

                if (now - operation.lastScan >= expiry) {
                    i.remove();
                    operation.forceScan = true;
                    operation.state = State.WAITING;
                    operation.resetFailed();
                    operation.exception = null;
                    /*
                     *  This is a periodic scan, so check for repartitioning.
                     */
                    operation.unit = ScanSummary.ALL_UNITS;
                    waiting.put(pool, operation);
                } else {
                    /**
                     * time-ordering invariant guarantees there are
                     * no more candidates at this point
                     */
                    break;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void scanWaiting() {
        lock.lock();
        try {
            long now = System.currentTimeMillis();
            long downExpiry = downGracePeriodUnit.toMillis(downGracePeriod);
            long restartExpiry =
                            restartGracePeriodUnit.toMillis(restartGracePeriod);

            for (Iterator<Entry<String, PoolOperation>> i
                 = waiting.entrySet().iterator(); i.hasNext(); ) {
                Entry<String, PoolOperation> entry = i.next();
                String pool = entry.getKey();
                PoolOperation operation = entry.getValue();
                long expiry;

                switch (operation.currStatus) {
                    case DOWN:  expiry = downExpiry; break;
                    default:    expiry = restartExpiry; break;
                }

                /*
                 *  promote to running if force scan is true or
                 *  the grace period has expired; only promote
                 *  when a slot is available
                 */
                if ((operation.forceScan ||
                                now - operation.lastUpdate >= expiry)
                                && running.size() < maxConcurrentRunning) {
                    i.remove();
                    LOGGER.trace("{}, lapsed time {}, running {}: submitting.",
                                 operation,
                                 now - operation.lastUpdate,
                                 running.size());
                    submit(pool, operation);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void submit(String pool, PoolOperation operation) {
        operation.task = new PoolScanTask(pool,
                                          operation.currStatus.getMessageType(),
                                          operation.group,
                                          operation.unit,
                                          operation.forceScan,
                                          handler);
        operation.state = State.RUNNING;
        operation.lastUpdate = System.currentTimeMillis();
        operation.lastStatus = operation.currStatus;
        operation.task.setErrorHandler(e -> update(pool, 0, e));
        running.put(pool, operation);
        LOGGER.trace("Submitting pool scan task for {}.", pool);
        operation.task.submit();
    }

    private void terminate(String pool, PoolOperation operation) {
        LOGGER.debug("terminate, pool {}, {}.", pool, operation);
        String operationType = Operation.get(operation.currStatus).name();

        if (operation.exception != null) {
            operation.state = State.FAILED;
            counters.incrementOperationFailed(operationType);
        } else {
            operation.state = State.IDLE;
            counters.incrementOperation(operationType);
        }

        if (running.containsKey(pool)) {
            running.remove(pool);
        } else {
            waiting.remove(pool);
        }

        reset(pool, operation);
    }

    private void update(PoolMatcher filter,
                        Map<String, PoolOperation> queue,
                        boolean include,
                        Set<String> visited) {
        ImmutableSet.copyOf(queue.keySet()).stream().forEach((k) -> {
            if (!visited.contains(k)) {
                PoolOperation operation = queue.get(k);
                if (filter.matches(k, operation)) {
                    if (!include) {
                        if (operation.task != null) {
                            operation.task.cancel("pool include/exclude admin command");
                        }
                        operation.state = State.EXCLUDED;
                        queue.remove(k);
                        reset(k, operation);
                        visited.add(k);
                    } else if (operation.state == State.EXCLUDED) {
                        operation.state = State.IDLE;
                        /*
                         * treat the operation as if emerging from an
                         * undefined state; allow user to decide
                         * whether to scan or not
                         */
                        operation.currStatus = PoolStatusForResilience.UNINITIALIZED;
                        update(poolInfoMap.getPoolState(k));
                        visited.add(k);
                    }
                    /*
                     *  mark the pool information so that other operations
                     *  know this pool is temporarily in limbo.
                     */
                    poolInfoMap.getPoolInformation(poolInfoMap.getPoolIndex(k))
                               .setExcluded(!include);
                }
            }
        });
    }
}