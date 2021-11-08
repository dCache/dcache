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
package org.dcache.qos.services.scanner.data;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.CacheException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.qos.data.PoolQoSStatus;
import org.dcache.qos.services.scanner.data.PoolScanOperation.NextAction;
import org.dcache.qos.services.scanner.data.PoolScanOperation.State;
import org.dcache.qos.services.scanner.handlers.PoolOpHandler;
import org.dcache.qos.services.scanner.util.PoolScanTask;
import org.dcache.qos.services.scanner.util.QoSScannerCounters;
import org.dcache.qos.util.ExceptionMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains three queues corresponding to the IDLE, WAITING, and RUNNING pool operation states. The
 * runnable method sweeps the queues, promoting tasks as slots become available, and returning pool
 * operation placeholders to IDLE when the related scan completes.
 * <p/>
 * When a pool status DOWN update is received, a certain grace period is observed before actually
 * launching the associated task.
 * <p/>
 * Subsequent duplicate messages are handled according to a transition table (see {@link
 * PoolScanOperation#getNextAction(PoolQoSStatus)}) which defines whether the current operation
 * should be kept, replaced or cancelled (see {@link #update}).
 * <p/>
 * When a pool scan terminates, the update of the task records whether it completed successfully,
 * was cancelled or failed, and the task is returned to the idle queue.
 * <p/>
 * Periodically scheduled scans are not done on individual pools, but are controlled by the system
 * scan operation map.
 * <p/>
 * Map provides methods for cancellation of running scans, and for ad hoc submission of a scan.
 * <p/>
 * If pools are added or removed from the Pool Selection Unit (via the arrival of a PoolMonitor
 * message), the corresponding pool operation will be added or removed here.
 * <p/>
 * Class is not marked final for stubbing/mocking purposes.
 */
public class PoolOperationMap extends ScanOperationMap {

    private static final long INITIALIZATION_GRACE_PERIOD = TimeUnit.MINUTES.toMillis(5);
    private static final Logger LOGGER = LoggerFactory.getLogger(PoolOperationMap.class);

    @VisibleForTesting
    protected final Map<String, PoolScanOperation> idle = new LinkedHashMap<>();
    protected final Map<String, PoolScanOperation> waiting = new LinkedHashMap<>();
    protected final Map<String, PoolScanOperation> running = new HashMap<>();

    private PoolSelectionUnit currentPsu;
    private PoolOpHandler handler;
    protected QoSScannerCounters counters;

    private String excludedPoolsFile;

    private long initializationGracePeriod = INITIALIZATION_GRACE_PERIOD;
    private TimeUnit initializationGracePeriodUnit = TimeUnit.MINUTES;

    private int downGracePeriod;
    private TimeUnit downGracePeriodUnit;

    private int restartGracePeriod;
    private TimeUnit restartGracePeriodUnit;

    private int maxRunningIdleTime;
    private TimeUnit maxRunningIdleTimeUnit;

    private int maxConcurrentRunning;

    /**
     * Read back in from the excluded pools file pool names.
     * <p/>
     * Deletes the file when done; NOP if there is no file.
     */
    private static Collection<String> load(String excludedPoolsFile) {
        File current = new File(excludedPoolsFile);
        if (!current.exists()) {
            return Collections.EMPTY_LIST;
        }

        Collection<String> excluded = new ArrayList<>();

        try (BufferedReader fr = new BufferedReader(new FileReader(current))) {
            excluded = Files.readLines(current, StandardCharsets.US_ASCII);
            current.delete();
        } catch (FileNotFoundException e) {
            LOGGER.error("Unable to reload excluded pools file: {}", e.getMessage());
        } catch (IOException e) {
            LOGGER.error("Unrecoverable error during reload excluded pools file: {}",
                  e.getMessage());
        }

        return excluded;
    }

    /**
     * Save the excluded pool names to a file.
     * <p/>
     * If there already is such a file, it is deleted.
     *
     * @param excludedPoolsFile to read
     * @param operations        the pools which could potentially be in the excluded state.
     */
    private static void save(String excludedPoolsFile,
          Map<String, PoolScanOperation> operations) {
        File current = new File(excludedPoolsFile);
        try {
            java.nio.file.Files.deleteIfExists(current.toPath());
        } catch (IOException e) {
            LOGGER.error("Unable to delete {}: {}", current, e.getMessage());
        }
        try (PrintWriter fw = new PrintWriter(new FileWriter(excludedPoolsFile, false))) {
            operations.entrySet().stream().filter((e) -> e.getValue().isExcluded())
                  .forEach((e) -> fw.println(e.getKey()));
        } catch (FileNotFoundException e) {
            LOGGER.error("Unable to save excluded pools file: {}", e.getMessage());
        } catch (IOException e) {
            LOGGER.error("Unrecoverable error during save of excluded pools file: {}",
                  e.getMessage());
        }
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
     * Called by the admin interface.
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

    public String configSettings() {
        return String.format("down grace period %s %s\n"
                    + "restart grace period %s %s\n"
                    + "maximum idle time before a running operation is canceled  %s %s\n"
                    + "maximum concurrent operations %s\n"
                    + "period set to %s %s\n\n",
              downGracePeriod,
              downGracePeriodUnit,
              restartGracePeriod,
              restartGracePeriodUnit,
              maxRunningIdleTime,
              maxRunningIdleTimeUnit,
              maxConcurrentRunning,
              timeout,
              timeoutUnit);
    }

    @VisibleForTesting
    public PoolQoSStatus getCurrentStatus(String pool) {
        /*
         *  Used only in testing, will not return null.
         */
        return get(pool).currStatus;
    }

    @Override
    public void getInfo(PrintWriter pw) {
        StringBuilder builder = new StringBuilder();
        builder.append(configSettings());
        counters.appendRunning(builder);
        counters.appendSweep(builder);
        builder.append("\n");
        counters.appendCounts(builder);
        pw.print(builder);
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

    public int getMaxRunningIdleTime() {
        return maxRunningIdleTime;
    }

    public TimeUnit getMaxRunningIdleTimeUnit() {
        return maxRunningIdleTimeUnit;
    }

    public int getRestartGracePeriod() {
        return restartGracePeriod;
    }

    public TimeUnit getRestartGracePeriodUnit() {
        return restartGracePeriodUnit;
    }

    public String getState(String pool) {
        PoolScanOperation operation = get(pool);
        if (operation == null) {
            return null;
        }
        return operation.state.name();
    }

    public void handlePoolStatusChange(String pool, PoolQoSStatus status) {
        if (status != PoolQoSStatus.UNINITIALIZED) {
            updateStatus(pool, status);
        }
    }

    public boolean isInitialized(PoolV2Mode mode) {
        return PoolQoSStatus.valueOf(mode) != PoolQoSStatus.UNINITIALIZED;
    }

    /**
     * Called from admin interface.
     */
    public String list(PoolMatcher filter) {
        StringBuilder builder = new StringBuilder();
        TreeMap<String, PoolScanOperation>[] tmp = new TreeMap[]{new TreeMap(),
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

        for (TreeMap<String, PoolScanOperation> map : tmp) {
            for (Entry<String, PoolScanOperation> entry : map.entrySet()) {
                String key = entry.getKey();
                PoolScanOperation op = entry.getValue();
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
     */
    public List<String> loadPools() {
        List<String> removed = new ArrayList<>();
        lock.lock();
        try {
            LOGGER.info("Getting mapped pools.");
            Set<String> pools = currentPsu.getAllDefinedPools(false)
                  .stream()
                  .map(SelectionPool::getName)
                  .collect(Collectors.toSet());

            LOGGER.info("Eliminating old pools from running.");
            for (String pool : running.keySet()) {
                if (!pools.contains(pool)) {
                    removed.add(pool);
                }
            }

            LOGGER.info("Eliminating old pools from waiting.");
            for (String pool : waiting.keySet()) {
                if (!pools.contains(pool)) {
                    removed.add(pool);
                }
            }

            LOGGER.info("Eliminating old pools from idle.");
            for (String pool : idle.keySet()) {
                if (!pools.contains(pool)) {
                    removed.add(pool);
                }
            }

            /*
             *  Will not overwrite existing placeholder.
             */
            LOGGER.info("Adding pools.");
            pools.stream().forEach(this::addPool);

        } finally {
            lock.unlock();
        }

        LOGGER.info("loading excluded pools.");
        load(excludedPoolsFile).stream().forEach((p) -> {
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
                running.remove(pool).task.cancel("pool no longer valid");
            } else if (waiting.remove(pool) == null) {
                idle.remove(pool);
            }
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void runScans() {
        scanWaiting();
        scanRunning();
    }

    public void saveExcluded() {
        lock.lock();
        try {
            save(excludedPoolsFile, idle);
        } finally {
            lock.unlock();
        }
    }

    public boolean scan(String pool, String addedTo, String removedFrom,
          String storageUnit, PoolV2Mode mode, boolean bypassStateCheck) {
        lock.lock();
        try {
            return doScan(pool, addedTo, removedFrom, storageUnit, mode, bypassStateCheck);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Called by admin command.
     * <p/>
     * Tries to match the filter against pool operation on the WAITING or IDLE queue. As in the
     * auxiliary method called, WAITING operations have their forceScan flag set to true, but are
     * not promoted to RUNNING here.
     * <p/>
     * Also checks to see if the See documentation at {@link #doScan}.
     */
    public PoolScanReply scan(PoolFilter filter) {
        PoolScanReply reply = new PoolScanReply();
        lock.lock();
        try {
            Set<String> pools = getMappedPools();
            for (String pool : pools) {
                PoolScanOperation operation = null;
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
                        PoolV2Mode mode = null;
                        SelectionPool selectionPool = currentPsu.getPool(pool);
                        if (selectionPool != null) {
                            mode = currentPsu.getPool(pool).getPoolMode();
                        }
                        if (doScan(pool, null, null,
                              null, mode, true)) {
                            reply.addPool(pool, Optional.empty());
                        }
                    } catch (IllegalArgumentException e) {
                        reply.addPool(pool, Optional.of(e));
                    }
                }
            }
        } finally {
            lock.unlock();
            return reply;
        }
    }

    public void setCounters(QoSScannerCounters counters) {
        this.counters = counters;
    }

    public void setCurrentPsu(PoolSelectionUnit currentPsu) {
        lock.lock();
        try {
            this.currentPsu = currentPsu;
        } finally {
            lock.unlock();
        }
    }

    public void setExcludedPoolsFile(String excludedPoolsFile) {
        this.excludedPoolsFile = excludedPoolsFile;
    }

    public void setHandler(PoolOpHandler handler) {
        this.handler = handler;
    }

    /**
     * Called by the admin interface.
     * <p/>
     * Sets pool operation state to either included or excluded.  If the latter, it will not be
     * susceptible to pool scans or status change messages, though it will continue to be checked
     * for status when other pools are scanned. The arrival of a new mode update will change the
     * state but trigger nothing.
     *
     * @param filter   used only with regular expression for pools.
     * @param included whether to include or not.
     * @return the number of pool operations which have been included or excluded.
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

    public void setInitializationGracePeriod(long initializationGracePeriod) {
        this.initializationGracePeriod = initializationGracePeriod;
    }

    public void setInitializationGracePeriodUnit(TimeUnit initializationGracePeriodUnit) {
        this.initializationGracePeriodUnit = initializationGracePeriodUnit;
    }

    public void setDownGracePeriod(int downGracePeriod) {
        this.downGracePeriod = downGracePeriod;
    }

    public void setDownGracePeriodUnit(TimeUnit downGracePeriodUnit) {
        this.downGracePeriodUnit = downGracePeriodUnit;
    }

    public void setMaxConcurrentRunning(int maxConcurrentRunning) {
        this.maxConcurrentRunning = maxConcurrentRunning;
    }

    public void setMaxRunningIdleTime(int maxRunningIdleTime) {
        this.maxRunningIdleTime = maxRunningIdleTime;
    }

    public void setMaxRunningIdleTimeUnit(TimeUnit maxRunningIdleTimeUnit) {
        this.maxRunningIdleTimeUnit = maxRunningIdleTimeUnit;
    }

    public void setRestartGracePeriod(int restartGracePeriod) {
        this.restartGracePeriod = restartGracePeriod;
    }

    public void setRestartGracePeriodUnit(TimeUnit restartGracePeriodUnit) {
        this.restartGracePeriodUnit = restartGracePeriodUnit;
    }

    /**
     * Called upon receipt of a pool status update (generated via comparison of PoolMonitor data).
     */
    public void updateStatus(String pool, PoolQoSStatus status) {
        LOGGER.trace("updateStatus for {}: {}.", pool, status);
        lock.lock();
        try {
            Map<String, PoolScanOperation> queue = running;
            PoolScanOperation operation = queue.get(pool);

            if (operation == null) {
                queue = waiting;
                operation = queue.get(pool);
                if (operation == null) {
                    queue = idle;
                    operation = queue.get(pool);
                }
            }

            NextAction nextAction = operation.getNextAction(status);
            if (nextAction == NextAction.NOP) {
                return;
            }

            if (operation.state == State.RUNNING) {
                /*
                 *  NOTE:  there is a need to notify the verifier here,
                 *  so that all waiting file tasks would eventually find the
                 *  available replica count changed and act accordingly.
                 *  The problem is with possible inconsistencies in the child
                 *  counts for the operation, which need to be zeroed out
                 *  in order to guarantee the second operation will complete
                 *  successfully.
                 */
                operation.task.cancel("pool " + pool + " changed");
                handler.handlePoolScanCancelled(pool, status);
            }

            switch (nextAction) {
                case DOWN_TO_UP:
                case UP_TO_DOWN:
                    if (operation.state == State.WAITING) {
                        LOGGER.trace("Update, {} already on WAITING queue, {}.",
                              pool, operation);
                        break;
                    }

                    LOGGER.trace("Update, putting {} on WAITING queue, {}.",
                          pool, operation);
                    queue.remove(pool);
                    operation.resetChildren();
                    operation.resetFailed();
                    operation.lastUpdate = System.currentTimeMillis();
                    operation.lastScan = System.currentTimeMillis();
                    operation.state = State.WAITING;
                    operation.exception = null;
                    operation.task = null;
                    waiting.put(pool, operation);
                    break;
            }
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void update(String pool, boolean failed) {
        LOGGER.debug("Parent {}, child operation has completed.", pool);
        lock.lock();
        try {
            PoolScanOperation operation = get(pool);
            operation.incrementCompleted(failed);
            if (operation.isComplete()) {
                terminate(pool, operation);
                condition.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    public void update(String pool, long children) {
        update(pool, children, null);
    }

    public void update(String pool, long children, CacheException exception) {
        LOGGER.debug("Pool {}, operation update, children {}.", pool, children);
        lock.lock();
        try {
            PoolScanOperation operation = get(pool);
            operation.exception = exception;
            operation.setChildren(children);
            operation.lastUpdate = System.currentTimeMillis();

            if (children == 0 || operation.isComplete()) {
                terminate(pool, operation);
                condition.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    protected void clear() {
        lock.lock();

        try {
            running.clear();
            waiting.clear();
            idle.clear();
        } finally {
            lock.unlock();
        }
    }

    protected void recordSweep(long start, long end) {
        counters.recordSweep(end, end - start);
    }

    @GuardedBy("lock")
    private void addPool(String pool) {
        /*
         *  Idempotency.  Should not fail.
         */
        if (idle.containsKey(pool)
              || waiting.containsKey(pool)
              || running.containsKey(pool)) {
            return;
        }

        idle.put(pool,
              new PoolScanOperation(
                    initializationGracePeriodUnit.toMillis(initializationGracePeriod)));
    }

    @GuardedBy("lock")
    private long cancel(Map<String, PoolScanOperation> queue, PoolMatcher filter) {
        AtomicLong canceled = new AtomicLong(0);

        ImmutableSet.copyOf(queue.keySet()).stream().forEach((k) -> {
            PoolScanOperation operation = queue.get(k);
            if (filter.matches(k, operation)) {
                cancel(k, operation, queue);
                canceled.incrementAndGet();
            }
        });

        return canceled.get();
    }

    @GuardedBy("lock")
    private void cancel(String pool, PoolScanOperation operation,
          Map<String, PoolScanOperation> queue) {
        if (operation.task != null) {
            operation.task.cancel("qos admin command");
            operation.task = null;
        }
        counters.incrementCancelled(pool, operation.currStatus, operation.getCompleted(),
              operation.forceScan,
              System.currentTimeMillis() - operation.lastScan);
        queue.remove(pool);
        operation.state = State.CANCELED;
        reset(pool, operation);
        handler.handlePoolScanCancelled(pool, operation.currStatus);
    }

    private PoolSelectionUnit currentPsu() {
        lock.lock();
        try {
            return currentPsu;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Serves ad hoc scans. Ignores the grace period timeouts (this corresponds to setting the
     * <code>forceScan</code> flag on the operation).
     * <p/>
     * Will <i>not</i> override the behavior of normal task submission by cancelling any outstanding
     * task for this pool.
     * <p/>
     * If indicated, bypasses the transition checking of pool status.
     * <p/>
     * Called after lock has been acquired.
     *
     * @param bypassStateCheck if false, will not bypass considerations of whether the pool has been
     *                         previously scanned because it went down.  NOTE: an excluded pool will
     *                         not be scanned under any circumstances until it is included.
     * @return true only if operation has been promoted from idle to waiting.
     */
    @GuardedBy("lock")
    private boolean doScan(String pool,
          String addedTo,
          String removedFrom,
          String storageUnit,
          PoolV2Mode mode,
          boolean bypassStateCheck) {
        LOGGER.debug("doScan called: (pool {})(addedTo {})(removedFrom {})"
                    + "(unit {})(mode {})(bypass check {})",
              pool, addedTo, removedFrom, storageUnit, mode, bypassStateCheck);

        if (running.containsKey(pool)) {
            LOGGER.debug("Scan of {} is already in progress", pool);
            return false;
        }

        PoolScanOperation operation;

        if (waiting.containsKey(pool)) {
            LOGGER.debug("Scan of {} is already in waiting state, setting its "
                  + "force flag to true.", pool);
            waiting.get(pool).forceScan = true;
            return false;
        }

        operation = idle.remove(pool);
        if (operation == null) {
            LOGGER.warn("No entry for {} in any queues; "
                  + "pool is not (yet) registered.", pool);
            return false;
        }

        if (operation.currStatus == PoolQoSStatus.UNINITIALIZED) {
            LOGGER.info("Cannot scan {} –– uninitialized", pool);
            reset(pool, operation);
            return false;
        }

        if (operation.state == State.EXCLUDED) {
            LOGGER.info("Skipping scan {} –– pool is excluded", pool);
            reset(pool, operation);
            return false;
        }

        if (!bypassStateCheck) {
            if (operation.currStatus == PoolQoSStatus.DOWN &&
                  operation.lastStatus == PoolQoSStatus.DOWN) {
                LOGGER.info("Skipping scan {} –– pool is down and was already "
                      + "scanned", pool);
                reset(pool, operation);
                return false;
            }
        }

        operation.getNextAction(PoolQoSStatus.valueOf(mode));
        operation.group = addedTo != null ? addedTo : removedFrom;
        operation.unit = storageUnit;
        operation.forceScan = true;
        operation.lastUpdate = System.currentTimeMillis();
        operation.state = State.WAITING;
        operation.exception = null;
        operation.resetFailed();
        operation.task = null;
        waiting.put(pool, operation);
        return true;
    }

    /**
     * @return operation, or <code>null</code> if not mapped.
     */
    @GuardedBy("lock")
    private PoolScanOperation get(String pool) {
        PoolScanOperation operation = running.get(pool);

        if (operation == null) {
            operation = waiting.get(pool);
        }

        if (operation == null) {
            operation = idle.get(pool);
        }

        return operation;
    }

    private Set<String> getMappedPools() {
        Set<String> allPools = new HashSet<>();
        allPools.addAll(idle.keySet());
        allPools.addAll(waiting.keySet());
        allPools.addAll(running.keySet());
        return allPools;
    }

    @GuardedBy("lock")
    private void reset(String pool, PoolScanOperation operation) {
        operation.lastUpdate = System.currentTimeMillis();
        operation.group = null;
        operation.unit = null;
        operation.forceScan = false;
        operation.resetChildren();
        if (currentPsu().getPool(pool) != null) {
            idle.put(pool, operation);
        } else if (operation.state == State.FAILED || operation.failedChildren() > 0) {
            String message = operation.exception == null ? "" : "exception: " +
                  new ExceptionMessage(operation.exception);
            LOGGER.error(AlarmMarkerFactory.getMarker(
                        PredefinedAlarm.FAILED_REPLICATION, pool),
                  "{} was removed but final scan {}; {} failed file operations.",
                  pool, message, operation.failedChildren());
        }
    }

    private void scanWaiting() {
        lock.lock();
        try {
            long now = System.currentTimeMillis();
            long downExpiry = downGracePeriodUnit.toMillis(downGracePeriod);
            long restartExpiry = restartGracePeriodUnit.toMillis(restartGracePeriod);

            for (Iterator<Entry<String, PoolScanOperation>> i = waiting.entrySet().iterator();
                  i.hasNext(); ) {
                Entry<String, PoolScanOperation> entry = i.next();
                String pool = entry.getKey();
                PoolScanOperation operation = entry.getValue();
                long expiry;

                switch (operation.currStatus) {
                    case DOWN:
                        expiry = downExpiry;
                        break;
                    default:
                        expiry = restartExpiry;
                        break;
                }

                /*
                 *  promote to running if force scan is true or
                 *  the grace period has expired; only promote
                 *  when a slot is available
                 */
                if ((operation.forceScan || now - operation.lastUpdate >= expiry)
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

    private void scanRunning() {
        lock.lock();
        try {
            long now = System.currentTimeMillis();
            long maxIdle = maxRunningIdleTimeUnit.toMillis(maxRunningIdleTime);
            List<Entry<String, PoolScanOperation>> toCancel = new ArrayList<>();

            running.entrySet().forEach(entry -> {
                String pool = entry.getKey();
                PoolScanOperation operation = entry.getValue();
                if (now - maxIdle >= operation.lastUpdate) {
                    LOGGER.warn(
                          "pool scan operation for {} was last updated more than {} {} ago; cancelling.",
                          pool, maxRunningIdleTime, maxRunningIdleTimeUnit);
                    toCancel.add(entry);
                }
            });

            toCancel.forEach(entry -> cancel(entry.getKey(), entry.getValue(), running));
        } finally {
            lock.unlock();
        }
    }

    @GuardedBy("lock")
    private void submit(String pool, PoolScanOperation operation) {
        operation.task = new PoolScanTask(pool,
              operation.currStatus.toMessageType(),
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

    @GuardedBy("lock")
    private void terminate(String pool, PoolScanOperation operation) {
        LOGGER.debug("terminate, pool {}, {}.", pool, operation);

        if (operation.exception != null) {
            operation.state = State.FAILED;
        } else {
            operation.state = State.IDLE;
        }

        long sinceLast = operation.lastUpdate - operation.lastScan;
        counters.increment(pool, operation.currStatus, operation.state == State.FAILED,
              operation.getCompleted(), operation.forceScan, sinceLast);

        if (running.containsKey(pool)) {
            running.remove(pool);
        } else {
            waiting.remove(pool);
        }

        operation.lastScan = operation.lastUpdate;

        reset(pool, operation);
    }

    @GuardedBy("lock")
    private void update(PoolMatcher filter,
          Map<String, PoolScanOperation> queue,
          boolean include,
          Set<String> visited) {
        ImmutableSet.copyOf(queue.keySet()).stream().forEach((k) -> {
            if (!visited.contains(k)) {
                PoolScanOperation operation = queue.get(k);
                if (filter.matches(k, operation)) {
                    if (!include) {
                        if (operation.task != null) {
                            operation.task.cancel("pool include/exclude admin command");
                        }
                        operation.state = State.EXCLUDED;
                        queue.remove(k);
                        reset(k, operation);
                        visited.add(k);
                        handler.handlePoolExclusion(k);
                    } else if (operation.state == State.EXCLUDED) {
                        operation.state = State.IDLE;
                        /*
                         * treat the operation as if emerging from an
                         * undefined state; allow user to decide
                         * whether to scan or not
                         */
                        operation.currStatus = PoolQoSStatus.UNINITIALIZED;
                        PoolV2Mode mode = currentPsu().getPool(k).getPoolMode();
                        updateStatus(k, PoolQoSStatus.valueOf(mode));
                        visited.add(k);
                        handler.handlePoolInclusion(k);
                    }
                }
            }
        });
    }
}