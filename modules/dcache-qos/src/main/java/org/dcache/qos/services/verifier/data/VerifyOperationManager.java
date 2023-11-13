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
package org.dcache.qos.services.verifier.data;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.dcache.qos.data.QoSAction.VOID;
import static org.dcache.qos.data.QoSAction.WAIT_FOR_STAGE;
import static org.dcache.qos.data.QoSMessageType.POOL_STATUS_DOWN;
import static org.dcache.qos.data.QoSMessageType.POOL_STATUS_UP;
import static org.dcache.qos.data.QoSMessageType.SYSTEM_SCAN;
import static org.dcache.qos.services.verifier.data.VerifyOperationState.ABORTED;
import static org.dcache.qos.services.verifier.data.VerifyOperationState.CANCELED;
import static org.dcache.qos.services.verifier.data.VerifyOperationState.READY;
import static org.dcache.qos.services.verifier.data.VerifyOperationState.RUNNING;

import com.google.common.base.Throwables;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellInfoProvider;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.dcache.qos.QoSException;
import org.dcache.qos.data.FileQoSUpdate;
import org.dcache.qos.data.QoSAction;
import org.dcache.qos.data.QoSMessageType;
import org.dcache.qos.services.verifier.data.db.VerifyOperationDao;
import org.dcache.qos.services.verifier.data.db.VerifyOperationDao.VerifyOperationCriterion;
import org.dcache.qos.services.verifier.handlers.VerifyAndUpdateHandler;
import org.dcache.qos.services.verifier.util.QoSVerifierCounters;
import org.dcache.qos.services.verifier.util.VerificationTask;
import org.dcache.qos.util.CacheExceptionUtils;
import org.dcache.qos.util.CacheExceptionUtils.FailureType;
import org.dcache.qos.util.QoSHistory;
import org.dcache.qos.vehicles.QoSAdjustmentRequest;
import org.dcache.util.RunnableModule;
import org.dcache.util.SignalAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 *  Responsible for the entire lifecycle of a given verification operation.
 *  Is called by the handler to create an operation entry and to update the
 *  status of running operations.  Is called back by the queues when submitting
 *  a task to run and also to be post-processed.
 *  <p/>
 *  The manager initializes the queues and submits ready operations to them
 *  on the basis of message types.
 *  <p/>
 *  The run() method runs post-processing, operation removal or resetting, and
 *  statistics updates either in response to an update signal or periodically
 *  in the absence of signals.
 *  <p/>
 *  Also provides methods for listing the current operations and for cancellation
 *  of operations.
 *  <p/>
 *  When more than one event references a pnfsid which has a current entry which is active, only the
 *  current entry's storage unit is updated. This is because when the current adjustment finishes,
 *  another verification is always done.  When the file's requirements have been satisfied, the
 *  operation is voided to enable removal.
 *  <p/>
 *  Fairness is defined here as the availability of a second replica for a given file. This means
 *  that operations are processed FIFO, but those requiring more than one adjustment are reset and
 *  added to the end of the queues.  Operations being retried for failure are pushed onto the
 *  front of the queues.
 *  <p/>
 *  Because of the potential for long-running staging operations to starve out the running queue,
 *  these tasks are placed in the WAITING state once the adjustment request has been sent, thus
 *  implicitly decreasing the number of operations in the RUNNING state and allowing for other READY
 *  operations to be submitted.  The queues have separated lists for these three states.
 */
public class VerifyOperationManager extends RunnableModule implements CellInfoProvider,
      SignalAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(VerifyOperationManager.class);

    private static final String COUNTS_FORMAT = "    %-24s %15s\n";
    private static final int DEFAULT_RELOAD_GRACE_PERIOD = 2;
    private static final TimeUnit DEFAULT_RELOAD_GRACE_PERIOD_UNIT = MINUTES;

    /*
     * Placeholder for information to be sent via messaging when the operation is removed.
     */
    class CompletedOperation {
        final PnfsId pnfsId;
        final VerifyOperationState state;
        final QoSAction action;
        final CacheException exception;

        VerifyOperation operation;
        boolean aborted;

        CompletedOperation(PnfsId pnfsId,
              VerifyOperationState state,
              QoSAction action,
              CacheException exception) {
            this.pnfsId = pnfsId;
            this.state = state;
            this.action = action;
            this.exception = exception;
        }
    }

    abstract class OperationProcessor {
        final List<PnfsId> queue = new ArrayList<>();

        void add(PnfsId pnfsId) {
            synchronized (queue) {
                queue.add(pnfsId);
            }
        }

        abstract void process();
    }

    /*
     *  Handles database cleanup.  Executes a batch delete when the queue
     *  fills to maxRunning size or greater. Shutdown empties any remaining
     *  operations in the queue.
     */
    class OperationReaper extends OperationProcessor {

        void process() {
            LOGGER.trace("Running reaper.");
            List<PnfsId> tmp;

            synchronized (queue) {
                if (queue.size() < maxRunning) {
                    return;
                }
                LOGGER.trace("Reaper deleting {} operations.", queue.size());
                tmp = queue.stream().collect(Collectors.toList());
                queue.clear();
            }

            removalExecutor.submit(()->operationDao.deleteBatch(tmp, maxRunning));
        }
    }

    /*
     *  Handles the logic of the terminal operation state; either resubmits for further
     *  verification or VOIDs and removes the operation.
     */
    class OperationPostProcessor  extends OperationProcessor {
        final List<PnfsId> queue = new ArrayList<>();

        void add(PnfsId pnfsId) {
            synchronized (queue) {
                queue.add(pnfsId);
            }
        }

        void process() {
            LOGGER.trace("Running postprocessor.");

            List<PnfsId> tmp;

            synchronized (queue) {
                tmp = queue.stream().collect(Collectors.toList());
                queue.clear();
            }

            tmp.forEach(pnfsId->postProcessExecutor.submit(()->postProcess(pnfsId)));
        }

        private void postProcess(PnfsId id) {
            LOGGER.trace("postProcess {}.", id);

            VerifyOperation operation = get(id);
            if (operation == null) {
                LOGGER.info("post processor: no operation found for {}", id);
                return;
            }

            String pool = operation.getPrincipalPool();
            String source = operation.getSource();
            String target = operation.getTarget();

            boolean retry = false;
            VerifyOperationState opState = operation.getState();
            CompletedOperation completedOperation = null;

            switch (opState) {
                case FAILED:
                    FailureType type =
                          CacheExceptionUtils.getFailureType(operation.getException(),
                                operation.getAction());
                    switch (type) {
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
                             *  or the target is bad.  The best we can do is
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
                            String pgroup = operation.getPoolGroup();
                            int groupMembers
                                  = poolInfoMap.getMemberPools(pgroup, false).size();

                            if (groupMembers > operation.getTried().size()) {
                                operation.resetSourceAndTarget();
                                retry = true;
                                break;
                            }

                            /**
                             * fall through; no more possibilities left
                             */
                        case FATAL:
                            operation.addTargetToTriedLocations();
                            if (source != null) {
                                operation.addSourceToTriedLocations();
                            }

                            Set<String> tried = operation.getTried();
                            updateHandler.operationAborted(operation, pool, tried, maxRetries);

                            /*
                             * Only fatal errors count as operation failures.
                             */
                            counters.incrementFailed(pool);

                            /*
                             * Abort.
                             */
                            operation.abortOperation();
                            completedOperation = new CompletedOperation(operation.getPnfsId(),
                                  operation.getState(), // updated from abort
                                  operation.getAction(),
                                  operation.getException());
                            break;
                        default:
                            throw new IllegalStateException(
                                  String.format("%s: No such failure type: "
                                        + "%s: this is a bug.", operation.getPnfsId(), type));
                    }
                    break;
                case DONE:
                    /*
                     *  Only count DONE, not CANCELED
                     */
                    counters.increment(source, target, operation.getAction());

                    /*
                     *  fall through to notify and reset
                     */
                case CANCELED:
                    QoSAction action = operation.getAction();
                    if (action == VOID) {
                        action = operation.getPreviousAction();
                    }
                    completedOperation = new CompletedOperation(operation.getPnfsId(),
                          opState,
                          action,
                          operation.getException());
                    operation.setSource(null);
                    operation.setTarget(null);
            }

            /*
             *  If the operation has been aborted here, or marked VOID during verification,
             *  it is removed from the underlying store.
             */
            boolean abort = operation.getState() == ABORTED;
            if (abort || (!retry && operation.getAction() == VOID)) {
                completedOperation.operation = operation; // now possibly reset
                completedOperation.aborted = abort;
                removeOperation(completedOperation);
            } else {
                resetOperation(operation, retry);
            }
        }
    }

    /**
     *  Holds the operation in memory for its full lifetime.
     */
    private final Map<PnfsId, VerifyOperation> operationMap = new ConcurrentHashMap<>();

    /**
     *  Handles operation post-processing (either elimination of VOIDed operations or
     *  resetting for retry or additional requirements).
     */
    private final OperationPostProcessor postProcessor = new OperationPostProcessor();

    /**
     *  Handles operation batch deletion from database.
     */
    private final OperationReaper reaper = new OperationReaper();

    /**
     * For reporting operations terminated or canceled while the consumer thread is doing work
     * outside the wait monitor.  Supports the SignalAware interface.
     */
    private final AtomicInteger signalled = new AtomicInteger(0);

    /**
     *  Configurable set of message-to-queue mappings; starts and stops queues with
     *  internal executor.
     */
    private VerifyOperationQueueIndex queueIndex;

    /**
     * Limited access during terminal processing for finding new pools on operations which can be
     * retried.
     */
    private PoolInfoMap poolInfoMap;

    /**
     *  Persistent storage for recovery.
     */
    private VerifyOperationDao operationDao;

    /**
     * Callback to the handler.
     */
    private VerifyAndUpdateHandler updateHandler;

    /**
     * Thread pool to do postprocessing.
     */
    private ExecutorService postProcessExecutor;

    /**
     * Singleton executor to do entry removal from the database.
     */
    private ExecutorService removalExecutor;

    /**
     * Writes statistics counts out once a minute.
     */
    private Thread statisticsCollector;

    /*
     *  This is interpreted as per queue.
     */
    private int maxRunning = 200;

    /**
     * Meaning for a given source-target pair. When a source or target is changed, the retry count
     * is reset to 0.
     */
    private int maxRetries = 1;

    /**
     * Statistics collection.
     */
    private QoSVerifierCounters counters;

    /**
     * Circular buffer of recently completed operation summaries.
     */
    private QoSHistory history;

    /**
     *  So as not to generate a lot of timeouts or route not found on startup.
     */
    private int reloadGracePeriod = DEFAULT_RELOAD_GRACE_PERIOD;
    private TimeUnit reloadGracePeriodUnit = DEFAULT_RELOAD_GRACE_PERIOD_UNIT;

    public void cancel(VerifyOperationCancelFilter filter) {
        VerifyOperationCriterion criterion = filter.getCriterion(operationDao);
        if (criterion.isEmpty()) {
            LOGGER.error("Cannot use an empty filter to cancel operations.");
            return;
        }

        /*
         *  First, mark all the mapped operations that match.
         *  Once marked, the terminal check should guarantee they are removed from the queue lists.
         */
        operationMap.values().stream().filter(filter.getPredicate())
              .forEach(op -> op.setState(CANCELED));

        queueIndex.signalAll();

        /*
         * The queues do not access the store, so we can go ahead and delete here
         * if indicated.
         */
        if (filter.shouldRemove()) {
            operationDao.delete(filter.getCriterion(operationDao));
        }
    }

    public void cancel(PnfsId pnfsId, boolean remove) {
        VerifyOperationFilter filter = new VerifyOperationFilter();
        filter.setPnfsIds(pnfsId);
        cancel(new VerifyOperationCancelFilter(filter, remove));

    }

    public void cancelFileOpForPool(String pool, boolean onlyParent) {
        VerifyOperationFilter filter = new VerifyOperationFilter();
        filter.setParent(pool);
        cancel(new VerifyOperationCancelFilter(filter, true));
        if (!onlyParent) {
            filter = new VerifyOperationFilter();
            filter.setSource(pool);
            cancel(new VerifyOperationCancelFilter(filter, true));
            filter = new VerifyOperationFilter();
            filter.setTarget(pool);
            cancel(new VerifyOperationCancelFilter(filter, true));
        }
    }

    public int count(VerifyOperationFilter filter) {
        return (int) operationMap.values().stream().filter(filter.toPredicate()).count();
    }

    @Override
    public int countSignals() {
        return signalled.get();
    }

    /*
     *  Creates the operation or updates it if already present.
     *
     *  Only messages conveying system-wide changes to or additions of files are
     *  stored in the database.
     *
     *  If a duplicate request arrives with a second subject, the original subject
     *  is not changed.
     */
    public boolean createOrUpdateOperation(FileQoSUpdate data) {
        PnfsId pnfsId = data.getPnfsId();
        String storageUnit = data.getStorageUnit();
        QoSMessageType type = data.getMessageType();

        VerifyOperation operation = get(pnfsId);

        if (operation != null) {
            /*
             *  If the message type is now a pool status type
             *  and the current type is a system scan, promote it to the more urgent type.
             *
             *  These message types are not stored, so there is no need
             *  to do a dao update.
             */
            if (operation.getMessageType() == SYSTEM_SCAN &&
                  (type == POOL_STATUS_DOWN || type == POOL_STATUS_UP)) {
                operation.setMessageType(type);
            }

            /*
             *  Storage unit should always be prioritized.
             */
            operation.setStorageUnit(storageUnit);
            LOGGER.debug("createOrUpdateOperation, updated operation for {}, sunit {}.",
                  pnfsId, storageUnit);
            return false;
        }

        boolean isParent =
              type == POOL_STATUS_DOWN || type == POOL_STATUS_UP || type == SYSTEM_SCAN;
        String pool = data.getPool();
        String parent = null;
        String source = null;

        if (isParent) {
            parent = pool;
        } else {
            source = pool;
        }

        long now = System.currentTimeMillis();

        operation = new VerifyOperation(pnfsId);
        operation.setArrived(now);
        operation.setLastUpdate(now);
        operation.setMessageType(type);
        operation.setStorageUnit(storageUnit);
        operation.setParent(parent);
        operation.setSource(source);
        operation.setRetried(0);
        operation.setNeeded(0);
        operation.setState(READY);

        switch (type) {
            case POOL_STATUS_DOWN:
            case POOL_STATUS_UP:
            case SYSTEM_SCAN:
                LOGGER.debug("createOrUpdateOperation {}, {}: in-memory processing only.",
                      pnfsId, type);
                break;
            default:
                try {
                    if (!operationDao.store(operation)) {
                        LOGGER.debug("createOrUpdateOperation, a similar operation is already "
                                    + "in the store but not in memory: {}, {}.", pnfsId, type);
                    } else {
                        LOGGER.debug("createOrUpdateOperation, stored operation for {}, {}.",
                              pnfsId, type);
                    }
                } catch (QoSException e) {
                    LOGGER.error(
                          "createOrUpdateOperation, could not store operation for {}, {}: {}.",
                          pnfsId, type, e.toString());
                    return false;
                }
        }

        put(operation);
        addLast(operation);
        signal();

        return true;
    }

    public void getInfo(PrintWriter pw) {
        pw.println(infoMessage());
    }

    public String infoMessage() {
        StringBuilder info = new StringBuilder();
        info.append(String.format("maximum concurrent operations per queue: %s\n"
                    + "maximum retries on failure: %s\n\n",
              maxRunning, maxRetries));

        info.append(String.format("sweep interval: %s %s\n\n", timeout, timeoutUnit));
        counters.appendCounts(info);
        info.append("\n");

        Map<String, Long> counts = getCountsByState();

        info.append("\nCURRENT OPERATIONS BY STATE:\n");
        counts.entrySet()
              .forEach(e -> info.append(String.format(COUNTS_FORMAT, e.getKey(), e.getValue())));

        counts = getCountsByMessageType();
        info.append("\nCURRENT OPERATIONS BY MSG_TYPE:\n");
        counts.entrySet()
              .forEach(e -> info.append(String.format(COUNTS_FORMAT, e.getKey(), e.getValue())));

        return info.toString();
    }

    @Override
    public void initialize() {
        queueIndex.configure(this);
        super.initialize();
        startStatisticsCollector();
    }

    public VerifyOperation get(PnfsId pnfsId) {
        return operationMap.get(pnfsId);
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public int getMaxRunning() {
        return maxRunning;
    }

    public VerifyOperation getRunning(PnfsId pnfsId) {
        VerifyOperation op = get(pnfsId);
        if (op == null) {
            return null;
        }
        return queueIndex.getQueue(op.getMessageType()).isRunning(pnfsId) ? op : null;
    }

    public String list(VerifyOperationFilter filter, int limit) {
        return operationMap.values().stream().filter(filter.toPredicate())
                  .map(o -> o.toString()).limit(limit)
                  .collect(Collectors.joining("\n")).toString();
    }

    public void reload() {
        synchronized (this) {
            try {
                /*
                 *  Give the pools a chance to register/come on line.
                 */
                wait(reloadGracePeriodUnit.toMillis(reloadGracePeriod));
            } catch (InterruptedException e) {
                LOGGER.debug("reload wait interrupted.");
                return;
            }
        }

        try {
            List<VerifyOperation> fromStore = operationDao.load();
            for (VerifyOperation op : fromStore) {
                    PnfsId pnfsId = op.getPnfsId();
                    operationMap.put(pnfsId, op);
                    queueIndex.getQueue(op.getMessageType()).addLast(pnfsId);
            }
        } catch (QoSException e) {
            LOGGER.error("problem reloading operations from datastore: {}, {}.",
                  e.getMessage(), String.valueOf(Throwables.getRootCause(e)));
        } finally {
            /*
             *  Delay queue start to here, to avoid trying to postprocess operations
             *  not yet known to the manager.
             */
            queueIndex.startQueues();
        }
    }

    @Override
    public void run() {
        try {
            while (!Thread.interrupted()) {
                signalled.set(0);

                postProcessor.process();
                reaper.process();

                LOGGER.trace("Signalling queues.");
                queueIndex.signalAll();

                if (Thread.interrupted()) {
                    break;
                }

                if (signalled.get() > 0) {
                    /*
                     *  Give the operations completed during the scan a chance
                     *  to turn over immediately, if possible, by rerunning now.
                     */
                    LOGGER.trace("sweep complete, received {} signals; "
                                + "sweeping again ...", signalled.get());
                    continue;
                }

                LOGGER.trace("Queues signalled, waiting ...");
                await();
            }
        } catch (InterruptedException e) {
            LOGGER.trace("Manager was interrupted.");
        }

        LOGGER.info("Exiting manager thread.");
    }

    @Required
    public void setCounters(QoSVerifierCounters counters) {
        this.counters = counters;
    }

    @Required
    public void setDao(VerifyOperationDao operationDao) {
        this.operationDao = operationDao;
    }

    @Required
    public void setHandler(VerifyAndUpdateHandler updateHandler) {
        this.updateHandler = updateHandler;
    }

    @Required
    public void setHistory(QoSHistory history) {
        this.history = history;
    }

    @Required
    public void setMaxRunning(int maxRunning) {
        this.maxRunning = maxRunning;
    }

    @Required
    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    @Required
    public void setPoolInfoMap(PoolInfoMap poolInfoMap) {
        this.poolInfoMap = poolInfoMap;
    }

    @Required
    public void setPostProcessExecutor(ExecutorService postProcessExecutor) {
        this.postProcessExecutor = postProcessExecutor;
    }

    @Required
    public void setQueueIndex(VerifyOperationQueueIndex queueIndex) {
        this.queueIndex = queueIndex;
    }

    @Required
    public void setReloadGracePeriod(int reloadGracePeriod) {
        this.reloadGracePeriod = reloadGracePeriod;
    }

    @Required
    public void setReloadGracePeriodUnit(TimeUnit reloadGracePeriodUnit) {
        this.reloadGracePeriodUnit = reloadGracePeriodUnit;
    }

    @Required
    public void setRemovalExecutor(ExecutorService removalExecutor) {
        this.removalExecutor = removalExecutor;
    }

    @Override
    public void shutdown() {
        queueIndex.stopQueues();
        stopStatisticsCollector();
        operationDao.deleteBatch(reaper.queue, maxRunning);
        super.shutdown();
    }

    @Override
    public synchronized void signal() {
        signalled.incrementAndGet();
        notifyAll();
    }

    public int size() {
        return operationMap.size();
    }

    public void submitToRun(PnfsId pnfsId, ExecutorService executor) {
        VerifyOperation operation = get(pnfsId);
        if (operation == null) {
            LOGGER.error("no operation currently mapped for {}.", pnfsId);
            return;
        }
        operation.setState(RUNNING);
        new VerificationTask(operation.getPnfsId(), updateHandler, executor).submit();
    }

    public void submitForPostProcessing(PnfsId id) {
        postProcessor.add(id);
        signal();
    }

    public void updateOperation(PnfsId pnfsId, CacheException error) {
        VerifyOperation operation = get(pnfsId);
        if (operation == null) {
            /*
             *  It is possible that a cancellation could remove an operation which is
             *  in return flight to the verifier, so we just treat this benignly.
             */
            LOGGER.info("{} was no longer present when updated.", pnfsId);
            return;
        }

        /*
         *  Operation is updated in memory, and left in the queue's running list
         *  until the terminal processing thread removes it.
         */
        if (operation.updateOperation(error)) {
            LOGGER.debug("updated operation {}.", operation);
            queueIndex.getQueue(operation.getMessageType()).signal();
        }
    }

    /*
     * Update done by handler when it has finished determining the adjustment.
     * Called after the operation held in memory has been updated.
     */
    public void updateOperation(QoSAdjustmentRequest request) {
        if (request.getAction() == WAIT_FOR_STAGE) {
            PnfsId pnfsId = request.getPnfsId();
            VerifyOperation op = get(pnfsId);
            queueIndex.getQueue(op.getMessageType()).updateToWaiting(pnfsId);
        }
    }

    public void updateVoided(VerifyOperation operation) {
        operation.voidOperation();
        queueIndex.getQueue(operation.getMessageType()).signal();
    }

    private synchronized void await() throws InterruptedException {
        wait(timeoutUnit.toMillis(timeout));
    }

    private void addFirst(VerifyOperation operation) {
        queueIndex.getQueue(operation.getMessageType()).addFirst(operation.getPnfsId());
    }

    private void addLast(VerifyOperation operation) {
        queueIndex.getQueue(operation.getMessageType()).addLast(operation.getPnfsId());
    }

    private Map<String, Long> getCountsByState() {
        Map<String, Long> counts = new HashMap<>();
        for (VerifyOperationState state : VerifyOperationState.values()) {
            long count = operationMap.values().stream().filter(o -> o.getState() == state).count();
            if (count > 0) {
                counts.put(state.name(), count);
            }
        }
        return counts;
    }

    private Map<String, Long> getCountsByMessageType() {
        Map<String, Long> counts = new HashMap<>();
        for (QoSMessageType type : QoSMessageType.values()) {
            long count = operationMap.values().stream().filter(o -> o.getMessageType() == type)
                  .count();
            if (count > 0) {
                counts.put(type.name(), count);
            }
        }
        return counts;
    }

    private void put(VerifyOperation operation) {
        operationMap.put(operation.getPnfsId(), operation);
    }

    private void recordStatistics() {
        long interval = timeoutUnit.toMillis(timeout);
        long toWait = interval;
        long elapsed;

        while (!Thread.interrupted()) {
            long start = System.currentTimeMillis();
            synchronized (this) {
                try {
                    wait(toWait);
                } catch (InterruptedException e) {
                    LOGGER.debug("recordStatistics wait was interrupted; exiting.");
                    break;
                }
            }

            long end = System.currentTimeMillis();
            elapsed = end - start;

            if (elapsed < interval) {
                toWait = interval - elapsed;
                continue;
            } else {
                toWait = interval;
            }

            LOGGER.debug("recordStatistics calling recordSweep.");
            counters.recordSweep(end, elapsed);
        }
    }

    private void remove(PnfsId pnfsId) {
        operationMap.remove(pnfsId);
    }

    private void removeOperation(CompletedOperation completed) {
        LOGGER.debug("removeOperation processing {}.", completed.pnfsId);

        /*
         *  If this is a bound operation (scan, viz. the 'parent' of the
         *  operation is defined), we notify the handler, which will
         *  relay to the scanning service when appropriate.  Note that
         *  for system scans, the pool/parent is the scan's UUID string.
         */
        String parent = completed.operation.getParent();
        if (parent != null) {
            updateHandler.updateScanRecord(parent, completed.aborted);
        }

        history.add(completed.pnfsId, completed.operation.toArchiveString(),
              completed.exception != null);

        /*
         *  For canceled operations, deletion from the database is done after
         *  the post-processing run.  The operations should already have
         *  been removed from the queue lists.
         */
        remove(completed.pnfsId);

        switch (completed.operation.getMessageType()) {
            case SYSTEM_SCAN:
            case POOL_STATUS_DOWN:
            case POOL_STATUS_UP:
                break;
            default:
                reaper.add(completed.pnfsId);
        }

        updateHandler.handleQoSActionCompleted(completed.pnfsId, completed.state, completed.action,
              completed.exception);
    }

    /*
     *  Update done when there is more work, or a failure which can be retried occurs.
     *  The operation has already been removed from the running list of its queue
     *  by the time of this call.
     */
    private void resetOperation(VerifyOperation operation, boolean retry) {
        LOGGER.debug("resetOperation {}, {}.", operation, retry);
        operation.resetOperation(retry);
        LOGGER.debug("resetOperation, after reset {}.", operation);
        if (retry || operation.getNeededAdjustments() < 2) {
            addFirst(operation);
        } else {
            addLast(operation);
        }
    }

    private void startStatisticsCollector() {
        LOGGER.info("starting statistics collector");
        statisticsCollector = new Thread(this::recordStatistics);
        statisticsCollector.start();
    }

    private void stopStatisticsCollector() {
        statisticsCollector.interrupt();
    }
}
