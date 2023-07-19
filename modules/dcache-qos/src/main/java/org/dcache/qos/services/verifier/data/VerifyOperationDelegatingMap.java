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
import static org.dcache.qos.services.verifier.data.VerifyOperationState.ABORTED;
import static org.dcache.qos.services.verifier.data.VerifyOperationState.CANCELED;
import static org.dcache.qos.services.verifier.data.VerifyOperationState.RUNNING;

import com.google.common.annotations.VisibleForTesting;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellInfoProvider;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.dcache.qos.data.FileQoSUpdate;
import org.dcache.qos.data.QoSAction;
import org.dcache.qos.services.verifier.handlers.VerifyAndUpdateHandler;
import org.dcache.qos.services.verifier.util.QoSVerifierCounters;
import org.dcache.qos.services.verifier.util.VerificationTask;
import org.dcache.qos.util.CacheExceptionUtils;
import org.dcache.qos.util.CacheExceptionUtils.FailureType;
import org.dcache.qos.util.QoSHistory;
import org.dcache.qos.vehicles.QoSAdjustmentRequest;
import org.dcache.util.RunnableModule;
import org.dcache.util.SignalAware;

/**
 * The main locus of operations for verification.</p>
 * <p>
 * <p/>
 * Tracks all operations on individual files by storing and retrieving instances of {@link
 * VerifyOperation}.
 * <p/>
 * When more than one event references a pnfsid which has a current entry which is active, only the
 * current entry's storage unit is updated. This is because when the current adjustment finishes,
 * another verification is always done.  When the file's requirements have been satisfied, the
 * operation is voided to enable removal.
 * <p/>
 * At every pass of the main thread, operations which have been terminated are post-processed. Then
 * new verification requests are launched from operations which are in the ready state. The number
 * of slots for running adjustments should not exceed the number of maxRunning tasks chosen for the
 * adjustment service.
 * <p/>
 * Fairness is defined here as the availability of a second replica for a given file. This means
 * that operations are processed FIFO, but those requiring more than one adjustment are reset with
 * their last updated time updated, so that they are naturally ordered after those having arrived
 * earlier.  Operations being retried for failure have their updated timestamp reset to their
 * arrival time, allowing them to be prioritized.
 * <p/>
 * The map distinguishes between three classes of requests, to which it dedicates three separate
 * queues:  (a) new cache locations and cancellations; (b) pool scan verifications; and (c) qos
 * modifications. A simple clock algorithm is used to select operations from them to run.
 * <p/>
 * Because of the potential for long-running staging operations to starve out the running queue,
 * these tasks are placed in the WAITING state once the adjustment request has been sent, thus
 * implicitly decreasing the number of operations in the RUNNING state and allowing for other READY
 * operations to be submitted.
 * <p/>
 * This version of the map uses a delegate to talk to the dao implementation.
 * <p/>
 * Class is not marked final for stubbing/mocking purposes.
 */
public class VerifyOperationDelegatingMap extends RunnableModule
      implements VerifyOperationMap, CellInfoProvider, SignalAware {

    private static final String COUNTS_FORMAT = "    %-24s %15s\n";

    private static final int DEFAULT_RELOAD_GRACE_PERIOD = 5;
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

    /**
     * First handles cancellations, then searches to see which adjustments have completed. All
     * terminated operations are passed to post-processing, which determines whether the operation
     * can be permanently removed or if needs to be re-queued.
     */
    class TerminalOperationProcessor {

        void processTerminated() {
            Collection<VerifyOperationCancelFilter> filters = new ArrayList<>();

            synchronized (cancelFilters) {
                filters.addAll(cancelFilters);
                cancelFilters.clear();
            }

            /*
             *  Updates STATE and VOIDs action if this is forced removal.
             */
            filters.forEach(delegate::cancel);

            /*
             *  Will also gather the canceled operations for purposes of finalization/
             *  notification.
             *
             *  In order not to overwhelm the processor with terminal task processing
             *  (in the case of mass cancellation, for instance), fetch up to batch size.
             *  (implemented here to use cache capacity).
             */
            Collection<VerifyOperation> toProcess = delegate.terminated();
            LOGGER.debug("Found {} terminated operations.", toProcess.size());

            toProcess.stream().forEach(this::postProcess);
            toProcess.clear();

            /*
             *  Post processing skips deletion for cancelled operations.
             *  That way the underlying call can be done more efficiently based
             *  on the filtering criteria.  This call should not delete operations
             *  not marked for removal.
             *
             *  Note that since the current implementation uses the cache capacity as fetch limit,
             *  which is >> maxRunning, and since the operations are ordered by timestamp,
             *  we may safely assume that by the time that post-processing finishes,
             *  any running operations that were cancelled will have been included and
             *  other components will have been notified about them.
             *
             *  Remove also applies a filter to remove cancelled operations from memory. Cancelled
             *  operations that have been voided are deleted in bulk from the data store.
             */
            filters.forEach(delegate::remove);
        }

        /**
         * Exceptions are analyzed to determine if any more work can be done. In the case of fatal
         * errors, an alarm is sent.  Operations that have not failed fatally (aborted) and are not
         * VOID are reset to READY; otherwise, the operation record will have been deactivated when
         * this method returns.
         */
        private void postProcess(VerifyOperation operation) {
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
                delegate.resetOperation(operation, retry);
            }
        }
    }

    /**
     * Fetches ready operations and submits them for task execution.
     */
    class ReadyOperationProcessor {

        void processReady() {
            int available = delegate.available();

            LOGGER.debug("Available to run: {}", available);

            while (available > 0) {
                VerifyOperation operation = delegate.next();
                if (operation == null) {
                    break;
                }
                submit(operation);
                --available;
            }

            delegate.refresh();
        }
    }

    /**
     * List of filters for cancelling operations.  Processing of cancellation is done during the
     * main consumer scan, as it has to be synchronous.
     */
    final Collection<VerifyOperationCancelFilter> cancelFilters = new ArrayList<>();

    /**
     * Encapsulates the logic for terminal operation processing.
     */
    final TerminalOperationProcessor terminalProcessor = new TerminalOperationProcessor();

    /**
     * Encapsulates the logic for ready operation processing.
     */
    final ReadyOperationProcessor readyProcessor = new ReadyOperationProcessor();

    /**
     * For reporting operations terminated or canceled while the consumer thread is doing work
     * outside the wait monitor.  Supports the SignalAware interface.
     */
    final AtomicInteger signalled = new AtomicInteger(0);

    /**
     * Limited access during terminal processing for finding new pools on operations which can be
     * retried.
     */
    private PoolInfoMap poolInfoMap;

    /**
     * Delegate.  Most operations defined by this interface are passed through.
     */
    private VerifyOperationDelegate delegate;

    /**
     * Callback to the handler.
     */
    private VerifyAndUpdateHandler updateHandler;

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
     * Writes statistics counts out one a minute.
     */
    private Thread statisticsCollector;

    private int reloadGracePeriod = DEFAULT_RELOAD_GRACE_PERIOD;

    private TimeUnit reloadGracePeriodUnit = DEFAULT_RELOAD_GRACE_PERIOD_UNIT;

    /**
     * Batch version of cancel.  In this case, the filter will indicate whether the operation should
     * be cancelled <i>in toto</i> or only the current verification. The actual scan is done on the
     * consumer thread.
     */
    public void cancel(VerifyOperationCancelFilter filter) {
        synchronized (cancelFilters) {
            cancelFilters.add(filter);
        }
        signal();
    }

    /**
     * Avoids pass through to delegate, which is done exclusively on consumer thread.
     */
    @Override
    public void cancel(PnfsId pnfsId, boolean remove) {
        VerifyOperationFilter filter = new VerifyOperationFilter();
        filter.setPnfsIds(pnfsId);
        cancel(new VerifyOperationCancelFilter(filter, remove));
    }

    /**
     * Avoids pass through to delegate, which is done exclusively on consumer thread.
     */
    @Override
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

    @Override
    public int count(VerifyOperationFilter filter) {
        return delegate.count(filter);
    }

    public int countSignals() {
        return signalled.get();
    }

    @Override
    public boolean createOrUpdateOperation(FileQoSUpdate data) {
        return delegate.createOrUpdateOperation(data);
    }

    @Override
    public VerifyOperation getRunning(PnfsId pnfsId) {
        return delegate.getRunning(pnfsId);
    }

    public void getInfo(PrintWriter pw) {
        pw.println(infoMessage());
    }

    public String infoMessage() {
        StringBuilder info = new StringBuilder();
        info.append(String.format("maximum concurrent operations %s\n"
                    + "maximum number of ready operations in memory %s\n"
                    + "maximum retries on failure %s\n\n",
              delegate.maxRunning(), delegate.capacity(), maxRetries));

        info.append(String.format("sweep interval %s %s\n\n", timeout, timeoutUnit));
        counters.appendCounts(info);
        info.append("\n");

        Map<String, Long> counts = delegate.aggregateCounts("state");

        info.append("\nACTIVE OPERATIONS BY STATE:\n");
        counts.entrySet()
              .forEach(e -> info.append(String.format(COUNTS_FORMAT, e.getKey(), e.getValue())));

        counts = delegate.aggregateCounts("msg_type");
        info.append("\nACTIVE OPERATIONS BY MSG_TYPE:\n");
        counts.entrySet()
              .forEach(e -> info.append(String.format(COUNTS_FORMAT, e.getKey(), e.getValue())));

        return info.toString();
    }

    public void initialize() {
        super.initialize();
        startStatisticsCollector();
    }

    public String list(VerifyOperationFilter filter, int limit) {
        return delegate.list(filter, limit);
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
        delegate.reload();
    }

    /**
     * The consumer thread. When notified or times out, checks the state of running operations and
     * submits waiting operations if there are open slots.  Removes completed operations.
     * <p/>
     * Note that since the scan takes place outside of the monitor, the signals sent by various
     * update methods will not be caught before the current thread is inside {@link #await}; for
     * this reason, a counter is used and reset to 0 before each scan. No wait occurs if the counter
     * is non-zero after the scan.
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
    }

    /**
     * Operation sweep: the main queue update sequence (run by the consumer).
     */
    @VisibleForTesting
    public void scan() {
        terminalProcessor.processTerminated();
        readyProcessor.processReady();
    }

    public void setCounters(QoSVerifierCounters counters) {
        this.counters = counters;
    }

    public void setCache(VerifyOperationDelegate delegate) {
        /*
         *  Set this here to avoid Spring circular dependency.
         */
        this.delegate = delegate.callback(this);
    }

    public void setHistory(QoSHistory history) {
        this.history = history;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public void setPoolInfoMap(PoolInfoMap poolInfoMap) {
        this.poolInfoMap = poolInfoMap;
    }

    public void setReloadGracePeriod(int reloadGracePeriod) {
        this.reloadGracePeriod = reloadGracePeriod;
    }

    public void setReloadGracePeriodUnit(TimeUnit reloadGracePeriodUnit) {
        this.reloadGracePeriodUnit = reloadGracePeriodUnit;
    }

    public void setUpdateHandler(VerifyAndUpdateHandler updateHandler) {
        this.updateHandler = updateHandler;
    }

    public void shutdown() {
        stopStatisticsCollector();
        super.shutdown();
    }

    /**
     * Supports the callback from the delegate.
     */
    public synchronized void signal() {
        signalled.incrementAndGet();
        notifyAll();
    }

    public int size() {
        return delegate.size();
    }

    public void updateOperation(PnfsId pnfsId, CacheException error) {
        delegate.updateOperation(pnfsId, error);
    }

    public void updateOperation(QoSAdjustmentRequest request) {
        delegate.updateOperation(request);
    }

    /**
     * Called after the handler has determined nothing needs to be done.
     */
    public void voidOperation(VerifyOperation operation) {
        delegate.voidOperation(operation);
    }

    @VisibleForTesting
    void submit(VerifyOperation operation) {
        delegate.updateOperation(operation, RUNNING);
        new VerificationTask(operation.getPnfsId(), updateHandler).submit();
    }

    private synchronized void await() throws InterruptedException {
        wait(timeoutUnit.toMillis(timeout));
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

    private void removeOperation(CompletedOperation completed) {
        LOGGER.debug("removeOperation processing {}.", completed.pnfsId);
        /*
         *  If this is a pool-bound operation (scan, viz. the 'parent' of the
         *  operation is defined), we notify the handler, which will
         *  relay to the scanning service when appropriate.  Note that
         *  for system scans, the pool/parent is the scan's UUID string.
         */
        String parent = completed.operation.getParent();
        if (parent != null) {
            updateHandler.updateScanRecord(parent, completed.aborted);
        }

        history.add(completed.pnfsId,
              completed.operation.toArchiveString(),
              completed.exception != null);

        /*
         *  For canceled operations, deletion from the database is done after
         *  this post-processing call.  The operations should already have
         *  been removed from the memory cache.
         */
        if (completed.state != CANCELED) {
            delegate.remove(completed.pnfsId);
        }

        updateHandler.handleQoSActionCompleted(completed.pnfsId,
              completed.state,
              completed.action,
              completed.exception);
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