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
package org.dcache.services.bulk.manager;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.dcache.services.bulk.BulkRequestStatus.STARTED;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkRequestStatus;
import org.dcache.services.bulk.BulkServiceException;
import org.dcache.services.bulk.BulkStorageException;
import org.dcache.services.bulk.handler.BulkRequestCompletionHandler;
import org.dcache.services.bulk.handler.BulkSubmissionHandler;
import org.dcache.services.bulk.job.AbstractRequestContainerJob;
import org.dcache.services.bulk.manager.scheduler.BulkRequestScheduler;
import org.dcache.services.bulk.manager.scheduler.BulkSchedulerProvider;
import org.dcache.services.bulk.store.BulkRequestStore;
import org.dcache.services.bulk.store.BulkTargetStore;
import org.dcache.services.bulk.util.BulkRequestTarget;
import org.dcache.services.bulk.util.BulkRequestTarget.State;
import org.dcache.services.bulk.util.BulkServiceStatistics;
import org.dcache.util.FireAndForgetTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * Initializes a processor thread to handle the promotion of requests from queued to active, and to
 * manage request termination and cancellation.
 * <p>
 * The request container job is held in memory for its entire lifecycle (there is a limit to the
 * number of active requests which can be run concurrently which is configurable).
 * <p>
 * Each container job is launched on the executor provided by its specific activity.
 */
public final class ConcurrentRequestManager implements BulkRequestManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentRequestManager.class);

    private static final int DEFAULT_MAX_ACTIVE_REQUESTS = 100;
    private static final long DEFAULT_PROCESSOR_TIMEOUT = 5;
    private static final TimeUnit DEFAULT_PROCESSOR_TIMEOUT_UNIT = SECONDS;

    class ConcurrentRequestProcessor implements Runnable {

        final BulkRequestScheduler scheduler;

        /*
         *  For reporting operations terminated or cancelled while the consumer thread is doing
         *  work outside the wait monitor.
         */
        AtomicInteger signals = new AtomicInteger(0);

        ConcurrentRequestProcessor(BulkRequestScheduler scheduler) {
            this.scheduler = scheduler;
        }

        @Override
        public void run() {
            try {
                while (!Thread.interrupted()) {
                    doRun();
                    await();
                }
            } catch (InterruptedException e) {
                LOGGER.warn("interrupted.");
            } finally {
                /*
                 *  Report exit even in the case of an uncaught exception.
                 */
                LOGGER.warn("ConcurrentRequestProcessor exiting...");
            }
        }

        public void signal() {
            signals.incrementAndGet();
            synchronized (this) {
                notifyAll();
            }
        }

        private void await() throws InterruptedException {
            if (signals.get() > 0) {
                LOGGER.trace("received {} signals; returning immediately for more work.",
                      signals.get());
                signals.set(0);
                return;
            }
            synchronized (this) {
                LOGGER.trace("waiting ...");
                wait(timeoutUnit.toMillis(timeout));
            }
        }

        private void broadcastTargetCancel() {
            List<String> toCancel;

            synchronized (cancelledJobs) {
                toCancel = cancelledJobs.stream().collect(Collectors.toList());
                cancelledJobs.clear();
            }

            synchronized (requestJobs) {
                toCancel.forEach(key -> {
                    String[] ridId = BulkRequestTarget.parse(key);
                    AbstractRequestContainerJob job = requestJobs.get(ridId[0]);
                    if (job != null) {
                        long id = Long.valueOf(ridId[1]);
                        job.cancel(id);
                        targetStore.cancel(id);
                    }
                });
            }
        }

        private void doRun() throws InterruptedException {
            LOGGER.trace("NextJobProcessor, starting doRun().");

            long start = System.currentTimeMillis();

            broadcastTargetCancel();

            if (Thread.interrupted()) {
                throw new InterruptedException();
            }

            removeTerminatedRequests();

            if (Thread.interrupted()) {
                throw new InterruptedException();
            }

            processNextRequests();

            if (Thread.interrupted()) {
                throw new InterruptedException();
            }

            statistics.sweepFinished(System.currentTimeMillis() - start);

            LOGGER.trace("doRun() completed");
        }

        @GuardedBy("requestJobs")
        private boolean isTerminated(String rid) {
            AbstractRequestContainerJob job = requestJobs.get(rid);
            if (job != null && job.getTarget().getState() == State.CANCELLED) {
                return completionHandler.checkTerminated(rid, true);
            }
            return completionHandler.checkTerminated(rid, false);
        }

        private void processNextRequests() {
            LOGGER.trace("processNextRequests()");

            try {
                List<BulkRequest> next = scheduler.processNextRequests(maxActiveRequests);
                if (next.isEmpty()) {
                    LOGGER.debug(
                          "processNextRequests(), no more QUEUED requests to process.");
                } else {
                    next.forEach(ConcurrentRequestManager.this::activateRequest);
                }
            } catch (BulkServiceException e) {
                LOGGER.error(
                      "processNextRequests(), problem retrieving queued requests: {}",
                      e.toString());
            }

            /*
             *   The activated requests have been stored, placed in the map and can now be
             *   immediately started.
             */
            synchronized (requestJobs) {
                requestJobs.values().stream().filter(AbstractRequestContainerJob::isReady)
                      .forEach(ConcurrentRequestManager.this::startJob);
            }
        }

        private void removeTerminatedRequests() {
            LOGGER.trace("removeTerminatedRequests");
            synchronized (requestJobs) {
                userRequests().values().stream().filter(this::isTerminated)
                      .forEach(requestJobs::remove);
            }
        }

        private ListMultimap<String, String> userRequests() {
            ListMultimap<String, String> requestsByUser;
            try {
                requestsByUser = requestStore.getActiveRequestsByUser();
            } catch (BulkStorageException e) {
                LOGGER.error("userRequests, could not get requests: {}.", e.toString());
                return ArrayListMultimap.create();
            }

            if (requestsByUser.isEmpty()) {
                LOGGER.trace("userRequests, no requests currently active.");
                return ArrayListMultimap.create();
            }

            LOGGER.trace("found next requests by user {}.", requestsByUser);

            return requestsByUser;
        }
    }

    /**
     * Used to fetch ready requests to run.
     */
    private BulkRequestStore requestStore;

    /**
     * Used to fetch ready targets to run, and to update state.
     */
    private BulkTargetStore targetStore;

    /**
     * Handles callback to check for request cancellation or termination.
     */
    private BulkRequestCompletionHandler completionHandler;

    /**
     * Handles the initial request activation.
     */
    private BulkSubmissionHandler submissionHandler;

    /**
     * Provides the implementation of the request scheduler
     */
    private BulkSchedulerProvider schedulerProvider;

    /**
     * Thread dedicated to running the processor.
     */
    private ExecutorService processorExecutorService;

    /**
     * Records number of jobs and requests processed.
     */
    private BulkServiceStatistics statistics;

    /**
     * Maximum time to wait until running the processing loop again.
     */
    private long timeout = DEFAULT_PROCESSOR_TIMEOUT;
    private TimeUnit timeoutUnit = DEFAULT_PROCESSOR_TIMEOUT_UNIT;

    /**
     * Serves as the upper limit on the number of requests that can be active.
     */
    private int maxActiveRequests = DEFAULT_MAX_ACTIVE_REQUESTS;

    /**
     * Held for the lifetime of the container run.
     */
    private Map<String, AbstractRequestContainerJob> requestJobs;

    /**
     * Ids of jobs cancelled individually. To avoid doing cancellation on the calling thread.
     */
    private Collection<String> cancelledJobs;

    /**
     * Handles the promotion of jobs to running.
     */
    private ConcurrentRequestProcessor processor;

    @Override
    public void initialize() throws Exception {
        requestJobs = new LinkedHashMap<>();
        cancelledJobs = new HashSet<>();
        schedulerProvider.initialize();
        processor = new ConcurrentRequestProcessor(schedulerProvider.getRequestScheduler());
        processorExecutorService.execute(processor);
    }

    @Override
    public void cancel(BulkRequestTarget target) {
        synchronized (cancelledJobs) {
            cancelledJobs.add(target.getKey());
        }
        processor.signal();
    }

    @Override
    public boolean cancelRequest(String requestId) {
        synchronized (requestJobs) {
            AbstractRequestContainerJob job = requestJobs.get(requestId);
            if (job != null) {
                job.cancel();
                processor.signal();
                LOGGER.debug("Request container job cancelled for {}.", requestId);
                return true;
            }
        }

        LOGGER.debug("Request {} cancelled, no container running.", requestId);
        return false;
    }

    @Override
    public void cancelTargets(String id, List<String> targetPaths) {
        synchronized (requestJobs) {
            AbstractRequestContainerJob job = requestJobs.get(id);
            if (job != null) {
                targetPaths.forEach(job::cancel);
                LOGGER.trace("{} request targets cancelled for {}.", targetPaths.size(), id);
            } else {
                LOGGER.debug(
                      "No currently running container for {} exists; target cancellation is void.",
                      id);
            }
        }
    }

    @Override
    public int countSignals() {
        return processor.signals.get();
    }

    @Override
     public int getMaxActiveRequests() {
        return maxActiveRequests;
    }

    @Required
    public void setCompletionHandler(BulkRequestCompletionHandler completionHandler) {
        this.completionHandler = completionHandler;
    }

    @Required
    public void setTargetStore(BulkTargetStore targetStore) {
        this.targetStore = targetStore;
    }

    @Required
    public void setMaxActiveRequests(int maxActiveRequests) {
        this.maxActiveRequests = maxActiveRequests;
    }

    @Required
    public void setProcessorExecutorService(ExecutorService processorExecutorService) {
        this.processorExecutorService = processorExecutorService;
    }

    @Required
    public void setRequestStore(BulkRequestStore requestStore) {
        this.requestStore = requestStore;
    }

    @Required
    public void setSchedulerProvider(BulkSchedulerProvider schedulerProvider) {
        this.schedulerProvider = schedulerProvider;
    }

    @Required
    public void setStatistics(BulkServiceStatistics statistics) {
        this.statistics = statistics;
    }

    @Required
    public void setSubmissionHandler(BulkSubmissionHandler submissionHandler) {
        this.submissionHandler = submissionHandler;
    }

    @Required
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    @Required
    public void setTimeoutUnit(TimeUnit timeoutUnit) {
        this.timeoutUnit = timeoutUnit;
    }

    /**
     * For external calls (from the service, request handler and admin interface).
     */
    @Override
    public void signal() {
        /*
         *  Wake up the job processor.
         */
        processor.signal();
    }

    @Override
    public void submit(AbstractRequestContainerJob job) {
        synchronized (requestJobs) {
            requestJobs.put(job.getTarget().getRuid(), job);
        }

        processor.signal();
    }

    /*
     *   The submission handler creates the job and stores it as a target, then calls
     *   submit() above.
     */
    void activateRequest(BulkRequest request) {
        LOGGER.trace("activateRequest {}.", request.getUid());
        try {
            submissionHandler.submitRequestJob(request);
            LOGGER.debug("activateRequest, updating {} to STARTED.", request.getUid());
            requestStore.update(request.getUid(), STARTED);
        } catch (BulkStorageException e) {
            LOGGER.error(
                  "Unrecoverable storage update error for {}: {}; aborting.",
                  request.getUid(),
                  e.toString());
            requestStore.abort(request, e);
        } catch (BulkServiceException e) {
            LOGGER.error(
                  "Problem activating request for {}: {}; aborting.", request.getUid(),
                  e.toString());
            requestStore.abort(request, e);
        } catch (RuntimeException e) {
            requestStore.abort(request, e);
            Thread thisThread = Thread.currentThread();
            UncaughtExceptionHandler ueh = thisThread.getUncaughtExceptionHandler();
            ueh.uncaughtException(thisThread, e);
        }
    }

    void startJob(AbstractRequestContainerJob job) {
        String key = job.getTarget().getKey();
        long id = job.getTarget().getId();
        LOGGER.trace("submitting job {} to executor, target {}.", key,
              job.getTarget());
        job.setCallback(this);
        try {
            if (isJobValid(job)) { /* possibly cancelled in flight */
                job.update(State.RUNNING);
                job.getActivity().getActivityExecutor().submit(new FireAndForgetTask(job));
            }
        } catch (RuntimeException e) {
            job.getTarget().setErrorObject(e);
            job.cancel();
            Thread thisThread = Thread.currentThread();
            UncaughtExceptionHandler ueh = thisThread.getUncaughtExceptionHandler();
            ueh.uncaughtException(thisThread, e);
        }
    }

    /*
     *  This is here mostly in order to catch jobs which have changed state on the fly
     *  during cancellation. It is only called by the processor thread (when it invokes startJob).
     */
    private boolean isJobValid(AbstractRequestContainerJob job) {
        BulkRequestTarget target = job.getTarget();

        if (target.isTerminated()) {
            return false;
        }

        Optional<BulkRequestStatus> status;
        try {
            status = requestStore.getRequestStatus(target.getRuid());
        } catch (BulkStorageException e) {
            LOGGER.error("isJobValid {}: {}", target, e.toString());
            return false;
        }

        if (status.isPresent()) {
            switch (status.get()) {
                case CANCELLING:
                case CANCELLED:
                    job.update(State.CANCELLED);
                    try {
                        targetStore.update(target.getId(), State.CANCELLED, target.getThrowable());
                    } catch (BulkStorageException e) {
                        LOGGER.error("updateJobState", e.toString());
                    }
                    return false;
            }
        }

        return true;
    }
}
