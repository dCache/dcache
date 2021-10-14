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
package org.dcache.services.bulk.handlers;

import static org.dcache.services.bulk.BulkRequestStatus.Status.CANCELLED;
import static org.dcache.services.bulk.BulkRequestStatus.Status.COMPLETED;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import javax.annotation.concurrent.GuardedBy;
import javax.security.auth.Subject;
import org.dcache.auth.attributes.Restriction;
import org.dcache.services.bulk.BulkJobStorageException;
import org.dcache.services.bulk.BulkPermissionDeniedException;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkRequestNotFoundException;
import org.dcache.services.bulk.BulkRequestStatus;
import org.dcache.services.bulk.BulkRequestStatus.Status;
import org.dcache.services.bulk.BulkServiceException;
import org.dcache.services.bulk.BulkStorageException;
import org.dcache.services.bulk.job.BulkJob;
import org.dcache.services.bulk.job.BulkJob.State;
import org.dcache.services.bulk.job.BulkJobFactory;
import org.dcache.services.bulk.job.BulkJobKey;
import org.dcache.services.bulk.job.BulkRequestJob;
import org.dcache.services.bulk.job.MultipleTargetJob;
import org.dcache.services.bulk.job.SingleTargetJob;
import org.dcache.services.bulk.job.TargetExpansionJob;
import org.dcache.services.bulk.queue.BulkServiceQueue;
import org.dcache.services.bulk.store.BulkJobStore;
import org.dcache.services.bulk.store.BulkRequestStore;
import org.dcache.services.bulk.util.BulkServiceStatistics;
import org.dcache.util.list.ListDirectoryHandler;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * The task of this handler is to take the appropriate action at the various phases in the lifetime
 * of a given request.
 * <p>
 * This includes submitting and handling the completion of individual target jobs.
 * <p>
 * It interacts with the queue, the job factory and the stores.
 * <p>
 * It receives callbacks from the queue upon job and request termination.
 */
public class BulkRequestHandler implements BulkSubmissionHandler, BulkRequestCompletionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(BulkRequestHandler.class);
    private static final String PREMATURE_CLEAR_ERROR
          = "Request cannot be cleared until all jobs have terminated; "
          + "try cancelling the request first.";

    private ListDirectoryHandler listHandler;
    private BulkServiceQueue queue;
    private BulkJobStore jobStore;
    private BulkRequestStore requestStore;
    private BulkJobFactory jobFactory;
    private BulkServiceStatistics statistics;
    private ExecutorService callbackExecutorService;

    /**
     * Caused by an internal issue.
     * <p>
     * Essentially a premature failure.
     */
    @Override
    public synchronized void abortRequestTarget(String requestId, String target,
          Throwable exception)
          throws BulkServiceException {
        LOGGER.trace("requestTargetAborted {}, {}, {}; calling abort on request store",
              requestId, target, exception.toString());

        requestStore.targetAborted(requestId, target, exception);
        queue.signal();

        statistics.incrementJobsAborted();
    }

    @Override
    public synchronized void cancelRequest(Subject subject, String requestId)
          throws BulkServiceException {
        handleCancelled(subject, requestId);
    }

    @Override
    public synchronized void clearRequest(Subject subject, String requestId)
          throws BulkServiceException {
        if (requestHasStoredJobs(requestId)) {
            throw new BulkPermissionDeniedException(requestId + ": " + PREMATURE_CLEAR_ERROR);
        }

        LOGGER.trace("clearRequest {}, calling clear on request store", requestId);
        requestStore.clear(subject, requestId);
    }

    /**
     * Incoming from the admin user.
     * <p>
     * Cancels all jobs bound to this request that are not in a terminal state.
     * <p>
     * Upon job termination, the queue should call request completed (below).
     */
    @Override
    public synchronized void requestCancelled(String requestId) throws BulkServiceException {
        LOGGER.trace("requestCancelled: {}.", requestId);
        BulkRequestStatus status = requestStore.getStatus(requestId).orElse(null);

        if (status != null) {
            switch (status.getStatus()) {
                case CANCELLED:
                case COMPLETED:
                    LOGGER.info("already terminated: {}.", requestId);
                    return;
                case CANCELLING:
                    LOGGER.info("already being cancelled: {}.", requestId);
                    return;
            }
        }

        if (requestHasStoredJobs(requestId)) {
            requestStore.update(requestId, Status.CANCELLING);
            cancelAllJobs(requestId);
        } else {
            requestStore.update(requestId, CANCELLED);
        }

        statistics.incrementRequestsCancelled();
    }

    /**
     * Callback from the queue.
     * <p>
     * Removes the job from the job store.
     * <p>
     * Updates the status of the request.
     */
    @Override
    public synchronized void requestTargetCancelled(BulkJob job) throws BulkServiceException {
        if (handleTerminatedJob("requestTargetCancelled", job)) {
            statistics.incrementJobsCancelled();
        }
    }

    /**
     * Callback from the queue.
     * <p>
     * Removes the job from the job store.
     * <p>
     * Updates the status of the request.
     */
    @Override
    public synchronized void requestTargetFailed(BulkJob job) throws BulkServiceException {
        if (handleTerminatedJob("requestTargetFailed", job)) {
            statistics.incrementJobsFailed();
        }
    }

    /**
     * Callback from the queue.
     * <p>
     * Removes the job from the job store.
     * <p>
     * Updates the status of the request.
     */
    @Override
    public synchronized void requestTargetCompleted(BulkJob job) throws BulkServiceException {
        if (handleTerminatedJob("requestTargetCompleted", job)) {
            statistics.incrementJobsCompleted();
        }
    }


    @Required
    public void setCallbackExecutorService(ExecutorService service) {
        callbackExecutorService = service;
    }

    @Required
    public void setJobFactory(BulkJobFactory jobFactory) {
        this.jobFactory = jobFactory;
    }

    @Required
    public void setJobStore(BulkJobStore jobStore) {
        this.jobStore = jobStore;
    }

    @Required
    public void setListHandler(ListDirectoryHandler listHandler) {
        this.listHandler = listHandler;
    }

    @Required
    public void setQueue(BulkServiceQueue queue) {
        this.queue = queue;
    }

    @Required
    public void setRequestStore(BulkRequestStore requestStore) {
        this.requestStore = requestStore;
    }

    @Required
    public void setStatistics(BulkServiceStatistics statistics) {
        this.statistics = statistics;
    }

    /**
     * Processes the request as a top-level BulkRequestJob, adds a completion listener, and submits
     * the job to the queue.
     */
    @Override
    public synchronized void submitRequest(BulkRequest request) throws BulkServiceException {
        LOGGER.trace("submitRequest {}.", request.getId());

        String requestId = request.getId();
        Optional<Subject> subject = requestStore.getSubject(requestId);
        if (!subject.isPresent()) {
            throw new RuntimeException("subject missing for " + requestId);
        }

        Optional<Restriction> restriction = requestStore.getRestriction(requestId);
        if (!restriction.isPresent()) {
            throw new RuntimeException("restrictions missing for " + requestId);
        }

        LOGGER.trace("submitRequest {}, creating multiple target job.", request);
        BulkRequestJob job = jobFactory.createRequestJob(request, subject.get(), restriction.get());
        job.setSubmissionHandler(this);

        LOGGER.trace("submitRequest {}, setting a new completion handler.", job.getKey().getKey());
        job.setCompletionHandler(new BulkJobCompletionHandler(queue));

        LOGGER.trace("submitRequest {}, calling submit.", requestId);
        queue.submit(job);
    }

    @Override
    public synchronized void submitSingleTargetJob(String target,
          BulkJobKey parentKey,
          FileAttributes attributes,
          MultipleTargetJob parent)
          throws BulkServiceException {
        SingleTargetJob job = jobFactory.createSingleTargetJob(target, parentKey, attributes,
              parent);
        job.setExecutorService(callbackExecutorService);
        submit(job);
    }

    @Override
    public synchronized void submitTargetExpansionJob(String target,
          FileAttributes attributes,
          MultipleTargetJob parent)
          throws BulkServiceException {
        /*
         *  The parent key is always the key of this job.
         */
        TargetExpansionJob job = jobFactory.createTargetExpansionJob(target, attributes, parent);
        job.setListHandler(listHandler);
        job.setSubmissionHandler(this);
        submit(job);
    }

    @GuardedBy("this")
    private void cancelAllJobs(String requestId) throws BulkServiceException {
        LOGGER.trace("cancelAllJobs: {}.", requestId);

        /*
         *  The top-level BulkRequestJobs are not stored,
         *  so we need to find them on the queue if they are still there.
         */
        queue.cancelRequestJob(requestId);

        jobStore.cancelAll(requestId);
    }

    /**
     * Queries the jobStore to see if there are running jobs belonging to the request, and cancels
     * them.
     * <p>
     * Calls update on the requestStore.
     */
    @GuardedBy("this")
    private void handleCancelled(Subject subject, String requestId)
          throws BulkServiceException {
        LOGGER.trace("cancelRequest {}, {}.", subject, requestId);

        if (!requestStore.isRequestSubject(subject, requestId)) {
            throw new BulkPermissionDeniedException(requestId);
        }

        if (isRequestAlreadyCleared(requestId)) {
            throw new BulkRequestNotFoundException(requestId);
        }

        if (isRequestActive(requestId)) {
            requestCancelled(requestId);
        }
    }

    @GuardedBy("this")
    private boolean handleTerminatedJob(String message, BulkJob job)
          throws BulkServiceException {
        BulkJobKey key = job.getKey();
        if (!jobStore.getJob(key).isPresent()) {
            LOGGER.trace("{}: {}, job already deleted.", message, key.getKey());
            return false;
        }

        LOGGER.trace("{}: {}, calling delete on job store.", message, key.getKey());
        jobStore.delete(key);

        String requestId = key.getRequestId();

        try {
            boolean stillRunning = requestHasStoredJobs(requestId);

            /*
             *  The condition on the job completion handler method
             *  denotes that there are no further jobs running.
             *
             *  If it is true, we need to check the job store
             *  to see if all jobs have been removed yet.
             *
             *  In the case of both, we mark the request as terminated.
             */
            if (job.getCompletionHandler().isRequestCompleted() &&
                  !stillRunning) {
                setTerminalRequestState(job.getKey().getRequestId());
            }

            if (job.getState() == State.FAILED) {
                requestStore.getRequest(requestId).ifPresent(request -> {
                    if (request.isCancelOnFailure() && stillRunning) {
                        /*
                         *  Cancel this request, but allow the request
                         *  to terminate as completed, so that
                         *  clearOnFailure can have effect as well
                         *  if it is set.
                         */
                        try {
                            cancelAllJobs(requestId);
                        } catch (BulkServiceException e) {
                            LOGGER.error(
                                  "{} failed; request clear on failure, error  cancelling jobs: {}.",
                                  key.getKey(), e.toString());
                        }
                    }
                });
            }

            if (job instanceof SingleTargetJob) {
                LOGGER.debug("{}: {}, {}, calling target completed on request store.", message,
                      key.getKey(),
                      job.getTarget());
                requestStore.targetCompleted(requestId, job.getTarget(), job.getErrorObject());
                return true;
            }
        } catch (BulkRequestNotFoundException e) {
            LOGGER.debug("request {} was already cleared.", requestId);
        }

        return false;
    }

    @GuardedBy("this")
    private boolean isRequestAlreadyCleared(String requestId)
          throws BulkStorageException {
        /*
         *  There could be a race if an automatic clear option is set.
         */
        if (!requestStore.getRequest(requestId).isPresent()) {
            LOGGER.debug("request already cleared", requestId);
            return true;
        }

        return false;
    }

    @GuardedBy("this")
    private boolean isRequestActive(String requestId)
          throws BulkStorageException {
        if (isRequestAlreadyCleared(requestId)) {
            return false;
        }

        BulkRequestStatus requestStatus = requestStore.getStatus(requestId).orElse(null);
        if (requestStatus != null) {
            Status status = requestStatus.getStatus();
            switch (status) {
                case COMPLETED:
                case CANCELLED:
                case CANCELLING:
                    return false;
            }
        }

        return true;
    }

    private boolean requestHasStoredJobs(String requestId)
          throws BulkJobStorageException {
        return !jobStore.find(j -> j.getKey().getRequestId().equals(requestId),
              1L).isEmpty();
    }

    @GuardedBy("this")
    private void setTerminalRequestState(String requestId) {
        try {
            requestStore.getStatus(requestId).ifPresent(brs -> {
                switch (brs.getStatus()) {
                    case STARTED:
                    case QUEUED:
                        try {
                            requestStore.update(requestId, COMPLETED);
                            statistics.incrementRequestsCompleted();
                        } catch (BulkServiceException e) {
                            LOGGER.error("Failed to post-process request {}: {}.", requestId,
                                  e.toString());
                        }
                        break;
                    case CANCELLING:
                        try {
                            requestStore.update(requestId, CANCELLED);
                            statistics.incrementRequestsCompleted();
                        } catch (BulkServiceException e) {
                            LOGGER.error("Failed to post-process request {}: {}.", requestId,
                                  e.toString());
                        }
                        break;
                    default:
                }
            });
        } catch (BulkServiceException e) {
            LOGGER.error("Failed to post-process request {}: {}.", requestId, e.toString());
        }
    }

    /**
     * Stores the job.
     * <p>
     * Submits the job to the queue.
     */
    @GuardedBy("this")
    private void submit(BulkJob job) throws BulkServiceException {
        BulkJobKey key = job.getKey();

        if (!isRequestActive(key.getRequestId())) {
            LOGGER.trace("submit: {}, not storing/submitting job; request no longer active",
                  key.getKey());
            job.cancel();
            requestTargetCancelled(job);
            return;
        }

        LOGGER.trace("submit: {}, storing job.", key.getKey());
        jobStore.store(job);

        if (job instanceof SingleTargetJob) {
            LOGGER.trace("submit: {}, adding target to request store.", key.getKey());
            requestStore.addTarget(key.getRequestId());
        }

        LOGGER.trace("submit: {}, passing job to the queue.", key.getKey());
        queue.submit(job);
    }
}