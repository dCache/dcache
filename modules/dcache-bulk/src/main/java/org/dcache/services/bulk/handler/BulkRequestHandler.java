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
package org.dcache.services.bulk.handler;

import static org.dcache.services.bulk.BulkRequestStatus.CANCELLED;
import static org.dcache.services.bulk.BulkRequestStatus.CANCELLING;
import static org.dcache.services.bulk.BulkRequestStatus.COMPLETED;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import javax.security.auth.Subject;
import org.dcache.services.bulk.BulkPermissionDeniedException;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkRequestNotFoundException;
import org.dcache.services.bulk.BulkRequestStatus;
import org.dcache.services.bulk.BulkServiceException;
import org.dcache.services.bulk.BulkStorageException;
import org.dcache.services.bulk.job.AbstractRequestContainerJob;
import org.dcache.services.bulk.job.RequestContainerJobFactory;
import org.dcache.services.bulk.manager.BulkRequestManager;
import org.dcache.services.bulk.store.BulkRequestStore;
import org.dcache.services.bulk.store.BulkTargetStore;
import org.dcache.services.bulk.util.BulkRequestTarget;
import org.dcache.services.bulk.util.BulkServiceStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * Takes the appropriate action at the various phases in the lifetime of a given request. Interacts
 * with the request manager, the job factory and the stores. Receives callbacks from the manager
 * upon request termination.
 */
public final class BulkRequestHandler implements BulkSubmissionHandler,
      BulkRequestCompletionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(BulkRequestHandler.class);

    private static final String PREMATURE_CLEAR_ERROR
          = "Request cannot be cleared until all jobs have terminated; "
          + "try cancelling the request first.";

    private BulkRequestManager requestManager;
    private RequestContainerJobFactory jobFactory;
    private BulkTargetStore targetStore;
    private BulkRequestStore requestStore;
    private BulkServiceStatistics statistics;
    private ExecutorService cancelExecutor;

    @Override
    public void cancelRequest(Subject subject, String id) throws BulkServiceException {
        LOGGER.trace("cancelRequest {}, {}.", subject, id);

        if (!requestExists(id)) {
            throw new BulkRequestNotFoundException(id);
        }

        if (!requestStore.isRequestSubject(subject, id)) {
            throw new BulkPermissionDeniedException(id);
        }

        cancelExecutor.submit(() -> cancelRequest(id));
    }

    @Override
    public void cancelTargets(Subject subject, String id, List<String> targetPaths)
          throws BulkServiceException {
        if (!requestStore.isRequestSubject(subject, id)) {
            throw new BulkPermissionDeniedException("request not owned by user.");
        }

        LOGGER.trace("cancelTargets {}, calling cancel targets on manager for {}", id, targetPaths);

        cancelExecutor.submit(() -> requestManager.cancelTargets(id, targetPaths));
    }

    @Override
    public void clearRequest(Subject subject, String id, boolean cancelIfRunning)
          throws BulkServiceException {
        Long key = requestStore.getKey(id);
        if (requestHasUnprocessedJobs(key)) {
            if (!cancelIfRunning) {
                throw new BulkPermissionDeniedException(id + ": " + PREMATURE_CLEAR_ERROR);
            }
            /*
             *  update the request to have clear set, then cancel
             */
            LOGGER.trace("clearRequest {}, updating to clear on termination, and calling cancel", id);
            requestStore.clearWhenTerminated(subject, id);
            cancelExecutor.submit(() -> cancelRequest(id));
            return;
        }

        LOGGER.trace("clearRequest {}, calling clear on request store", id);
        requestStore.clear(subject, id);
    }

    @Override
    public boolean checkTerminated(String id, boolean cancelled) {
        boolean terminated = setStateIfTerminated(id, cancelled);
        try {
            Long key = requestStore.getKey(id);
            if (!terminated && requestHasFailedJobs(key)) {
                requestStore.getRequest(id).ifPresent(request -> {
                    if (request.isCancelOnFailure()) {
                        /*
                         *  Cancel this request. When the manager discovers that
                         *  the request has terminated, clearOnFailure will be handled
                         *  if it is set.
                         */
                        cancelExecutor.submit(() -> cancelRequest(id));
                    }
                });
            }
        } catch (BulkStorageException e) {
            LOGGER.error("checkTerminated, check for cancel on failure {}: {}.", id,
                  e.toString());
        }
        return terminated;
    }

    @Required
    public void setCancelExecutor(ExecutorService cancelExecutor) {
        this.cancelExecutor = cancelExecutor;
    }

    @Required
    public void setJobFactory(RequestContainerJobFactory jobFactory) {
        this.jobFactory = jobFactory;
    }

    @Required
    public void setRequestManager(BulkRequestManager requestManager) {
        this.requestManager = requestManager;
    }

    @Required
    public void setRequestStore(BulkRequestStore requestStore) {
        this.requestStore = requestStore;
    }

    @Required
    public void setStatistics(BulkServiceStatistics statistics) {
        this.statistics = statistics;
    }

    @Required
    public void setTargetStore(BulkTargetStore targetStore) {
        this.targetStore = targetStore;
    }

    @Override
    public void submitRequestJob(BulkRequest request) throws BulkServiceException {
        AbstractRequestContainerJob job = jobFactory.createRequestJob(request);
        if (storeJobTarget(job.getTarget())) {
            requestManager.submit(job);
        }
    }

    /**
     * Cancels all jobs bound to this request that are not in a terminal state.
     * <p>
     * Upon job termination, the manager should call request completed (below).
     */
    private void cancelRequest(String id) {
        LOGGER.trace("cancelRequest: {}.", id);
        try {
            Optional<BulkRequestStatus> status = requestStore.getRequestStatus(id);
            if (status.isPresent()) {
                switch (status.get()) {
                    case CANCELLED:
                    case COMPLETED:
                        LOGGER.debug("already terminated: {}.", id);
                        return;
                    case QUEUED:
                        /* cancel all targets*/
                        Long key = requestStore.getKey(id);
                        targetStore.cancelAll(key);
                        break;
                }
            } else {
                throw new IllegalStateException("request status for " + id
                      + " does not exist, but the request has not been cleared; this is a bug.");
            }

            requestStore.update(id, CANCELLING);

            if (!requestManager.cancelRequest(id)) {
                if (!requestStore.update(id, CANCELLED)) {
                    LOGGER.error(
                          "cancelRequest {}, container job could not be cancelled, but there are "
                                + "RUNNING targets; manual intervention is needed.", id);
                } else {
                    statistics.incrementRequestsCancelled();
                }

                return;
            }

            /*
             *  Container job has been cancelled; wait for all targets to terminate.
             */
            while (!requestStore.update(id, CANCELLED)) {
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                } catch (InterruptedException e) {
                    LOGGER.debug("cancelRequest interrupted for {}.", id);
                    return;
                }
            }

            statistics.incrementRequestsCancelled();
        } catch (BulkServiceException e) {
            LOGGER.error("cancelRequest {} failed fatally: {}.", id, e.toString());
        }
    }

    private boolean isRequestActive(String id) throws BulkStorageException {
        if (!requestExists(id)) {
            return false;
        }

        Optional<BulkRequestStatus> status = requestStore.getRequestStatus(id);
        if (status.isPresent()) {
            switch (status.get()) {
                case COMPLETED:
                case CANCELLED:
                case CANCELLING:
                    return false;
            }
        }

        return true;
    }

    private boolean requestExists(String id) throws BulkStorageException {
        return requestStore.getRequest(id).isPresent();
    }

    private boolean requestHasUnprocessedJobs(Long rid)
          throws BulkStorageException {
        return targetStore.countUnprocessed(rid) > 0;
    }

    private boolean requestHasFailedJobs(Long rid)
          throws BulkStorageException {
        return targetStore.countFailed(rid) > 0;
    }

    private boolean setStateIfTerminated(String id, boolean cancelled) {
        LOGGER.trace("setStateIfTerminated: {}", id);
        try {
            if (requestStore.update(id, cancelled ? CANCELLED : COMPLETED)) {
                statistics.incrementRequestsCompleted();
                return true;
            }
        } catch (BulkServiceException e) {
            LOGGER.error("Failed to post-process request {}: {}.", id,
                  e.toString());
        }
        return false;
    }

    private boolean storeJobTarget(BulkRequestTarget target) throws BulkServiceException {
        String requestId = target.getRuid();
        String key = target.getKey();

        if (!isRequestActive(requestId)) {
            LOGGER.trace("submit: {}, not storing job; request no longer active", key);
            target.cancel();
            return false;
        }

        LOGGER.trace("submit: {}, storing job.", key);

        if (!targetStore.store(target)) {
            LOGGER.trace("submit: {}, target {} already processed.", key, target);
            return false;
        }

        return true;
    }
}