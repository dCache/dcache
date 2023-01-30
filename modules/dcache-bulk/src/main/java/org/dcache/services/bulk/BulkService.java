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
package org.dcache.services.bulk;

import static org.dcache.services.bulk.job.AbstractRequestContainerJob.findAbsolutePath;
import static org.dcache.services.bulk.util.BulkRequestTarget.computeFsPath;

import com.google.common.base.Strings;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.Message;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.Reply;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.cells.CellStub;
import org.dcache.cells.MessageReply;
import org.dcache.namespace.FileAttribute;
import org.dcache.services.bulk.BulkRequest.Depth;
import org.dcache.services.bulk.activity.BulkActivityFactory;
import org.dcache.services.bulk.handler.BulkSubmissionHandler;
import org.dcache.services.bulk.manager.BulkRequestManager;
import org.dcache.services.bulk.store.BulkRequestStore;
import org.dcache.services.bulk.store.BulkTargetStore;
import org.dcache.services.bulk.util.BulkServiceStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * Bulk service façade.  Handles incoming messages.  Handles restart reloading.
 */
public final class BulkService implements CellLifeCycleAware, CellMessageReceiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(BulkService.class);
    private static final String TARGET_COUNT_ERROR_FORMAT = "The number of targets %s exceeds "
          + "maximum %s for directory expansion %s.";

    /**
     * Error message as per WLCG Tape REST API (v1) reference document.
     */
    private static final String INVALID_TARGET =
          "The file %s does not belong to the %s request %s. No modification has been "
                + "made to this request.";

    private BulkRequestManager requestManager;
    private BulkActivityFactory activityFactory;
    private BulkRequestStore requestStore;
    private BulkTargetStore targetStore;
    private BulkSubmissionHandler submissionHandler;
    private BulkServiceStatistics statistics;
    private ExecutorService incomingExecutorService;
    private CellStub namespace;
    private Depth allowedDepth;
    private int maxRequestsPerUser;
    private int maxFlatTargets;
    private int maxShallowTargets;
    private int maxRecursiveTargets;

    @Override
    public void afterStart() {
        /*
         *  Done here to guarantee providers are loaded before manager runs.
         */
        LOGGER.info("Initializing the activity factory.");
        activityFactory.initialize();

        /*
         *  In case the service is starting up at same time as namespace, which
         *  is necessary for processing request targets.
         */
        waitForNamespace();

        incomingExecutorService.execute(() -> initialize());
    }

    public Reply messageArrived(BulkRequestMessage message) {
        LOGGER.trace("received BulkRequestMessage {}", message);
        MessageReply<Message> reply = new MessageReply<>();
        incomingExecutorService.execute(() -> {
            try {
                BulkRequest request = message.getRequest();
                Subject subject = message.getSubject();
                Restriction restriction = message.getRestriction();
                checkQuota(subject);
                String uid = UUID.randomUUID().toString();
                request.setUid(uid);
                checkRestrictions(restriction, uid);
                checkActivity(request);
                checkDepthConstraints(request);
                requestStore.store(subject, restriction, request);
                statistics.incrementRequestsReceived(request.getActivity());
                requestManager.signal();
                message.setRequestUrl(request.getUrlPrefix() + "/" + request.getUid());
                reply.reply(message);
            } catch (BulkServiceException e) {
                LOGGER.error("messageArrived(BulkRequestMessage) {}: {}", message, e.toString());
                reply.fail(message, e);
            } catch (Exception e) {
                reply.fail(message, e);
                Thread thisThread = Thread.currentThread();
                thisThread.getUncaughtExceptionHandler().uncaughtException(thisThread, e);
            }
        });
        return reply;
    }

    public Reply messageArrived(BulkRequestListMessage message) {
        LOGGER.trace("received BulkRequestListMessage {}", message);

        MessageReply<Message> reply = new MessageReply<>();
        incomingExecutorService.execute(() -> {
            try {
                List<BulkRequestSummary> requests = requestStore.getRequestSummaries(
                            message.getStatus(), message.getOwners(), message.getPath(),
                            message.getOffset())
                      .stream().collect(Collectors.toList());
                message.setRequests(requests);
                reply.reply(message);
            } catch (BulkServiceException e) {
                LOGGER.error("messageArrived(BulkRequestListMessage) {}: {}", message,
                      e.toString());
                reply.fail(message, e);
            } catch (Exception e) {
                reply.fail(message, e);
                Thread thisThread = Thread.currentThread();
                thisThread.getUncaughtExceptionHandler().uncaughtException(thisThread, e);
            }
        });
        return reply;
    }

    public Reply messageArrived(BulkRequestStatusMessage message) {
        LOGGER.trace("received BulkRequestStatusMessage {}", message);

        MessageReply<Message> reply = new MessageReply<>();
        incomingExecutorService.execute(() -> {
            try {
                Subject subject = message.getSubject();
                String uuid = message.getRequestUuid();
                checkRestrictions(message.getRestriction(), uuid);
                matchActivity(message.getActivity(), uuid);
                BulkRequestInfo status = requestStore.getRequestInfo(subject, uuid,
                      message.getOffset());
                message.setInfo(status);
                reply.reply(message);
            } catch (BulkServiceException e) {
                LOGGER.error("messageArrived(BulkRequestStatusMessage) {}: {}", message,
                      e.toString());
                reply.fail(message, e);
            } catch (Exception e) {
                reply.fail(message, e);
                Thread thisThread = Thread.currentThread();
                thisThread.getUncaughtExceptionHandler().uncaughtException(thisThread, e);
            }
        });
        return reply;
    }

    public Reply messageArrived(BulkRequestCancelMessage message) {
        LOGGER.trace("received BulkRequestCancelMessage {}", message);

        MessageReply<Message> reply = new MessageReply<>();
        incomingExecutorService.execute(() -> {
            try {
                Subject subject = message.getSubject();
                String uuid = message.getRequestUuid();
                checkRestrictions(message.getRestriction(), uuid);
                matchActivity(message.getActivity(), uuid);
                List<String> targetPaths = message.getTargetPaths();
                if (targetPaths == null || targetPaths.isEmpty()) {
                    submissionHandler.cancelRequest(subject, uuid);
                } else {
                    validateTargets(uuid, subject, targetPaths);
                    submissionHandler.cancelTargets(subject, uuid, targetPaths);
                }
                reply.reply(message);
            } catch (BulkServiceException e) {
                LOGGER.error("messageArrived(BulkRequestCancelMessage) {}: {}", message,
                      e.toString());
                reply.fail(message, e);
            } catch (Exception e) {
                reply.fail(message, e);
                Thread thisThread = Thread.currentThread();
                thisThread.getUncaughtExceptionHandler().uncaughtException(thisThread, e);
            }
        });
        return reply;
    }

    public Reply messageArrived(BulkRequestClearMessage message) {
        LOGGER.trace("received BulkRequestClearMessage {}", message);

        MessageReply<Message> reply = new MessageReply<>();
        incomingExecutorService.execute(() -> {
            try {
                String uuid = message.getRequestUuid();
                Subject subject = message.getSubject();
                checkRestrictions(message.getRestriction(), uuid);
                matchActivity(message.getActivity(), uuid);
                submissionHandler.clearRequest(subject, uuid, message.isCancelIfRunning());
                reply.reply(message);
            } catch (BulkServiceException e) {
                LOGGER.error("messageArrived(BulkRequestClearMessage) {}: {}", message,
                      e.toString());
                reply.fail(message, e);
            } catch (Exception e) {
                reply.fail(message, e);
                Thread thisThread = Thread.currentThread();
                thisThread.getUncaughtExceptionHandler().uncaughtException(thisThread, e);
            }
        });
        return reply;
    }

    public synchronized int getMaxFlatTargets() {
        return maxFlatTargets;
    }

    public synchronized int getMaxShallowTargets() {
        return maxShallowTargets;
    }

    public synchronized int getMaxRecursiveTargets() {
        return maxRecursiveTargets;
    }

    public synchronized int getMaxRequestsPerUser() {
        return maxRequestsPerUser;
    }

    public synchronized Depth getAllowedDepth() {
        return allowedDepth;
    }

    @Required
    public synchronized void setAllowedDepth(Depth allowedDepth) {
        this.allowedDepth = allowedDepth;
    }

    @Required
    public synchronized void setMaxFlatTargets(int maxFlatTargets) {
        this.maxFlatTargets = maxFlatTargets;
    }

    @Required
    public synchronized void setMaxShallowTargets(int maxShallowTargets) {
        this.maxShallowTargets = maxShallowTargets;
    }

    @Required
    public synchronized void setMaxRecursiveTargets(int maxRecursiveTargets) {
        this.maxRecursiveTargets = maxRecursiveTargets;
    }

    @Required
    public synchronized void setMaxRequestsPerUser(int maxRequestsPerUser) {
        this.maxRequestsPerUser = maxRequestsPerUser;
    }

    @Required
    public void setActivityFactory(BulkActivityFactory activityFactory) {
        this.activityFactory = activityFactory;
    }

    @Required
    public void setNamespace(CellStub namespace) {
        this.namespace = namespace;
    }

    @Required
    public void setRequestStore(BulkRequestStore requestStore) {
        this.requestStore = requestStore;
    }

    @Required
    public void setRequestManager(BulkRequestManager requestManager) {
        this.requestManager = requestManager;
    }

    @Required
    public void setSubmissionHandler(BulkSubmissionHandler submissionHandler) {
        this.submissionHandler = submissionHandler;
    }

    @Required
    public void setTargetStore(BulkTargetStore targetStore) {
        this.targetStore = targetStore;
    }

    @Required
    public void setIncomingExecutorService(ExecutorService incomingExecutorService) {
        this.incomingExecutorService = incomingExecutorService;
    }

    @Required
    public void setStatistics(BulkServiceStatistics statistics) {
        this.statistics = statistics;
    }

    private void checkActivity(BulkRequest request) throws BulkServiceException {
        String activity = request.getActivity();
        if (!activityFactory.isValidActivity(activity)) {
            throw new BulkServiceException(activity + " is not a recognized activity.");
        }
    }

    private synchronized void checkDepthConstraints(BulkRequest request)
          throws BulkPermissionDeniedException {
        switch (request.getExpandDirectories()) {
            case ALL:
                switch (allowedDepth) {
                    case ALL:
                        checkTargetCount(request);
                        return;
                    default:
                        throw new BulkPermissionDeniedException(
                              "full directory recursion not permitted.");
                }
            case TARGETS:
                switch (allowedDepth) {
                    case ALL:
                    case TARGETS:
                        checkTargetCount(request);
                        return;
                    default:
                        throw new BulkPermissionDeniedException(
                              "processing children of a directory not permitted.");
                }
            default:
                checkTargetCount(request);
                return;
        }
    }

    private synchronized void checkQuota(Subject subject) throws BulkQuotaExceededException,
          BulkStorageException {
        String user = BulkRequestStore.uidGidKey(subject);
        if (requestStore.countNonTerminated(user) >= maxRequestsPerUser) {
            throw new BulkQuotaExceededException(user);
        }
        statistics.addUserRequest(user);
    }

    private void checkRestrictions(Restriction restriction, String uuid)
          throws BulkPermissionDeniedException, BulkStorageException {
        if (restriction == null || Restrictions.none().equals(restriction)) {
            return;
        }

        Optional<Restriction> option = requestStore.getRestriction(uuid);
        Restriction original = option.orElse(null);

        if (original != null && !original.isSubsumedBy(restriction)) {
            throw new BulkPermissionDeniedException(uuid);
        }
    }

    private synchronized void checkTargetCount(BulkRequest request)
          throws BulkPermissionDeniedException {
        List<String> targets = request.getTarget();
        int listSize = targets == null ? 0 : targets.size();
        switch (request.getExpandDirectories()) {
            case NONE:
                if (listSize > maxFlatTargets) {
                    throw new BulkPermissionDeniedException(
                          String.format(TARGET_COUNT_ERROR_FORMAT, listSize, maxFlatTargets,
                                Depth.NONE.name()));
                }
                break;
            case TARGETS:
                if (listSize > maxShallowTargets) {
                    throw new BulkPermissionDeniedException(
                          String.format(TARGET_COUNT_ERROR_FORMAT, listSize, maxShallowTargets,
                                Depth.TARGETS.name()));
                }
                break;
            case ALL:
                if (listSize > maxRecursiveTargets) {
                    throw new BulkPermissionDeniedException(
                          String.format(TARGET_COUNT_ERROR_FORMAT, listSize, maxRecursiveTargets,
                                Depth.ALL.name()));
                }
                break;
        }
    }

    private void initialize() {
        /*
         * See store specifics for how reload is handled, but the minimal contract is
         * that all incomplete requests be reset to the QUEUED state.
         * There is no danger in a race here because the state of the
         * requests is not checked until the request job manager is initialized
         * and started.
         */
        try {
            LOGGER.info("Loading requests into the request store/queue; "
                  + "incomplete requests will be reset to QUEUED.");
            requestStore.load();
        } catch (BulkServiceException e) {
            LOGGER.error("There was a problem reloading requests: {}.", e.toString());
        }

        try {
            LOGGER.info("Initializing the job manager.");
            requestManager.initialize();
        } catch (Exception e) {
            LOGGER.error("There was a problem initializing the job queue: {}.", e.toString());
        }

        LOGGER.info("Signalling the job manager.");
        requestManager.signal();

        LOGGER.info("Service startup completed.");
    }

    private void matchActivity(String activity, String uuid)
          throws BulkServiceException {
        Optional<BulkRequest> request = requestStore.getRequest(uuid);
        if (request.isPresent()) {
            if (Strings.emptyToNull(activity) != null && !request.get().getActivity()
                  .equalsIgnoreCase(activity)) {
                throw new BulkPermissionDeniedException(
                      uuid + " activity does not match " + activity);
            }
        } else {
            throw new BulkStorageException("request " + uuid + " is not valid");
        }
    }

    private void waitForNamespace() {
        PnfsHandler handler = new PnfsHandler(namespace);
        handler.setSubject(Subjects.ROOT);
        handler.setRestriction(Restrictions.none());

        LOGGER.info("pinging PnfsManager ...");
        while (true) {
            try {
                handler.getFileAttributes(FsPath.ROOT,
                      Collections.unmodifiableSet(EnumSet.of(FileAttribute.PNFSID)));
            } catch (TimeoutCacheException e) {
                LOGGER.info("PnfsManager unavailable, waiting 5 seconds.");
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(5));
                } catch (InterruptedException ie) {
                    LOGGER.info("Sleep 5 seconds interrupted.");
                    break;
                }
                continue;
            } catch (CacheException e) {
                LOGGER.error("Error trying to contact PnfsManager.", e.toString());
            }
            break;
        }
    }

    private void validateTargets(String uuid, Subject subject, List<String> paths)
          throws BulkServiceException {
        Optional<BulkRequest> optional = requestStore.getRequest(uuid);
        if (optional.isEmpty()) {
            throw new BulkRequestNotFoundException(uuid);
        }

        BulkRequest request = optional.get();
        if (!requestStore.isRequestSubject(subject, uuid)) {
            throw new BulkPermissionDeniedException("request not owned by user.");
        }

        /*
         *  We cannot guarantee a path does not belong to the request if the
         *  request is not flat, so we ignore those cases.
         */
        if (request.getExpandDirectories() != Depth.NONE) {
            return;
        }

        String prefix = request.getTargetPrefix();
        Set<FsPath> submitted = targetStore.getInitialTargets(request.getId(), false)
              .stream().map(p -> computeFsPath(prefix, p.getPath().toString()))
              .collect(Collectors.toSet());
        for (String path : paths) {
            if (!submitted.contains(findAbsolutePath(prefix, path))) {
                throw new BulkServiceException(String.format(INVALID_TARGET,
                      path, request.getActivity(), request.getUid()));
            }
        }
    }
}