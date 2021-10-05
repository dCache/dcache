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

import diskCacheV111.vehicles.Message;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.Reply;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.cells.MessageReply;
import org.dcache.services.bulk.handlers.BulkSubmissionHandler;
import org.dcache.services.bulk.queue.BulkServiceQueue;
import org.dcache.services.bulk.store.BulkRequestStore;
import org.dcache.services.bulk.util.BulkServiceStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * Handles incoming messages.  Handles restart reloading. Provides components for request and job
 * processing.
 */
public class BulkService implements CellLifeCycleAware, CellMessageReceiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(BulkService.class);

    private BulkRequestStore requestStore;
    private BulkServiceQueue queue;
    private BulkSubmissionHandler submissionHandler;

    private ExecutorService incomingExecutorService;
    private BulkServiceStatistics statistics;
    private int maxRequestsPerUser;

    @Override
    public void afterStart() {
        /*
         * Maintaining checkpoints on jobs that have been launched
         * is currently unimplemented (it presents several notable
         * complications).
         *
         * Since individual jobs are thus considered lost on restart, we
         * must treat all unfinished requests as if they were idempotent.
         *
         * We thus reload the requests from the persistent store into memory,
         * resetting any requests with non-terminal states to QUEUED
         * (the latter is taken care of by the underlying implementation.)
         *
         * There is no danger in a race here because the state of the
         * requests is not checked until the job queue is initialized
         * and started.
         */
        try {
            LOGGER.info("Loading requests into the request store/queue; "
                  + "incomplete requests will be reset to QUEUED.");
            requestStore.load();
        } catch (BulkServiceException e) {
            LOGGER.error("There was a problem reloading requests {}.", e.toString());
        }

        LOGGER.info("Initializing the job queue.");
        queue.initialize();

        LOGGER.info("Signalling the job queue.");
        queue.signal();

        LOGGER.info("Service startup completed.");
    }

    @Override
    public void beforeStop() {
        try {
            requestStore.save();
        } catch (BulkServiceException e) {
            LOGGER.warn("Problem storing on shutdown: {}.", e.toString());
        }
    }

    public Reply messageArrived(BulkRequestMessage message) {
        LOGGER.trace("received BulkRequestMessage {}", message);

        MessageReply<Message> reply = new MessageReply<>();
        incomingExecutorService.execute(() -> {
            try {
                BulkRequest request = message.getRequest();
                Subject subject = message.getSubject();
                checkQuota(subject);
                checkRestrictions(message.getRestriction(), request.getId());
                request.setId(UUID.randomUUID().toString());
                requestStore.store(subject, message.getRestriction(), request, null);
                queue.signal();
                statistics.incrementRequestsReceived(request.getActivity());
                message.setRequestUrl(request.getUrlPrefix() + "/" + request.getId());
                reply.reply(message);
            } catch (BulkServiceException e) {
                LOGGER.error("messageArrived(BulkRequestMessage) {}: {}", message, e.toString());
                reply.fail(message, e);
            }
        });
        return reply;
    }

    public Reply messageArrived(BulkRequestListMessage message) {
        LOGGER.trace("received BulkRequestListMessage {}", message);

        MessageReply<Message> reply = new MessageReply<>();
        incomingExecutorService.execute(() -> {
            try {
                List<String> requests = requestStore.getRequestUrls(message.getSubject(),
                            message.getStatus())
                      .stream()
                      .collect(Collectors.toList());
                message.setRequests(requests);
                reply.reply(message);
            } catch (BulkServiceException e) {
                LOGGER.error("messageArrived(BulkRequestListMessage) {}: {}", message,
                      e.toString());
                reply.fail(message, e);
            }
        });
        return reply;
    }

    public Reply messageArrived(BulkRequestStatusMessage message) {
        LOGGER.trace("received BulkRequestStatusMessage {}", message);

        MessageReply<Message> reply = new MessageReply<>();
        incomingExecutorService.execute(() -> {
            try {
                String requestId = message.getRequestId();
                checkRestrictions(message.getRestriction(), requestId);
                /*
                 *  Checks permissions and presence of request.
                 */
                BulkRequestStatus status = requestStore.getStatus(message.getSubject(), requestId);
                message.setStatus(status);
                reply.reply(message);
            } catch (BulkServiceException e) {
                LOGGER.error("messageArrived(BulkRequestStatusMessage) {}: {}", message,
                      e.toString());
                reply.fail(message, e);
            }
        });
        return reply;
    }

    public Reply messageArrived(BulkRequestCancelMessage message) {
        LOGGER.trace("received BulkRequestCancelMessage {}", message);

        MessageReply<Message> reply = new MessageReply<>();
        incomingExecutorService.execute(() -> {
            try {
                String requestId = message.getRequestId();
                checkRestrictions(message.getRestriction(), requestId);
                /*
                 *  Checks permissions and presence of request.
                 */
                submissionHandler.cancelRequest(message.getSubject(), requestId);
                reply.reply(message);
            } catch (BulkServiceException e) {
                LOGGER.error("messageArrived(BulkRequestCancelMessage) {}: {}", message,
                      e.toString());
                reply.fail(message, e);
            }
        });
        return reply;
    }

    public Reply messageArrived(BulkRequestClearMessage message) {
        LOGGER.trace("received BulkRequestClearMessage {}", message);

        MessageReply<Message> reply = new MessageReply<>();
        incomingExecutorService.execute(() -> {
            try {
                String requestId = message.getRequestId();
                checkRestrictions(message.getRestriction(), requestId);
                /*
                 *  Checks permissions and presence of request.
                 */
                submissionHandler.clearRequest(message.getSubject(), requestId);
                reply.reply(message);
            } catch (BulkServiceException e) {
                LOGGER.error("messageArrived(BulkRequestClearMessage) {}: {}", message,
                      e.toString());
                reply.fail(message, e);
            }
        });
        return reply;
    }

    public BulkServiceQueue getQueue() {
        return queue;
    }

    @Required
    public void setQueue(BulkServiceQueue queue) {
        this.queue = queue;
    }

    public BulkSubmissionHandler getSubmissionHandler() {
        return submissionHandler;
    }

    @Required
    public void setSubmissionHandler(BulkSubmissionHandler submissionHandler) {
        this.submissionHandler = submissionHandler;
    }

    public BulkRequestStore getRequestStore() {
        return requestStore;
    }

    @Required
    public void setRequestStore(BulkRequestStore requestStore) {
        this.requestStore = requestStore;
    }

    @Required
    public void setIncomingExecutorService(ExecutorService incomingExecutorService) {
        this.incomingExecutorService = incomingExecutorService;
    }

    @Required
    public void setMaxRequestsPerUser(int maxRequestsPerUser) {
        this.maxRequestsPerUser = maxRequestsPerUser;
    }

    @Required
    public void setStatistics(BulkServiceStatistics statistics) {
        this.statistics = statistics;
    }

    /**
     * This may be subject to change.
     *
     * @param restriction from the incoming message
     * @param requestId
     * @throws BulkPermissionDeniedException
     * @throws BulkRequestStorageException
     */
    private void checkRestrictions(Restriction restriction,
          String requestId)
          throws BulkPermissionDeniedException,
          BulkRequestStorageException {
        Optional<Restriction> option = requestStore.getRestriction(requestId);
        Restriction original = option.orElse(null);

        if (restriction == null || Restrictions.none().equals(restriction)) {
            return;
        }

        if (original != null && !original.isSubsumedBy(restriction)) {
            throw new BulkPermissionDeniedException(requestId);
        }
    }

    private void checkQuota(Subject subject) throws BulkQuotaExceededException,
          BulkRequestStorageException {
        String user = BulkRequestStore.uidGidKey(subject);
        if (requestStore.countNonTerminated(user) >= maxRequestsPerUser) {
            throw new BulkQuotaExceededException(user);
        }
        statistics.addUserRequest(user);
    }
}