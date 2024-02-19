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
package org.dcache.qos.services.engine.handler;

import static org.dcache.qos.data.QoSAction.VOID;
import static org.dcache.qos.data.QoSMessageType.ADD_CACHE_LOCATION;
import static org.dcache.qos.data.QoSMessageType.CLEAR_CACHE_LOCATION;
import static org.dcache.qos.data.QoSMessageType.CORRUPT_FILE;
import static org.dcache.qos.data.QoSMessageType.QOS_MODIFIED;
import static org.dcache.qos.data.QoSMessageType.QOS_MODIFIED_CANCELED;
import static org.dcache.qos.services.engine.util.QoSEngineCounters.QOS_ACTION_COMPLETED;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.Message;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.NoRouteToCellException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.security.auth.Subject;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.cells.CellStub;
import org.dcache.cells.MessageReply;
import org.dcache.namespace.FileAttribute;
import org.dcache.qos.QoSException;
import org.dcache.qos.QoSPolicy;
import org.dcache.qos.QoSState;
import org.dcache.qos.data.FileQoSRequirements;
import org.dcache.qos.data.FileQoSUpdate;
import org.dcache.qos.data.FileQosPolicyInfo;
import org.dcache.qos.data.QoSAction;
import org.dcache.qos.data.QoSMessageType;
import org.dcache.qos.listeners.QoSActionCompletedListener;
import org.dcache.qos.listeners.QoSRequirementsListener;
import org.dcache.qos.listeners.QoSVerificationListener;
import org.dcache.qos.services.engine.data.QoSRecord;
import org.dcache.qos.services.engine.data.db.JdbcQoSEngineDao;
import org.dcache.qos.services.engine.util.QoSEngineCounters;
import org.dcache.qos.services.engine.util.QoSPolicyCache;
import org.dcache.qos.vehicles.QoSVerificationRequest;
import org.dcache.util.BoundedCachedExecutor;
import org.dcache.util.FireAndForgetTask;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.qos.FileQoSPolicyInfoMessage;
import org.dcache.vehicles.qos.QoSRequirementsModifiedMessage;
import org.dcache.vehicles.qos.QoSRequirementsRequestMessage;
import org.dcache.vehicles.qos.QoSTransitionCompletedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the gateway to requirements dispatching.
 * Handles messages by passing them to the listeners providing the file requirements
 * and forwarding notifications to the verification service.
 * <p/>
 * Also handles policy expiration and update using a policy cache and manager thread.
 */
public final class FileQoSStatusHandler implements CellInfoProvider,
      QoSActionCompletedListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileQoSStatusHandler.class);
    public static final int DEFAULT_PERIOD = 10;
    public static final TimeUnit DEFAULT_UNIT = TimeUnit.MINUTES;
    public static final int DEFAULT_QUERY_LIMIT = 1000;

    private static final Set<FileAttribute> REQ_QOS_INFO =
          Set.of(FileAttribute.PNFSID, FileAttribute.QOS_POLICY, FileAttribute.QOS_STATE);

    private final AtomicLong handledExpired = new AtomicLong(0L);

    /**
     *  This is just a concurrent implementation of a set, which is how it is used here.
     */
    private final Set<PnfsId> modifyRequests = new ConcurrentSkipListSet<>();

    private QoSRequirementsListener requirementsListener;
    private QoSVerificationListener verificationListener;
    private CellStub qosTransitionTopic;
    private ExecutorService namespaceRequestExecutor;
    private ExecutorService qosModifyExecutor;
    private ExecutorService verifierRequestExecutor;
    private ExecutorService policyStateExecutor;
    private ScheduledThreadPoolExecutor manager;
    private QoSEngineCounters counters;
    private QoSPolicyCache policyCache;
    private JdbcQoSEngineDao engineDao;
    private PnfsHandler pnfsHandler;
    private int period = DEFAULT_PERIOD;
    private TimeUnit periodUnit = DEFAULT_UNIT;
    private int limit = DEFAULT_QUERY_LIMIT;

    private FireAndForgetTask managerTask;

    @Override
    public void getInfo(PrintWriter pw) {
        StringBuilder builder = new StringBuilder();
        counters.appendRunning(builder);
        builder.append("Periodic transition check set to run every ").append(period).append(" ")
              .append(periodUnit).append(".\n\n");
        counters.appendCounts(builder);
        builder.append("\n").append("Number of expired policy states handled since start: ")
              .append(handledExpired.get()).append("\n");
        pw.print(builder.toString());
    }

    public String getQoSRecordIfExists(PnfsId pnfsId) {
        Optional<QoSRecord> optional = engineDao.getRecord(pnfsId);
        if (optional.isEmpty()) {
            return "no record for " + pnfsId;
        }

        return optional.get().toString();
    }

    public void handleAddCacheLocation(PnfsId pnfsId, String pool) {
        counters.increment(ADD_CACHE_LOCATION.name());
        namespaceRequestExecutor.execute(() -> {
            try {
                LOGGER.debug("handleAddCacheLocation calling fileQoSStatusChanged for {} on {}.",
                      pnfsId, pool);
                fileQoSStatusChanged(new FileQoSUpdate(pnfsId, pool, ADD_CACHE_LOCATION));
            } catch (QoSException e) {
                LOGGER.error("handleAddCacheLocation failed for {} on {}: {}.", pnfsId, pool,
                      e.toString());
            }
        });
    }

    public void handleBrokenFile(PnfsId pnfsId, String pool) {
        counters.increment(CORRUPT_FILE.name());
        namespaceRequestExecutor.execute(() -> {
            try {
                LOGGER.debug("handleBrokenFile calling fileQoSStatusChanged for {} on {}.", pnfsId,
                      pool);
                fileQoSStatusChanged(new FileQoSUpdate(pnfsId, pool, CORRUPT_FILE));
            } catch (QoSException e) {
                LOGGER.error("handleBrokenFile failed for {} on {}: {}.", pnfsId, pool,
                      e.toString());
            }
        });
    }

    public void handleClearCacheLocation(PnfsId pnfsId, String pool) {
        counters.increment(CLEAR_CACHE_LOCATION.name());
        namespaceRequestExecutor.execute(() -> {
            try {
                LOGGER.debug("handleClearCacheLocation calling fileQoSStatusChanged for {} on {}.",
                      pnfsId, pool);
                fileQoSStatusChanged(new FileQoSUpdate(pnfsId, pool, CLEAR_CACHE_LOCATION));
            } catch (QoSException e) {
                /*
                 *  The file was very likely deleted.  Log this only for informational purposes.
                 */
                LOGGER.debug("handleClearCacheLocation for {} on {}: {}.", pnfsId, pool,
                      e.toString());
            }
        });
    }

    public MessageReply<QoSRequirementsModifiedMessage> handleQoSModification(
          QoSRequirementsModifiedMessage message) {
        counters.increment(QOS_MODIFIED.name());
        modifyRequests.add(message.getRequirements().getPnfsId());
        MessageReply<QoSRequirementsModifiedMessage> reply = new MessageReply<>();
        qosModifyExecutor.submit(() -> {
            final FileQoSRequirements requirements = message.getRequirements();
            final Subject subject = message.getSubject();
            PnfsId pnfsId = requirements.getPnfsId();
            Exception exception = null;
            if (!modifyRequests.contains(pnfsId)) {
                LOGGER.debug("handleQoSModification, for {} was cancelled, "
                      + "skipping.", pnfsId);
                message.setSucceeded();
            } else {
                try {
                    LOGGER.debug(
                          "handleQoSModification calling fileQoSRequirementsModified for {}.",
                          pnfsId);
                    requirementsListener.fileQoSRequirementsModified(requirements, subject);
                    LOGGER.debug("handleQoSModification calling fileQoSStatusChanged for {}, {}.",
                          pnfsId, QOS_MODIFIED);
                    policyStateExecutor.submit(() -> {
                        if (!modifyRequests.remove(pnfsId)) {
                            LOGGER.debug("handleQoSModification, for {} was cancelled, "
                                  + "skipping.", pnfsId);
                            message.setSucceeded();
                        } else {
                            FileQoSUpdate update = new FileQoSUpdate(pnfsId, null, QOS_MODIFIED);
                            update.setSubject(subject);
                            try {
                                fileQoSStatusChanged(update);
                                message.setSucceeded();
                            } catch (QoSException e) {
                                String error = String.format(
                                      "could not complete fileQoSStatusChanged "
                                            + "for %s: %s, cause %s.", update, e.getMessage(),
                                      Throwables.getRootCause(e));
                                message.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
                                handleActionCompleted(pnfsId, VOID, error);
                            }
                        }
                    });
                } catch (CacheException e) {
                    exception = e;
                    message.setFailed(e.getRc(), e);
                } catch (QoSException e) {
                    exception = e;
                    message.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
                } catch (NoRouteToCellException e) {
                    exception = e;
                    message.setFailed(CacheException.SERVICE_UNAVAILABLE, e);
                } catch (InterruptedException e) {
                    message.setFailed(CacheException.TIMEOUT, e);
                    exception = e;
                }
            }

            if (exception != null) {
                LOGGER.error("Failed to handle QoS requirements for {}: {}.",
                      requirements.getPnfsId(), exception.getMessage());
                handleActionCompleted(pnfsId, VOID, exception.toString());
            }

            reply.reply(message);
        });
        return reply;
    }

    public void handleQoSModificationCancelled(PnfsId pnfsId, Subject subject) {
        counters.increment(QOS_MODIFIED_CANCELED.name());
        if (!modifyRequests.remove(pnfsId)) {
            qosModifyExecutor.execute(() -> {
                try {
                    LOGGER.debug(
                          "handleQoSModificationCancelled notifying verification listener to cancel {}.",
                          pnfsId);
                    verificationListener.fileQoSVerificationCancelled(pnfsId, subject);
                } catch (QoSException e) {
                    LOGGER.error("Failed to handle QoS requirements for {}: {}.", pnfsId,
                          e.toString());
                }
            });
        }
    }

    public void handleQoSPolicyInfoRequest(FileQoSPolicyInfoMessage message, MessageReply<Message> reply) {
        policyStateExecutor.execute(() -> {
            try {
                PnfsId pnfsId = message.getPnfsId();
                FileAttributes attributes =
                      pnfsId == null ? pnfsHandler.getFileAttributes(message.getPath(),
                            REQ_QOS_INFO) :
                            pnfsHandler.getFileAttributes(pnfsId, REQ_QOS_INFO);
                if (attributes.isDefined(FileAttribute.QOS_POLICY)) {
                    pnfsId = attributes.getPnfsId();
                    FileQosPolicyInfo info = new FileQosPolicyInfo();
                    info.setPath(message.getPath());
                    info.setPnfsId(pnfsId);
                    info.setPolicyName(attributes.getQosPolicy());
                    Optional<QoSRecord> optional = engineDao.getRecord(pnfsId);
                    if (optional.isEmpty()) {
                        info.setPolicyState(attributes.getQosState());
                    } else {
                        QoSRecord record = optional.get();
                        info.setPolicyState(record.getCurrentState());
                        info.setExpires(record.getExpiration());
                    }

                    message.setQosPolicyInfo(info);
                } else {
                    message.setQosPolicyInfo(new FileQosPolicyInfo());
                }

                reply.reply(message);
            } catch (Exception e) {
                reply.fail(message, e);
            }
        });
    }

    public void handleRequirementsRequestReply(MessageReply<QoSRequirementsRequestMessage> reply,
          QoSRequirementsRequestMessage message) {
        verifierRequestExecutor.execute(() -> {
            try {
                LOGGER.debug(
                      "handleRequirementsRequestReply calling fileQoSRequirementsRequested for {}.",
                      message.getUpdate());
                message.setRequirements(
                      requirementsListener.fileQoSRequirementsRequested(message.getUpdate()));
                reply.reply(message);
            } catch (QoSException e) {
                reply.fail(message, e);
            } catch (Exception e) {
                reply.fail(message, e);
            }
        });
    }

    public void handleActionCompleted(PnfsId pnfsId, QoSAction action, Serializable error) {
        fileQoSActionCompleted(pnfsId, action, error);
    }

    public void initialize() {
        LOGGER.info("initializing handler daemon thread to run every {} {}.", period, periodUnit);
        managerTask = new FireAndForgetTask(this::handleExpired);
        manager.scheduleAtFixedRate(managerTask, 2, period, periodUnit);
    }

    public synchronized void reset() {
        manager.remove(managerTask);
        initialize();
    }

    public void setCache(QoSPolicyCache policyCache) {
        this.policyCache = policyCache;
    }

    public void setEngineDao(JdbcQoSEngineDao engineDao) {
        this.engineDao = engineDao;
    }

    public void setQoSEngineCounters(QoSEngineCounters counters) {
        this.counters = counters;
    }

    public void setQosTransitionTopic(CellStub qosTransitionTopic) {
        this.qosTransitionTopic = qosTransitionTopic;
    }

    public void setNamespaceRequestExecutor(ExecutorService namespaceRequestExecutor) {
        this.namespaceRequestExecutor = namespaceRequestExecutor;
    }

    public void setPolicyStateExecutor(BoundedCachedExecutor policyStateExecutor) {
        this.policyStateExecutor = policyStateExecutor;
    }

    public void setQosModifyExecutor(BoundedCachedExecutor qosModifyExecutor) {
        this.qosModifyExecutor = qosModifyExecutor;
    }

    public void setVerifierRequestExecutor(BoundedCachedExecutor verifierRequestExecutor) {
        this.verifierRequestExecutor = verifierRequestExecutor;
    }

    public void setLimit(int period) {
        this.period = period;
    }

    public void setManager(ScheduledThreadPoolExecutor manager) {
        this.manager = manager;
    }

    public void setNamespace(CellStub namespace) {
        pnfsHandler = new PnfsHandler(namespace);
        pnfsHandler.setRestriction(Restrictions.none());
        pnfsHandler.setSubject(Subjects.ROOT);
    }

    public void setPeriod(int period) {
        this.period = period;
    }

    public void setPeriodUnit(TimeUnit periodUnit) {
        this.periodUnit = periodUnit;
    }

    public void setRequirementsListener(QoSRequirementsListener requirementsListener) {
        this.requirementsListener = requirementsListener;
    }

    public void setVerificationListener(QoSVerificationListener verificationListener) {
        this.verificationListener = verificationListener;
    }

    @Override
    public void fileQoSActionCompleted(PnfsId pnfsId, QoSAction action, Serializable error) {
        counters.increment(QOS_ACTION_COMPLETED);
        qosTransitionTopic.notify(new QoSTransitionCompletedMessage(pnfsId, action, error));
    }

    /*
     *  This includes postprocessing for QoS status changes: After a verification request
     *  is sent, the updates which signal replica location modification or requests to
     *  modify the status of a file are processed so that the pnfsid is added to or removed from
     *  the engine tables if appropriate.
     *
     *  This method is called on threads from all three executor pools.
     */
    private void fileQoSStatusChanged(FileQoSUpdate update) throws QoSException {
        PnfsId pnfsId = update.getPnfsId();
        QoSMessageType messageType = update.getMessageType();
        FileQoSRequirements requirements = null;

        try {
            requirements = requirementsListener.fileQoSRequirementsRequested(update);
        } catch (QoSException e) {
            if (e.getCause() instanceof TimeoutCacheException) {
                LOGGER.warn("namespace currently busy or unavailable; "
                      + "could not change status for {}.", pnfsId);
                return;
            }
        }

        /*
         * null requirements here means fatal exception or deletion of file
         */
        if (requirements == null) {
            engineDao.delete(pnfsId);
            return;
        }

        FileAttributes attributes = requirements.getAttributes();
        switch (messageType) {
            case ADD_CACHE_LOCATION:
                /*
                 *  provide for lazy update inside namespace
                 */
                updateQosOnNamespace(pnfsId, attributes);
            case QOS_MODIFIED:
                try {
                    LOGGER.debug(
                          "fileQoSStatusChanged calling updateQoSTransition for {}, {}.",
                          pnfsId, messageType);
                    updateQoSTransition(pnfsId, attributes);
                } catch (QoSException e) {
                    LOGGER.error(
                          "Failed to update QoS transition entry for {}, {}: {}, cause: {}.",
                          pnfsId, messageType, e.getMessage(),
                          Throwables.getRootCause(e));
                }
                break;
            case CLEAR_CACHE_LOCATION:
                if (attributes.isUndefined(FileAttribute.LOCATIONS) || attributes.getLocations().isEmpty()) {
                    // empty location here could mean file deletion
                    engineDao.delete(pnfsId);
                }
            default:
                break;
        }

        QoSVerificationRequest request = new QoSVerificationRequest();
        request.setUpdate(update);
        request.setRequirements(requirements);
        verificationListener.fileQoSVerificationRequested(request);
    }

    private synchronized void handleExpired() {
        LOGGER.info("handleExpired called ...");
        long offset = 0;
        List<QoSRecord> expired = new ArrayList<>();
        do {
            expired.clear();
            LOGGER.debug("handleExpired finding expired qos state durations...");
            offset = engineDao.findExpired(expired, offset, limit);
            LOGGER.debug("handleExpired found expired qos state durations: {}; next id is {}.",
                  expired, offset);
            expired.forEach(record -> policyStateExecutor.submit(
                  () -> nextQoSTransition(record, pnfsHandler)));
            LOGGER.info("handleExpired processed {}/{} records", expired.size(), limit);
            handledExpired.addAndGet(expired.size());
            ++offset;
        } while (expired.size() >= limit);
    }

    /*
     *  Called by the manager.   This is the procedure which serves to check if there
     *  is a new state that the file should be transitioned to, and to record it.
     *  Any change to a new temporary state calls #fileQoSStatusChanged.
     */
    private void nextQoSTransition(QoSRecord record, PnfsHandler pnfsHandler) {
        PnfsId pnfsId = record.getPnfsId();
        int currentState = record.getCurrentState();
        int nextState = currentState + 1;
        String policyName = "UNDEF";

        LOGGER.debug("nextQoSTransition for: {}; currentState is {}.", pnfsId, currentState);

        try {
            FileAttributes attributes = pnfsHandler.getFileAttributes(pnfsId,
                  EnumSet.of(FileAttribute.QOS_STATE, FileAttribute.QOS_POLICY));

            if (!attributes.isDefined(FileAttribute.QOS_POLICY)) {
                LOGGER.info("Stale policy entry for {}; removing from expiration table.", pnfsId);
                engineDao.delete(pnfsId);
                return;
            }

            policyName = attributes.getQosPolicy();
            Optional<QoSPolicy> optionalPolicy = policyCache.getPolicy(policyName);
            if (optionalPolicy.isEmpty()) {
                throw new RuntimeException("Cached policy was not found for "
                      + "file attribute name " + policyName + "; this is a bug.  "
                      + "Please notify dCache developers.");
            }

            int storedState = attributes.getQosState();

            if (storedState != currentState) {
                nextState = Math.max(storedState, currentState);
            }

            QoSPolicy policy = optionalPolicy.get();
            List<QoSState> stateList = policy.getStates();
            if (nextState >= stateList.size()) {
                LOGGER.debug("no further states to policy for {} ({}, {})",
                      pnfsId, policyName, currentState);
                /*
                 *  at the last state.
                 */
                updateState(pnfsId, currentState, policy);
                return;
            }

            FileAttributes modified = new FileAttributes();
            modified.setQosState(nextState);

            /*
             *  synchronous, should block
             */
            LOGGER.debug("Updating file attributes in namespace for {}, {}.", pnfsId, attributes);
            pnfsHandler.setFileAttributes(pnfsId, modified);

            /*
             *  after update to Pnfs, trigger verification.
             */
            LOGGER.debug("calling fileQoSStatusChanged for {}, {}.", pnfsId, QOS_MODIFIED.name());
            fileQoSStatusChanged(new FileQoSUpdate(pnfsId, null, QOS_MODIFIED));
        } catch (CacheException e) {
            LOGGER.error("problem with updating qos state {}, {}, {} -- {}: {}.", pnfsId, nextState,
                  policyName, e.getMessage(), Throwables.getRootCause(e).toString());
        } catch (QoSException e) {
            LOGGER.error("problem verifying changed qos state {}, {}, {} -- {}: {}.", pnfsId,
                  nextState, policyName, e.getMessage(), Throwables.getRootCause(e).toString());
        }
    }

    private void updateQosOnNamespace(PnfsId pnfsId, FileAttributes attributes)
          throws QoSException {
        if (attributes.isDefined(FileAttribute.QOS_POLICY)) {
            FileAttributes qos = new FileAttributes();
            qos.setQosPolicy(attributes.getQosPolicy());
            if (attributes.isDefined(FileAttribute.QOS_STATE)) {
                qos.setQosState(attributes.getQosState());
            } else {
                qos.setQosState(0);
            }
            try {
                pnfsHandler.setFileAttributes(pnfsId, qos);
            } catch (CacheException e) {
                throw new QoSException("Could not set qos file attributes for " + pnfsId, e);
            }
        }
    }

    /*
     *  Maintains the state table. Temporary states have a state index and expiration.
     */
    private void updateQoSTransition(PnfsId pnfsId, FileAttributes fileAttributes)
          throws QoSException {
        Optional<QoSPolicy> policy = Optional.empty();
        Integer index = null;
        if (fileAttributes.isDefined(FileAttribute.QOS_POLICY)) {
            String name = fileAttributes.getQosPolicy();
            if ( Strings.emptyToNull(name) != null) {
                policy = policyCache.getPolicy(name);
                if (fileAttributes.isDefined(FileAttribute.QOS_STATE)) {
                    index = fileAttributes.getQosState();
                } else {
                    index = 0;
                }
            }
        }

        if (policy.isEmpty()) {
            engineDao.delete(pnfsId);
            return;
        }

        updateState(pnfsId, index, policy.get());
    }

    private void updateState(PnfsId pnfsId, int stateIndex, QoSPolicy policy) {
        QoSState state = policy.getStates().get(stateIndex);
        String duration = state.getDuration();
        if (duration == null || "INF".equals(duration)) {
            engineDao.delete(pnfsId);
        } else {
            long durationInMillis = TimeUnit.SECONDS.toMillis(Duration.parse(duration).get(
                  ChronoUnit.SECONDS));
            /*
             * Update increments the expiration by duration.  This guarantees that
             * if something happens to lengthen the current duration (like the
             * qos service being offline), the original policy requirement is observed.
             */
            engineDao.upsert(pnfsId, stateIndex, durationInMillis);
        }
    }
}
