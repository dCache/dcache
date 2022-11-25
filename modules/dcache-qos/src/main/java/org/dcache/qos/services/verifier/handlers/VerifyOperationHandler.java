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
package org.dcache.qos.services.verifier.handlers;

import static org.dcache.qos.data.QoSAction.VOID;
import static org.dcache.qos.data.QoSMessageType.CLEAR_CACHE_LOCATION;
import static org.dcache.qos.services.verifier.data.PoolInfoMap.LIMITER;
import static org.dcache.qos.services.verifier.data.VerifyOperationState.CANCELED;
import static org.dcache.qos.services.verifier.handlers.FileStatusVerifier.VERIFY_FAILURE_MESSAGE;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import java.io.Serializable;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.qos.QoSException;
import org.dcache.qos.data.FileQoSRequirements;
import org.dcache.qos.data.FileQoSUpdate;
import org.dcache.qos.data.QoSAction;
import org.dcache.qos.data.QoSAdjustmentStatus;
import org.dcache.qos.listeners.QoSActionCompletedListener;
import org.dcache.qos.listeners.QoSAdjustmentListener;
import org.dcache.qos.listeners.QoSRequirementsListener;
import org.dcache.qos.services.verifier.data.PoolInfoMap;
import org.dcache.qos.services.verifier.data.VerifyOperation;
import org.dcache.qos.services.verifier.data.VerifyOperationMap;
import org.dcache.qos.services.verifier.data.VerifyOperationState;
import org.dcache.qos.services.verifier.data.VerifyScanRecordMap;
import org.dcache.qos.services.verifier.util.QoSVerifierCounters;
import org.dcache.qos.util.CacheExceptionUtils;
import org.dcache.qos.util.ExceptionMessage;
import org.dcache.qos.vehicles.QoSAdjustmentRequest;
import org.dcache.qos.vehicles.QoSAdjustmentResponse;
import org.dcache.qos.vehicles.QoSScannerVerificationRequest;
import org.dcache.qos.vehicles.QoSVerificationRequest;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main handler responsible for processing verification requests and operations.
 * <p/>
 * Receives the initial updates + requirements and determines whether to add an operation for that
 * file.
 * <p/>
 * Processes operation verification (called by the verification task).
 * <p/>
 * Updates operations when responses from the adjuster arrive.
 * <p/>
 * Tracks batch verification requests and cancellations arriving from the scanner.
 * <p/>
 * Relays notification from the operation map to the engine when an action is aborted or completed.
 * <p/>
 * Class is not marked final for stubbing/mocking purposes.
 */
public class VerifyOperationHandler implements VerifyAndUpdateHandler {

    private static final Logger LOGGER
          = LoggerFactory.getLogger(VerifyOperationHandler.class);
    private static final Logger ABORTED_LOGGER
          = LoggerFactory.getLogger("org.dcache.qos-log");

    private static final String ABORT_LOG_MESSAGE
          = "Storage unit {}: aborted operation for {}; "
          + "referring pool {}; pools tried: {}; {}";

    private static final String ABORT_ALARM_MESSAGE
          = "There are files (storage unit {}) for which an operation "
          + "has been aborted; please consult the qos "
          + "logs or 'history errors' for details.";

    private static final String INACCESSIBLE_FILE_MESSAGE
          = "Pool {} is inaccessible but it contains  "
          + "one or more QoS 'disk' files with no currently readable locations. "
          + "Administrator intervention is required; please consult the qos "
          + "logs or 'history errors' for details.";

    private static final String MISSING_LOCATIONS_MESSAGE
          = "{} has no locations in the namespace (file is lost). "
          + "Administrator intervention is required.";

    private static final String INCONSISTENT_LOCATIONS_ALARM
          = "QoS has detected an inconsistency or lag between "
          + "the namespace replica locations and the actual locations on disk.";

    /**
     * ------------------------------- ALARM HANDLING -------------------------------
     */

    private static void sendOutOfSyncAlarm() {
        /*
         *  Create a new alarm every hour by keying the alarm to
         *  an hourly timestamp.  Otherwise, the alarm counter will
         *  just be updated for each alarm sent.  The rate limiter
         *  will not alarm more than once every 1000 seconds (once every
         *  15 minutes).
         */
        if (LIMITER.tryAcquire()) {
            LOGGER.warn(AlarmMarkerFactory.getMarker(PredefinedAlarm.POOL_LOCATION_SYNC_ISSUE,
                        Instant.now().truncatedTo(ChronoUnit.HOURS).toString()),
                  INCONSISTENT_LOCATIONS_ALARM);
        }
    }

    /**
     * Tracks scan requests and cancellations.
     */
    private VerifyScanRecordMap scanRecordMap;

    /**
     * For communication with the store.
     */
    private VerifyOperationMap fileOpMap;

    /**
     * For retrieval of pool configuration and availability information.
     */
    private PoolInfoMap poolInfoMap;

    /**
     * Implements the logic for deciding what to do concerning the current status of a file.
     */
    private FileStatusVerifier statusVerifier;

    /**
     * For communication with other services.
     */
    private QoSAdjustmentListener adjustmentListener;
    private QoSRequirementsListener requirementsListener;
    private QoSActionCompletedListener actionCompletedListener;

    /**
     * Thread queues.
     */
    private ExecutorService updateExecutor;
    private ScheduledExecutorService taskExecutor;

    /**
     * Statistics.
     */
    private QoSVerifierCounters counters;

    public void cancelCurrentFileOpForPool(String pool) {
        fileOpMap.cancelFileOpForPool(pool, false);
    }

    public ScheduledExecutorService getTaskExecutor() {
        return taskExecutor;
    }

    /**
     * Callback from the adjustment service.
     */
    public void handleAdjustmentResponse(QoSAdjustmentResponse response) {
        counters.incrementReceived(QoSVerifierCounters.ADJ_RESP_MESSAGE);
        PnfsId pnfsId = response.getPnfsId();
        QoSAdjustmentStatus status = response.getStatus();
        Serializable error = response.getError();
        LOGGER.debug("{}, handleAdjustmentResponse: {}{}.", pnfsId, status,
              error == null ? "" : " " + error);
        updateExecutor.submit(() -> {
            switch (status) {
                case FAILED:
                    fileOpMap.updateOperation(pnfsId,
                          CacheExceptionUtils.getCacheExceptionFrom(error));
                    break;
                case CANCELLED:
                case COMPLETED:
                    fileOpMap.updateOperation(pnfsId, null);
                    break;
                default:
            }
        });
    }

    /**
     * From a pool mode or exclusion change communicated by the scanner. Just sets new info on map,
     * and can be done on message thread.
     */
    public void handleExcludedStatusChange(String location, boolean excluded) {
        counters.incrementReceived(QoSVerifierCounters.LOC_EXCL_MESSAGE);
        poolInfoMap.updatePoolInfo(location, excluded);
    }

    /**
     * Callback from scanner.
     */
    public void handleFileOperationsCancelledForPool(String pool) {
        counters.incrementReceived(QoSVerifierCounters.BVRF_CNCL_MESSAGE);
        /*
         *  Do not use the scan executor as cancellation could be queued behind the running
         *  batch updates that it pertains to.
         */
        updateExecutor.submit(() -> {
            scanRecordMap.cancel(pool);
            fileOpMap.cancelFileOpForPool(pool, true);
        });
    }

    /**
     * Incoming cancellation request from a client. Forces removal of operation.
     */
    public void handleFileOperationCancelled(PnfsId pnfsId) {
        counters.incrementReceived(QoSVerifierCounters.VRF_CNCL_MESSAGE);
        updateExecutor.submit(() -> fileOpMap.cancel(pnfsId, true));
    }

    /**
     * Called in the post-process method of the operation map.
     */
    public void handleQoSActionCompleted(PnfsId pnfsId, VerifyOperationState opState,
          QoSAction type, Serializable error) {
        actionCompletedListener.fileQoSActionCompleted(pnfsId, type, error);

        /*
         *  Need to call out to adjuster if this is a cancellation
         */
        if (opState == CANCELED) {
            try {
                adjustmentListener.fileQoSAdjustmentCancelled(pnfsId);
            } catch (QoSException e) {
                LOGGER.warn("Failed to notify adjustment service that {} was cancelled; {}.",
                      pnfsId, e.getMessage());
            }
        }
    }

    /**
     * The entry method for a verify operation.
     * <p/>
     * If the entry is already in the current map, its storage group info is updated.
     * <p/>
     */
    public void handleUpdate(FileQoSUpdate data) {
        LOGGER.debug("handleUpdate, update to be registered: {}", data);
        if (!fileOpMap.createOrUpdateOperation(data)) {
            LOGGER.debug("handleUpdate, operation already registered for: {}", data.getPnfsId());
            handleVerificationNop(data, false);
        }
    }

    /**
     * Called by the verification task.  Checks the status of the file and takes appropriate action.
     * This entails (a) failing the operation if the error is fatal; (b) voiding the operation if
     * nothing else needs to be done; (c) sending an adjustment request to the adjustment listener.
     */
    public void handleVerification(PnfsId pnfsId) {
        VerifyOperation operation = fileOpMap.getRunning(pnfsId);
        if (operation == null) {
            LOGGER.debug(
                  "handleVerification: operation for {} id not in the RUNNING state; returning.",
                  pnfsId);
            return;
        }

        /*
         *  Check for cancellation.  This is rechecked just before dispatching an adjustment request.
         */
        if (operation.isInTerminalState()) {
            LOGGER.debug(
                  "handleVerification: operation {} was terminated before processing; returning.",
                  operation);
            fileOpMap.updateOperation(pnfsId, operation.getException());
            return;
        }

        FileQoSRequirements requirements;
        QoSAction action;

        try {
            FileQoSUpdate data = new FileQoSUpdate(operation.getPnfsId(),
                  operation.getPrincipalPool(),
                  operation.getMessageType());
            requirements = requirementsListener.fileQoSRequirementsRequested(data);

            if (requirements == null) {
                if (operation.getMessageType() == CLEAR_CACHE_LOCATION) {
                    fileOpMap.voidOperation(operation);
                    return;
                }
                throw new QoSException("requirements could not be fetched; failing operation.");
            }

            if (!checkStorageUnit(operation, requirements)) {
                fileOpMap.voidOperation(operation);
                return;
            }

            /*
             *  Refresh pool group on the basis of the current replica locations.
             *  If the verification results in an adjustment, the updated group will
             *  be included in the underlying store update.
             */
            operation.setPoolGroup(poolInfoMap.getEffectivePoolGroup(requirements.getAttributes()
                  .getLocations()));

            action = statusVerifier.verify(requirements, operation);
        } catch (QoSException e) {
            String message = CacheExceptionUtils.getCacheExceptionErrorMessage(
                  VERIFY_FAILURE_MESSAGE,
                  pnfsId,
                  VOID,
                  null, e.getCause());
            /*
             *  FATAL error, should abort operation.
             */
            CacheException exception = new CacheException(
                  CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                  message, e.getCause());
            fileOpMap.updateOperation(pnfsId, exception);
            return;
        } catch (InterruptedException e) {
            LOGGER.debug("operation for {} was interrupted; returning.", pnfsId);
            return;
        }

        LOGGER.debug("handleVerification for {}, action is {}.", pnfsId, action);

        switch (action) {
            case VOID:
                /**  signals the operation map so that the operation can be removed. **/
                fileOpMap.voidOperation(operation);
                break;
            case NOTIFY_MISSING:
                /**  signals the operation map so that the operation can be removed. **/
                handleNoLocationsForFile(operation);
                break;
            case NOTIFY_INACCESSIBLE:
                /**  signals the operation map so that the operation can be removed. **/
                handleInaccessibleFile(operation);
                break;
            case NOTIFY_OUT_OF_SYNC:
                /**  signals the operation map so that the operation can be removed. **/
                handleNamespaceSyncError(pnfsId);
                break;
            case POOL_SELECTION_FAILURE:
                /**  signals the operation map so that the operation can be removed. **/
                handlePoolSelectionError(operation);
                break;
            case WAIT_FOR_STAGE:
            case CACHE_REPLICA:
            case PERSIST_REPLICA:
            case UNSET_PRECIOUS_REPLICA:
            case COPY_REPLICA:
            case FLUSH:
                /**  updates the operation action and sends out an adjustment notification. **/
                try {
                    handleAdjustment(requirements, operation, action);
                } catch (QoSException e) {
                    /*
                     *  FATAL error, should abort operation.
                     */
                    CacheException exception = CacheExceptionUtils.getCacheExceptionFrom(e);
                    fileOpMap.updateOperation(pnfsId, exception);
                }
        }
    }

    /**
     * Request originating from the engine in response to a location update or QoS change.
     */
    public void handleVerificationRequest(QoSVerificationRequest request) {
        counters.incrementReceived(QoSVerifierCounters.VRF_REQ_MESSAGE);
        LOGGER.debug("handleVerificationRequest for {}.", request.getUpdate());
        updateExecutor.submit(() -> handleUpdate(request.getUpdate()));
    }

    /**
     * Request originating from the scanner in response to a pool status change or forced scan.
     */
    public void handleVerificationRequest(QoSScannerVerificationRequest request) {
        counters.incrementReceived(QoSVerifierCounters.BVRF_REQ_MESSAGE);
        LOGGER.debug("handleVerificationRequest for {}, type is {}, group is {}, unit is {}.",
              request.getId(), request.getType(), request.getGroup(), request.getStorageUnit());
        scanRecordMap.updateArrived(request);
    }

    /**
     * Callback from the operation map when operation fails fatally.
     */
    public void operationAborted(VerifyOperation operation,
          String pool,
          Set<String> triedSources,
          int maxRetries) {
        PnfsId pnfsId = operation.getPnfsId();
        String storageUnit = operation.getStorageUnit();
        int retried = operation.getRetried();
        Exception e = operation.getException();
        if (retried >= maxRetries) {
            e = new Exception(String.format("Maximum number of attempts (%s) has been reached",
                  maxRetries), e);
        }

        storageUnit = storageUnit == null ? "*" : storageUnit;

        /*
         *  Alarm notification is keyed to the storage group, so as to avoid
         *  spamming the server or email forwarding. The alarm key changes every hour.
         *  This guarantees that a new alarm is registered each hour.
         *  Send this at warn level, so it is possible to throttle repeated
         *  messages in the domain log.
         */
        LOGGER.warn(AlarmMarkerFactory.getMarker(PredefinedAlarm.FAILED_QOS_OPERATION,
                    storageUnit,
                    "ABORT_QOS-" + Instant.now().truncatedTo(ChronoUnit.HOURS)
                          .toEpochMilli()),
              ABORT_ALARM_MESSAGE,
              storageUnit);

        /*
         *  Full info on the file is logged to the ".qos" log.
         */
        ABORTED_LOGGER.error(ABORT_LOG_MESSAGE,
              storageUnit,
              pnfsId,
              pool == null ? "none" : pool,
              triedSources,
              new ExceptionMessage(e));
    }

    public void setActionCompletedListener(QoSActionCompletedListener actionCompletedListener) {
        this.actionCompletedListener = actionCompletedListener;
    }

    public void setAdjustmentListener(QoSAdjustmentListener adjustmentListener) {
        this.adjustmentListener = adjustmentListener;
    }

    public void setCounters(QoSVerifierCounters counters) {
        this.counters = counters;
    }

    public void setFileOpMap(VerifyOperationMap fileOpMap) {
        this.fileOpMap = fileOpMap;
    }

    public void setPoolInfoMap(PoolInfoMap poolInfoMap) {
        this.poolInfoMap = poolInfoMap;
    }

    public void setRequirementsListener(QoSRequirementsListener requirementsListener) {
        this.requirementsListener = requirementsListener;
    }

    public void setScanRecordMap(VerifyScanRecordMap scanRecordMap) {
        this.scanRecordMap = scanRecordMap;
    }

    public void setStatusVerifier(FileStatusVerifier statusVerifier) {
        this.statusVerifier = statusVerifier;
    }

    public void setTaskExecutor(ScheduledExecutorService taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    public void setUpdateExecutor(ExecutorService updateExecutor) {
        this.updateExecutor = updateExecutor;
    }

    public void updateScanRecord(String pool, boolean failure) {
        LOGGER.debug("updateScanRecord {}, failure {}.", pool, failure);
        scanRecordMap.updateCompleted(pool, failure);

        /*
         *  The map method does batching (under synchronization) so that the message
         *  queue not be overwhelmed or an OOM triggered.
         */
        scanRecordMap.checkForNotify(pool);

        /*
         *  Check to see if record can be removed.
         */
        scanRecordMap.fetchAndRemoveIfCompleted(pool);
    }

    /*
     *  If the scan was triggered by a change in storage unit requirements,
     *  the storage unit of the update object will be non-null.
     *
     *  This could be from an altered number of required replicas, or from a change
     *  in tag partitioning; even if the required number of copies exist, they may need to
     *  be removed and recopied in the latter case.
     *
     *  If the file does not match the non-null unit, we skip it.
     */
    private boolean checkStorageUnit(VerifyOperation operation, FileQoSRequirements requirements) {
        String storageUnit = operation.getStorageUnit();
        if (storageUnit == null) {
            return true;
        }

        FileAttributes attr = requirements.getAttributes();
        String fileStorageUnit = attr.getStorageClass() + "@" + attr.getHsm();
        LOGGER.debug("{}, checkStorageUnit, storage unit {}, fileStorageUnit is {}",
              operation.getPnfsId(), storageUnit, fileStorageUnit);

        if (storageUnit.equals(storageUnit)) {
            return true;
        }

        LOGGER.debug("{}, checkStorageUnit, storage unit {}, fileStorageUnit is {}, skipping ...",
              operation.getPnfsId(), storageUnit, fileStorageUnit);
        return false;
    }

    private void handleAdjustment(FileQoSRequirements requirements,
          VerifyOperation operation,
          QoSAction action) throws QoSException {
        QoSAdjustmentRequest request = new QoSAdjustmentRequest();
        request.setAction(action);
        request.setPnfsId(requirements.getPnfsId());
        request.setAttributes(requirements.getAttributes());
        request.setPoolGroup(operation.getPoolGroup());

        String source = operation.getSource();

        if (source != null) {
            request.setSource(source);
        }

        String target = operation.getTarget();
        if (target != null) {
            request.setTarget(target);
            request.setTargetInfo(poolInfoMap.getPoolManagerInfo(target));
        }

        /*
         *  Notify subscribers.  This will normally be the adjustment service.
         *  Source and target have been set on the operation.
         *  Here we also set the action.
         */
        operation.requestAdjustment(request, adjustmentListener);

        fileOpMap.updateOperation(request);
    }

    private void handleInaccessibleFile(VerifyOperation operation) {
        String pool = operation.getParent();

        if (pool == null) {
            pool = operation.getSource();
        }

        if (pool != null) {
            LOGGER.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.INACCESSIBLE_FILE, pool),
                  INACCESSIBLE_FILE_MESSAGE, pool, pool);
        }

        PnfsId pnfsId = operation.getPnfsId();
        String error = String.format("%s currently has no active locations.", pnfsId);
        CacheException exception
              = CacheExceptionUtils.getCacheException(CacheException.PANIC, VERIFY_FAILURE_MESSAGE,
              pnfsId, VOID, error, null);

        fileOpMap.updateOperation(pnfsId, exception);
    }

    private void handlePoolSelectionError(VerifyOperation operation) {
        String poolGroup = operation.getPoolGroup();
        PoolInfoMap.sendPoolGroupMisconfiguredAlarm(poolGroup);
        PnfsId pnfsId = operation.getPnfsId();
        String error
              = String.format("%s: could not satisfy requirements for file; "
              + "either pools are currently unreachable "
              + "or the pool group %s cannot satisfy the requirements.", pnfsId, poolGroup);
        CacheException exception
              = CacheExceptionUtils.getCacheException(CacheException.PANIC, VERIFY_FAILURE_MESSAGE,
              pnfsId, VOID, error, null);
        fileOpMap.updateOperation(pnfsId, exception);
    }

    private void handleNamespaceSyncError(PnfsId pnfsId) {
        sendOutOfSyncAlarm();
        String error
              = String.format("The namespace is not in sync with the pool repositories for %s.",
              pnfsId);
        CacheException exception
              = CacheExceptionUtils.getCacheException(
              CacheException.PANIC,
              VERIFY_FAILURE_MESSAGE,
              pnfsId, VOID, error, null);
        fileOpMap.updateOperation(pnfsId, exception);
    }

    private void handleNoLocationsForFile(VerifyOperation operation) {
        PnfsId pnfsId = operation.getPnfsId();
        LOGGER.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.LOST_FILE,
                    pnfsId.toString()),
              MISSING_LOCATIONS_MESSAGE, pnfsId);
        String error = String.format("%s has no locations.", pnfsId);
        CacheException exception
              = CacheExceptionUtils.getCacheException(
              CacheException.PANIC,
              VERIFY_FAILURE_MESSAGE,
              pnfsId, VOID, error, null);
        fileOpMap.updateOperation(pnfsId, exception);
    }

    private void handleVerificationNop(FileQoSUpdate data, boolean failed) {
        switch (data.getMessageType()) {
            case POOL_STATUS_DOWN:
            case POOL_STATUS_UP:
            case SYSTEM_SCAN:
                String pool = data.getPool();
                LOGGER.debug("handleVerificationNop {}, updating scan record for {}",
                      data.getPnfsId(), pool);
                updateScanRecord(pool, failed);
                break;
            case QOS_MODIFIED:
                actionCompletedListener.fileQoSActionCompleted(data.getPnfsId(), VOID, null);
                break;
            default:
                // nothing to do
        }
    }
}
