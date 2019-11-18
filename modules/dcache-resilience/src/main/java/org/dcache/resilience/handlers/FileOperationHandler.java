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
package org.dcache.resilience.handlers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.HttpProtocolInfo;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageSender;
import dmg.cells.nucleus.CellPath;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.auth.Subjects;
import org.dcache.cells.CellStub;
import org.dcache.pool.migration.PoolMigrationCopyFinishedMessage;
import org.dcache.pool.migration.PoolSelectionStrategy;
import org.dcache.pool.migration.Task;
import org.dcache.pool.migration.TaskParameters;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.resilience.data.FileOperation;
import org.dcache.resilience.data.FileOperationMap;
import org.dcache.resilience.data.FileUpdate;
import org.dcache.resilience.data.MessageType;
import org.dcache.resilience.data.PoolInfoMap;
import org.dcache.resilience.data.StorageUnitConstraints;
import org.dcache.resilience.db.NamespaceAccess;
import org.dcache.resilience.util.CacheExceptionUtils;
import org.dcache.resilience.util.DegenerateSelectionStrategy;
import org.dcache.resilience.util.ExceptionMessage;
import org.dcache.resilience.util.LocationSelectionException;
import org.dcache.resilience.util.LocationSelector;
import org.dcache.resilience.util.RemoveLocationExtractor;
import org.dcache.resilience.util.ReplicaVerifier;
import org.dcache.resilience.util.ResilientFileTask;
import org.dcache.resilience.util.StaticSinglePoolList;
import org.dcache.util.CacheExceptionFactory;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.resilience.ForceSystemStickyBitMessage;
import org.dcache.vehicles.resilience.RemoveReplicaMessage;

import static org.dcache.resilience.data.MessageType.QOS_MODIFIED;

/**
 * <p>Principal resilience logic component.</p>
 *
 * <p>Contains methods for registering a pnfsid for handling and
 *    for updating the number of times it should be processed (based
 *    on the arrival of update messages), for verifying if some
 *    action indeed needs to be taken, for launching a copy/migration
 *    task and for eliminating an excess copy.</p>
 *
 * <p>Class is not marked final for stubbing/mocking purposes.</p>
 */
public class FileOperationHandler implements CellMessageSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(
                    FileOperationHandler.class);
    private static final Logger ACTIVITY_LOGGER =
            LoggerFactory.getLogger("org.dcache.resilience-log");

    private static final String INCONSISTENT_LOCATIONS_ALARM
                    = "Resilience has detected an inconsistency or lag between "
                    + "the namespace replica locations and the actual locations on disk.";

    private static final ImmutableList<StickyRecord> ONLINE_STICKY_RECORD
                    = ImmutableList.of(new StickyRecord("system",
                                                        StickyRecord.NON_EXPIRING));

    private static final RateLimiter LIMITER = RateLimiter.create(0.001);

    private static void sendOutOfSyncAlarm() {
        /*
         *  Create a new alarm every hour by keying the alarm to
         *  an hourly timestamp.  Otherwise, the alarm counter will
         *  just be updated for each alarm sent.  The rate limiter
         *  will not alarm more than once every 1000 seconds (once every
         *  15 minutes).
         */
        if (LIMITER.tryAcquire()) {
            LOGGER.warn(AlarmMarkerFactory.getMarker(
                            PredefinedAlarm.RESILIENCE_LOC_SYNC_ISSUE,
                            Instant.now().truncatedTo(ChronoUnit.HOURS).toString()),
                         INCONSISTENT_LOCATIONS_ALARM);
        }
    }

    /**
     * <p>For communication with the {@link ResilientFileTask}.</p>
     */
    public enum Type {
        COPY,
        REMOVE,
        VOID,
        WAIT_FOR_STAGE,
        SET_STICKY
    }

    private final PoolSelectionStrategy taskSelectionStrategy
                    = new DegenerateSelectionStrategy();
    private final ReplicaVerifier verifier = new ReplicaVerifier();

    private FileOperationMap fileOpMap;
    private PoolInfoMap      poolInfoMap;
    private NamespaceAccess  namespace;
    private LocationSelector locationSelector;

    private CellStub                  pinManager;
    private CellStub                  pools;
    private CellEndpoint              endpoint;
    private String                    poolManagerAddress;
    private ScheduledExecutorService  taskService;

    private ScheduledExecutorService  migrationTaskService;
    private FileTaskCompletionHandler completionHandler;
    private InaccessibleFileHandler   inaccessibleFileHandler;

    private long        launchDelay       =  0L;
    private TimeUnit    launchDelayUnit   = TimeUnit.SECONDS;

    public ScheduledExecutorService getTaskService() {
        return taskService;
    }

    public ScheduledExecutorService getRemoveService() {
        return migrationTaskService;
    }

    public long getLaunchDelay() {
       return launchDelay;
    }

    public TimeUnit getLaunchDelayUnit() {
        return launchDelayUnit;
    }

    public void handleBrokenFileLocation(PnfsId pnfsId, String pool) {
        try {
            FileAttributes attributes
                            = FileUpdate.getAttributes(pnfsId, pool,
                                                       MessageType.CORRUPT_FILE,
                                                       namespace);
            if (attributes == null) {
                LOGGER.trace("{} not ONLINE.", pnfsId);
                return;
            }

            if (!poolInfoMap.isResilientPool(pool)) {
                LOGGER.trace("{} not in resilient group.", pool);
                return;
            }

            Collection<String> locations = attributes.getLocations();
            Collection verified = verifier.verifyLocations(pnfsId, locations, pools);

            if (!verifier.exists(pool, verified)) {
                LOGGER.trace("Broken replica of {} on {} has already been removed.",
                             pnfsId, pool);
                return;
            }

            /*
             *  Get the sticky locations that have been verified.
             */
            Collection<String> sticky = verifier.getSticky(verified);

            if (sticky.contains(pool) && sticky.size() == 1) {
                /*
                 * This is the only copy; do nothing.
                 */
                LOGGER.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.INACCESSIBLE_FILE,
                                                          pnfsId.toString()),
                             "{}: Repair of broken replicas is not possible, "
                                             + "file currently inaccessible", pnfsId);
                return;
            }

            removeTarget(pnfsId, pool);

            locations.remove(pool);

            if (poolInfoMap.getCountableLocations(locations) > 1) {
                FileUpdate update = new FileUpdate(pnfsId, pool,
                                                   MessageType.CLEAR_CACHE_LOCATION,
                                                   false);
                /*
                 * Bypass the message guard check of CDC session.
                 */
                handleLocationUpdate(update);
            } else {
                /*
                 *  No alternate readable source; cannot attempt to make
                 *  any further replicas.
                 */
                LOGGER.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.INACCESSIBLE_FILE,
                                                          pnfsId.toString()),
                             "{}: Repair of broken replicas is not possible, "
                                             + "file currently inaccessible", pnfsId);
            }
        } catch (CacheException e) {
            LOGGER.error("Error during handling of broken file removal ({}, {}): {}",
                         pnfsId, pool, new ExceptionMessage(e));
        } catch (InterruptedException e) {
            LOGGER.warn("Handling of broken file removal interrupted ({}, {})",
                        pnfsId, pool);
        }
    }

    /**
     * <p>The entry method for a PnfsId operation from a location update, called
     *      in response to an incoming message.</p>
     *
     * <p>If the entry is already in the current map, its count is incremented.</p>
     *
     * <p>A check is then made to see if the source belongs to a resilient
     *      pool group.  If not, the update is discarded.</p>
     *
     * <p>All attributes of the file that are necessary for resilience
     *      processing are then fetched.  Thereafter a series of preliminary
     *      checks are run for other disqualifying conditions.  If the pnfsId
     *      does qualify, an entry is added to the {@link FileOperationMap}.</p>
     *
     * @return true if a new operation is added to the map.
     */
    public boolean handleLocationUpdate(FileUpdate data)
                    throws CacheException {
        LOGGER.trace("handleLocationUpdate {}", data);

        /*
         * Allow unverified pool through for QOS_MODIFIED in order to
         * fetch file attributes.
         */
        if (QOS_MODIFIED != data.type) {
            if (data.pool == null) {
                LOGGER.debug("Update of {} with no location; file has likely "
                                             + "been deleted from namespace.",
                             data.pnfsId);
                return false;
            }

            if (!data.verifyPoolGroup(poolInfoMap)) {
                LOGGER.debug("Handle location update ({}, {}, {}; "
                                             + "pool is not a member "
                                             + "of a resilient group.",
                             data.pnfsId, data.pool, data.getGroup());
                return false;
            }
        }

        /*
         * Prefetch all necessary file attributes, including current locations.
         */
        if (!data.validateAttributes(namespace)) {
            /*
             * Could be the result of a delete from namespace triggering a
             * clear cache location message.
             */
            return false;
        }

        /*
         * Now set the pool for QOS_MODIFIED.
         */
        if (QOS_MODIFIED == data.type) {
            /*
             *  Choose the first location.
             *  we know by this point that locations cannot be empty.
             */
            data.pool = data.getAttributes().getLocations().iterator().next();
            if (!data.verifyPoolGroup(poolInfoMap)) {
                LOGGER.debug("QOS_MODIFIED update ({}, {}, {}; "
                                             + "pool is not a member "
                                             + "of a resilient group.",
                             data.pnfsId, data.pool, data.getGroup());
                return false;
            }
        }

        /*
         *  Determine if action needs to be taken (counts).
         */
        if (!data.validateForAction(null,
                                    poolInfoMap,
                                    verifier,
                                    pools)) {
            return false;
        }

        LOGGER.trace("handleLocationUpdate, update to be registered: {}", data);
        return fileOpMap.register(data);
    }

    /**
     * <p>The entry method for a PnfsId operation from a pool scan task.</p>
     *
     * <p>If the entry is already in the current map, its count is incremented.</p>
     *
     * <p>All attributes of the file that are necessary for resilience
     *      processing are then fetched.  Preliminary checks run for disqualifying
     *      conditions here include whether this is a storage unit modification,
     *      in which case the task is registered if the file has the storage unit
     *      in question. Otherwise, verification proceeds as in the
     *      {@link #handleLocationUpdate(FileUpdate)} method.</p>
     *
     * @return true if a new operation is added to the map.
     */
    public boolean handleScannedLocation(FileUpdate data, Integer storageUnit)
                    throws CacheException {
        LOGGER.debug("handleScannedLocation {}", data);

        /*
         * Prefetch all necessary file attributes, including current locations.
         */
        if (!data.validateAttributes(namespace)) {
            /*
             * Could be the result of deletion from namespace during the scan.
             */
            return false;
        }

        data.verifyPoolGroup(poolInfoMap);

        /*
         *  Determine if action needs to be taken.
         */
        if (!data.validateForAction(storageUnit,
                                    poolInfoMap,
                                    verifier,
                                    pools)) {
            return false;
        }

        LOGGER.debug("handleScannedLocation, update to be registered: {}", data);
        return fileOpMap.register(data);
    }

    /**
     * <p>Wraps the creation of a migration {@link Task}.  The task is given
     *      a static single pool list and a degenerate selection strategy,
     *      since the target has already been selected by this handler.</p>
     */
    public Task handleMakeOneCopy(FileAttributes attributes) {
        PnfsId pnfsId = attributes.getPnfsId();
        FileOperation operation = fileOpMap.getOperation(pnfsId);

        LOGGER.trace("Configuring migration task for {}.", pnfsId);
        StaticSinglePoolList list;

        try {
            list = new StaticSinglePoolList(poolInfoMap.getPoolManagerInfo(operation.getTarget()));
        } catch (NoSuchElementException e) {
            CacheException exception = CacheExceptionUtils.getCacheException(
                            CacheException.NO_POOL_CONFIGURED,
                            "Copy %s, could not get PoolManager info for %s: %s.",
                            pnfsId, Type.COPY, poolInfoMap.getPool(operation.getTarget()),
                            e);
            completionHandler.taskFailed(pnfsId, exception);
            return null;
        }

        String source = poolInfoMap.getPool(operation.getSource());

        TaskParameters taskParameters = new TaskParameters(
                        pools,
                        null,   // PnfsManager cell stub not used
                        pinManager,
                        migrationTaskService,
                        taskSelectionStrategy,
                        list,
                        false,  // eager; update should not happen
                        false,  // isMetaOnly; just move the metadata
                        false,  // compute checksum on update; should not happen
                        false,  // force copy even if pool not readable
                        true,   // maintain atime
                        1);

        Task task = new Task(taskParameters, completionHandler, source, pnfsId,
                             ReplicaState.CACHED, ONLINE_STICKY_RECORD,
                             Collections.EMPTY_LIST, attributes,
                             attributes.getAccessTime());
        if (ACTIVITY_LOGGER.isInfoEnabled()) {
            List<String> allPools = list.getPools().stream()
                    .map(PoolManagerPoolInformation::getName)
                    .collect(Collectors.toList());
            ACTIVITY_LOGGER.info("Initiating replication of {} from {} to"
                    + " pools: {}, offline: {}", pnfsId, source, allPools,
                    list.getOfflinePools());
        }
        LOGGER.trace("Created migration task for {}: source {}, list {}.",
                     pnfsId, source, list);

        return task;
    }

    /**
     * @param message returned by pool migration task, needs to be passed
     *                to the migration task.
     */
    public void handleMigrationCopyFinished(
                    PoolMigrationCopyFinishedMessage message) {
        LOGGER.trace("Migration copy finished {}", message);
        try {
            fileOpMap.updateOperation(message);
        } catch (IllegalStateException e) {
            /*
             *  We treat the missing entry benignly, as it is
             *  possible to have a race between removal from forced cancellation
             *  and the arrival of the message from the pool.
             */
            LOGGER.trace("{}", new ExceptionMessage(e));
        }
    }

    public void handlePromoteToSticky(FileAttributes attributes) {
        PnfsId pnfsId = attributes.getPnfsId();
        FileOperation operation = fileOpMap.getOperation(pnfsId);
        String target = poolInfoMap.getPool(operation.getTarget());

        try {
            promoteToSticky(pnfsId, target);
            completionHandler.taskCompleted(pnfsId);
        } catch (CacheException e) {
            completionHandler.taskFailed(pnfsId, e);
        }
    }

    /**
     * <p>Calls {@link #removeTarget(PnfsId, String)} and then reports
     *    success or failure to the completion handler.</p>
     */
    public void handleRemoveOneCopy(FileAttributes attributes) {
        PnfsId pnfsId = attributes.getPnfsId();
        FileOperation operation = fileOpMap.getOperation(pnfsId);

        try {
            String target = poolInfoMap.getPool(operation.getTarget());
            LOGGER.trace("handleRemoveOneCopy {}, removing {}.", pnfsId,
                         target);
            removeTarget(pnfsId, target);
        } catch (CacheException e) {
            completionHandler.taskFailed(pnfsId, e);
        }

        completionHandler.taskCompleted(pnfsId);
    }

    /**
     * <p>Called when there are no available replicas, but the file
     *    can be retrieved from an HSM.</p>
     *
     * <p>Issues a fire and forget request.  Task is considered complete at
     *    that point.</p>
     *
     * <p>When staging actually completes on the PoolManager end, the new
     *    cache location message should be processed by Resilience as a
     *    new FileOperation.</p>
     *
     * <p>Should staging not complete before the pool is once again scanned,
     *    PoolManager should collapse the repeated staging request.</p>
     */
    public void handleStaging(PnfsId pnfsId, ResilientFileTask task) {
        try {
            FileOperation operation = fileOpMap.getOperation(pnfsId);
            FileAttributes attributes
                            = namespace.getRequiredAttributesForStaging(pnfsId);
            String poolGroup = poolInfoMap.getGroup(operation.getPoolGroup());
            LOGGER.trace("handleStaging {}, pool group {}.", pnfsId, poolGroup);
            migrationTaskService.schedule(() -> {
                try {
                    PoolMgrSelectReadPoolMsg msg =
                                    new PoolMgrSelectReadPoolMsg(attributes,
                                                                 getProtocolInfo(),
                                                                 null);
                    msg.setSubject(Subjects.ROOT);
                    msg.setPoolGroup(poolGroup);
                    CellMessage cellMessage
                                    = new CellMessage(new CellPath(poolManagerAddress),
                                                      msg);
                    ACTIVITY_LOGGER.info("Staging {}", pnfsId);
                    endpoint.sendMessage(cellMessage);
                    LOGGER.trace("handleStaging, sent select read pool message "
                                                 + "for {} to poolManager.", pnfsId);
                    completionHandler.taskCompleted(pnfsId);
                } catch (URISyntaxException e) {
                    completionHandler.taskFailed(pnfsId,
                                       CacheExceptionUtils.getCacheException(
                                       CacheException.INVALID_ARGS,
                                       "could not construct HTTP protocol: %s.",
                                       pnfsId, Type.WAIT_FOR_STAGE,
                                       e.getMessage(), null));
                }
            }, 0, TimeUnit.MILLISECONDS);
        } catch (CacheException ce) {
            completionHandler.taskFailed(pnfsId, ce);
        }
    }

    /**
     * <p>If the reply from the Pool Manager indicates that the file has
     *    been staged to a pool outside the pool group, resend the message
     *    with refreshed attributes to trigger a p2p by the Pool Manager to
     *    a readable resilient pool in the correct group.  Otherwise,
     *    discard the message.</p>
     */
    public void handleStagingReply(PoolMgrSelectReadPoolMsg reply) {
        PnfsId pnfsId = reply.getPnfsId();
        try {
            if (reply.getReturnCode() == CacheException.OUT_OF_DATE) {
                FileAttributes attributes
                                = namespace.getRequiredAttributesForStaging(pnfsId);
                /*
                 *  Check to see if one of the new locations is not resilient.
                 */
                Collection<String> locations = attributes.getLocations();
                List<String> valid
                                = poolInfoMap.getReadableLocations(locations)
                                             .stream()
                                             .filter(poolInfoMap::isResilientPool)
                                             .collect(Collectors.toList());
                LOGGER.trace("{}, handleStagingReply, readable resilience "
                                             + "locations are now {}.", valid);
                if (valid.size() == 0) {
                    LOGGER.trace("{}, handleStagingReply, "
                                                 + "PoolManager staged to"
                                                 + " a non-resilient pool, "
                                                 + "requesting p2p.",
                                 pnfsId);

                    /*
                     *  Figure out on which pool group this should be.
                     *
                     *  It's possible that the file was "resilient" when the
                     *  stage request was sent and a subsequently configuration
                     *  change has resulted in the file no longer being
                     *  "resilient".  If this happens, the file may have no
                     *  corresponding resilient pool group.
                     *
                     *  Another possibility is the lack of resilient pool group
                     *  is due to a bug somewhere.
                     */
                    Integer gIndex = null;

                    for (String loc: locations) {
                        Integer pIndex = poolInfoMap.getPoolIndex(loc);
                        gIndex = poolInfoMap.getResilientPoolGroup(pIndex);
                        if (gIndex != null) {
                            break;
                        }
                    }

                    if (gIndex == null) {
                        LOGGER.warn("{}, handleStagingReply, file no longer"
                                + " hosted on resilient pool group", pnfsId);
                        return;
                    }

                    final String poolGroup = poolInfoMap.getGroup(gIndex);
                    LOGGER.trace("{}, handleStagingReply, resilient pool group "
                                                 + "for p2p request: {}.",
                                 pnfsId, poolGroup);

                    migrationTaskService.schedule(() -> {
                        PoolMgrSelectReadPoolMsg msg =
                                        new PoolMgrSelectReadPoolMsg(attributes,
                                                                     reply.getProtocolInfo(),
                                                                     reply.getContext(),
                                                                     reply.getAllowedStates());
                        msg.setSubject(reply.getSubject());
                        msg.setPoolGroup(poolGroup);
                        CellMessage cellMessage = new CellMessage(
                                        new CellPath(poolManagerAddress),
                                        msg);
                        ACTIVITY_LOGGER.info("Selecting read pool for file {}"
                                + " staged to a non-resilient pool", pnfsId);
                        endpoint.sendMessage(cellMessage);
                        LOGGER.trace("handleStagingReply, resent select read pool "
                                                     + "message for {} to poolManager.",
                                     pnfsId);
                    }, 0, TimeUnit.MILLISECONDS);
                    return;
                }
            }
            LOGGER.trace("{} handleStagingReply {}, nothing to do.",
                         pnfsId, reply);
        } catch (CacheException ce) {
            LOGGER.error("handleStagingReply failed: {}.", ce.toString());
        }
    }

    /**
     * <p>Called when a pnfsid has been selected from the operation map for
     *    possible processing. Refreshes locations from namespace, and checks
     *    which of those are currently readable.  Sends an alarm if
     *    no operation can occur but should.</p>
     *
     * <p>Note:  because of the possibility of data loss due to a lag between
     *    the pools and namespace, the namespace locations are now verified
     *    by sending a message to the pool.</p>
     *
     * <p>The following table illustrates the progress of this routine
     *    by positing a file with one type of replica location each
     *    reported initially by the namespace:</p>
     *
     * <table>
     *   <thead>
     *     <th>
     *         <td style="text-align: center;">Cached+Sticky</td>
     *         <td style="text-align: center;">Precious+Sticky</td>
     *         <td style="text-align: center;">Precious</td>
     *         <td style="text-align: center;">Cached</td>
     *         <td style="text-align: center;">Broken</td>
     *         <td style="text-align: center;">Removed</td>
     *         <td style="text-align: center;">OFFLINE</td>
     *         <td style="text-align: center;">EXCLUDED</td>
     *     </th>
     *   </thead>
     *   <tbody>
     *     <tr>
     *             <td style="text-align: left;">LOCATIONS</td>
     *             <td style="text-align: center;">A</td>
     *             <td style="text-align: center;">B1</td>
     *             <td style="text-align: center;">B2</td>
     *             <td style="text-align: center;">C</td>
     *             <td style="text-align: center;">D</td>
     *             <td style="text-align: center;">[E]</td>
     *             <td style="text-align: center;">F</td>
     *             <td style="text-align: center;">G</td>
     *     </tr>
     *     <tr>
     *             <td style="text-align: left;">VERIFIED</td>
     *             <td style="text-align: center;">A</td>
     *             <td style="text-align: center;">B1</td>
     *             <td style="text-align: center;">B2</td>
     *             <td style="text-align: center;">C</td>
     *             <td style="text-align: center;">D</td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;">G</td>
     *     </tr>
     *     <tr>
     *             <td style="text-align: left;">BROKEN</td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;">D</td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *     </tr>
     *     <tr>
     *             <td style="text-align: left;"><i>(remove broken, return)</i></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *     </tr>
     *     <tr/>
     *     <tr>
     *             <td style="text-align: left;">LOCATIONS</td>
     *             <td style="text-align: center;">A</td>
     *             <td style="text-align: center;">B1</td>
     *             <td style="text-align: center;">B2</td>
     *             <td style="text-align: center;">C</td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;">[E]</td>
     *             <td style="text-align: center;">F</td>
     *             <td style="text-align: center;">G</td>
     *     </tr>
     *     <tr>
     *             <td style="text-align: left;">VERIFIED</td>
     *             <td style="text-align: center;">A</td>
     *             <td style="text-align: center;">B1</td>
     *             <td style="text-align: center;">B2</td>
     *             <td style="text-align: center;">C</td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;">G</td>
     *     </tr>
     *     <tr>
     *             <td style="text-align: left;">READABLE</td>
     *             <td style="text-align: center;">A</td>
     *             <td style="text-align: center;">B1</td>
     *             <td style="text-align: center;">B2</td>
     *             <td style="text-align: center;">C</td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;">[E]</td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;">G</td>
     *     </tr>
     *     <tr>
     *             <td style="text-align: left;">VERIFIED READABLE</td>
     *             <td style="text-align: center;">A</td>
     *             <td style="text-align: center;">B1</td>
     *             <td style="text-align: center;">B2</td>
     *             <td style="text-align: center;">C</td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;">G</td>
     *     </tr>
     *     <tr>
     *             <td style="text-align: left;"><i>(alarm, continue)</i></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *     </tr>
     *     <tr/>
     *     <tr>
     *             <td style="text-align: left;">OCCUPIED</td>
     *             <td style="text-align: center;">A</td>
     *             <td style="text-align: center;">B1</td>
     *             <td style="text-align: center;">B2</td>
     *             <td style="text-align: center;">C</td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;">F</td>
     *             <td style="text-align: center;">G</td>
     *     </tr>
     *     <tr>
     *             <td style="text-align: left;">STICKY READABLE</td>
     *             <td style="text-align: center;">A</td>
     *             <td style="text-align: center;">B1</td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;">G</td>
     *     </tr>
     *     <tr>
     *             <td style="text-align: left;">W/O EXCLUDED</td>
     *             <td style="text-align: center;">A</td>
     *             <td style="text-align: center;">B1</td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *     </tr>
     *     <tr>
     *             <td style="text-align: left;">REMOVABLE</td>
     *             <td style="text-align: center;">A</td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *             <td style="text-align: center;"></td>
     *     </tr>
     *   </tbody>
     * </table>
     *
     * @return COPY, REMOVE, SET_STICKY, WAIT_FOR_STAGE,
     *         or VOID if no operation is necessary.
     */
    public Type handleVerification(FileAttributes attributes) {
        PnfsId pnfsId = attributes.getPnfsId();
        FileOperation operation = fileOpMap.getOperation(pnfsId);

        int gindex = operation.getPoolGroup();

        /*
         *  Note that the location-dependent checks are run on this thread
         *  instead of being pre-checked by the FileOperationMap consumer thread
         *  because they require a call to the database.
         */
        try {
            namespace.refreshAttributes(attributes);
        } catch (CacheException e) {
            CacheException exception = CacheExceptionUtils.getCacheException(e.getRc(),
                            FileTaskCompletionHandler.VERIFY_FAILURE_MESSAGE,
                            pnfsId, Type.VOID, null, e.getCause());
            completionHandler.taskFailed(pnfsId, exception);
            return Type.VOID;
        }

        Collection<String> locations = attributes.getLocations();
        LOGGER.trace("handleVerification {}, refreshed access latency {}, "
                                     + "retention policy {}; "
                                     + "locations from namespace {}",
                     pnfsId,
                     attributes.getAccessLatency(),
                     attributes.getRetentionPolicy(),
                     locations);

        /*
         * Somehow, all the cache locations for this file have been removed
         * from the namespace.
         */
        if (locations.isEmpty()) {
            LOGGER.trace("handleVerification {}, no namespace locations found, "
                          + "checking to see if file can be staged.",
                         pnfsId);
            if (shouldTryToStage(attributes, operation)) {
                return Type.WAIT_FOR_STAGE;
            }
            return inaccessibleFileHandler.handleNoLocationsForFile(operation);
        }

        Set<String> members = poolInfoMap.getMemberLocations(gindex, locations);
        LOGGER.trace("handleVerification {}, valid group member locations {}",
                     pnfsId, members);

        /*
         * If all the locations are pools no longer belonging to the group,
         * the operation should be voided.
         */
        if (members.isEmpty()) {
            fileOpMap.voidOperation(pnfsId);
            return Type.VOID;
        }

        Collection responsesFromPools;

        /*
         * Verify the locations. The pools are sent a message which returns
         * whether the copy exists, is waiting, readable, removable, and has
         * the necessary sticky flag owned by system.
         */
        try {
            responsesFromPools = verifier.verifyLocations(pnfsId, members, pools);
        } catch (InterruptedException e) {
            LOGGER.warn("handleVerification, replica verification "
                                        + "for {} was interrupted; "
                                        + "cancelling operation.", pnfsId);
            completionHandler.taskCancelled(pnfsId);
            return Type.VOID;
        }

        LOGGER.trace("handleVerification {}, verified replicas: {}",
                     pnfsId, responsesFromPools);

        /*
         *  First, if there are broken replicas, remove the first one
         *  and iterate.  As with the broken file handler routine, do
         *  not remove the last sticky replica, whatever it is.
         */
        Set<String> broken = verifier.getBroken(responsesFromPools);
        if (!broken.isEmpty()) {
            String target = broken.iterator().next();
            if (!verifier.isSticky(target, responsesFromPools)
                            || verifier.getSticky(responsesFromPools).size() > 1) {
                fileOpMap.updateOperation(pnfsId, null, target);
                operation.incrementCount();
                return Type.REMOVE;
            }
        }

        /*
         *  Ensure that the readable locations from the namespace actually exist.
         *  This is crucial for the counts.
         */
        Set<String> namespaceReadable
                        = poolInfoMap.getReadableLocations(members);

        Set<String> exist = verifier.exist(namespaceReadable, responsesFromPools);

        LOGGER.trace("handleVerification, {}, namespace readable locations {},"
                                     + "verified locations {}",
                     pnfsId, namespaceReadable, exist);

        /*
         * This should now happen very rarely, since resilience itself
         * no longer sets the files to removed, but rather removes
         * the system sticky flag.
         */
        if (namespaceReadable.size() != exist.size()) {
            ACTIVITY_LOGGER.info("The namespace is not in sync with the pool "
                                                 + "repositories for {}: "
                                                 + "namespace locations "
                                                 + "that are readable: {}; "
                                                 + "actually found: {}.",
                                 pnfsId, namespaceReadable, exist);
            sendOutOfSyncAlarm();
        }

        /*
         *  While cached copies are excluded from the resilient count, we
         *  allow them to be included as readable sources.
         */
        if (inaccessibleFileHandler.isInaccessible(exist, operation)) {
            LOGGER.trace("handleVerification {}, "
                                         + "no valid readable locations found, "
                                         + "checking to see if "
                                         + "file can be staged.", pnfsId);
            if (shouldTryToStage(attributes, operation)) {
                return Type.WAIT_FOR_STAGE;
            }
            return inaccessibleFileHandler.handleInaccessibleFile(operation);
        }

        /*
         *  Find just the sticky locations.
         */
        Set<String> sticky = verifier.areSticky(exist, responsesFromPools);

        /*
         *  Check for NEARLINE.  If there are sticky replicas, make the first
         *  one target and return REMOVE.
         */
        if (AccessLatency.NEARLINE.equals(attributes.getAccessLatency())) {

            if (sticky.size() > 0) {
                String target = sticky.iterator().next();
                LOGGER.trace("handleVerification, {}, access latency is NEARLINE, "
                                             + "updating operation with first "
                                             + "sticky target to cache: {}",
                             pnfsId, target);
                fileOpMap.updateOperation(pnfsId, null, target);
                return Type.REMOVE;
            }

            LOGGER.trace("handleVerification, {}, access latency is NEARLINE, "
                                         + "but no sticky replicas:  responses {}",
                         pnfsId, responsesFromPools);
            completionHandler.taskCompleted(pnfsId);
            return Type.VOID;
        }

        /*
         *  Tagging of the pools may have changed and/or the requirements on
         *  the storage class may have changed.  If this is the case,
         *  the files may need to be redistributed.  This begins by choosing
         *  a location to evict.  When all evictions are done, the new copies
         *  are made from the remaining replica.  Again, this operation is
         *  only done on sticky replicas.
         */
        if (shouldEvictALocation(operation, sticky, responsesFromPools)) {
            LOGGER.trace("handleVerification, a replica should be evicted from {}",
                         sticky);
            return Type.REMOVE;
        }

        LOGGER.trace("handleVerification after eviction check, {}, "
                                     + "valid replicas {}",
                        pnfsId, sticky);

        /*
         *  Find the locations in the namespace that are actually occupied.
         *  This is an optimization so that we can choose a new pool from the
         *  group without failing and retrying the migration with a new target.
         *
         *  In effect, this means eliminating the phantom locations from
         *  the namespace.  We do this be adding back into the verified
         *  locations the offline replica locations.
         */
        Set<String> occupied = Sets.union(exist,
                                          Sets.difference(members,
                                                          namespaceReadable));
        /*
         *  Find the non-sticky locations.
         *  Partition the sticky locations between usable and excluded.
         */
        Set<String> nonSticky = Sets.difference(exist, sticky);
        Set<String> excluded
                        = verifier.areSticky(poolInfoMap.getExcludedLocationNames(members),
                                             responsesFromPools);
        sticky = Sets.difference(sticky, excluded);

        LOGGER.trace("handleVerification {}: member pools with a sticky replica  "
                                     + " but which have been manually excluded: {}.",
                     pnfsId, excluded);

        return determineTypeFromConstraints(operation,
                                            excluded.size(),
                                            occupied,
                                            sticky,
                                            nonSticky,
                                            responsesFromPools);
    }

    @Override
    public void setCellEndpoint(CellEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    public void setCompletionHandler(
                    FileTaskCompletionHandler completionHandler) {
        this.completionHandler = completionHandler;
    }

    public void setFileOpMap(FileOperationMap fileOpMap) {
        this.fileOpMap = fileOpMap;
    }

    public void setInaccessibleFileHandler(
                    InaccessibleFileHandler inaccessibleFileHandler) {
        this.inaccessibleFileHandler = inaccessibleFileHandler;
    }

    public void setLaunchDelay(long launchDelay) {
        this.launchDelay = launchDelay;
    }

    public void setLaunchDelayUnit(TimeUnit launchDelayUnit) {
        this.launchDelayUnit = launchDelayUnit;
    }

    public void setLocationSelector(LocationSelector locationSelector) {
        this.locationSelector = locationSelector;
    }

    public void setNamespace(NamespaceAccess namespace) {
        this.namespace = namespace;
    }

    public void setPinManagerStub(CellStub pinManager) {
        this.pinManager = pinManager;
    }

    public void setPoolInfoMap(PoolInfoMap poolInfoMap) {
        this.poolInfoMap = poolInfoMap;
    }

    public void setPoolManagerAddress(String poolManagerAddress) {
        this.poolManagerAddress = poolManagerAddress;
    }

    public void setPoolStub(CellStub pools) {
        this.pools = pools;
    }

    public void setMigrationTaskService(ScheduledExecutorService migrationTaskService) {
        this.migrationTaskService = migrationTaskService;
    }

    public void setTaskService(ScheduledExecutorService taskService) {
        this.taskService = taskService;
    }

    /**
     * <p>Checks the readable locations against the requirements.
     *    If previous operations on this pnfsId have already satisfied them,
     *    the operation should be voided.</p>
     *
     * @param operation -- on the given pnfsid
     * @param excluded -- number of member pools manually excluded by admins
     * @param occupied -- group member pools with a replica in any state
     * @param sticky -- group member replicas that are sticky
     * @param nonSticky -- group member replicas that are not sticky
     * @param verified -- the messages returned by the pools
     * @return the type of operation which should take place, if any.
     */
    private Type determineTypeFromConstraints(FileOperation operation,
                                              int excluded,
                                              Set<String> occupied,
                                              Set<String> sticky,
                                              Set<String> nonSticky,
                                              Collection verified) {
        PnfsId pnfsId = operation.getPnfsId();
        Integer gindex = operation.getPoolGroup();
        Integer sindex = operation.getStorageUnit();

        LOGGER.trace("determineTypeFromConstraints {}, group {}, unit {}.",
                     pnfsId, gindex, sindex);

        StorageUnitConstraints constraints
                        = poolInfoMap.getStorageUnitConstraints(sindex);

        int required = constraints.getRequired();
        int missing = required - sticky.size();

        /*
         *  First compute the missing files on the basis of just the readable
         *  files.  If this is positive, recompute by adding in all the
         *  excluded locations.  If these satisfy the requirement, void
         *  the operation.  Do no allow removes in this case, since this
         *  would imply decreasing already deficient locations.
         */
        if (missing > 0) {
            missing -= excluded;
            if (missing < 0) {
                missing = 0;
            }
        }

        Collection<String> tags = constraints.getOneCopyPer();

        LOGGER.trace("{}, required {}, excluded {}, missing {}.",
                     pnfsId, required, excluded, missing);

        Type type;
        String source = null;
        String target = null;

        try {
            /*
             *  Note that if the operation source or target is preset,
             *  and the location is valid, the selection is skipped.
             */
            if (missing < 0) {
                Integer index = operation.getTarget();
                if (index == null || !poolInfoMap.isPoolViable(index, true)
                                || !verifier.isRemovable(poolInfoMap.getPool(index),
                                                         verified)) {
                    Set<String> removable = verifier.areRemovable(sticky,
                                                                  verified);
                    target = locationSelector.selectRemoveTarget(operation,
                                                                 sticky,
                                                                 removable,
                                                                 tags);
                }

                LOGGER.trace("target to remove: {}", target);
                type = Type.REMOVE;
            } else if (missing > 0) {
                Integer viableSource = operation.getSource();

                if (viableSource != null &&
                                !poolInfoMap.isPoolViable(viableSource,
                                                          false)) {
                    viableSource = null;
                }

                Integer targetIndex = operation.getTarget();
                if (targetIndex == null) {
                    /*
                     *  See if we can avoid a copy by promoting an existing
                     *  non-sticky replica to sticky.
                     *
                     *  If the source pool is actually a non-sticky replica,
                     *  choose that first.
                     */
                    if (viableSource != null) {
                        source = poolInfoMap.getPool(viableSource);
                        if (nonSticky.contains(source)) {
                            fileOpMap.updateOperation(pnfsId, null, source);
                            LOGGER.trace("promoting source to sticky: {}", source);
                            return Type.SET_STICKY;
                        }
                    }

                    target = locationSelector.selectPromotionTarget(operation,
                                                                    sticky,
                                                                    nonSticky,
                                                                    tags);

                    if (target != null) {
                        fileOpMap.updateOperation(pnfsId, null, target);
                        LOGGER.trace("target to promote to sticky: {}", target);
                        return Type.SET_STICKY;
                    }

                    target = locationSelector.selectCopyTarget(operation,
                                                               gindex,
                                                               occupied,
                                                               tags);
                } else if (!poolInfoMap.isPoolViable(targetIndex, true)) {
                    target = locationSelector.selectCopyTarget(operation,
                                                               gindex,
                                                               occupied,
                                                               tags);
                }

                LOGGER.trace("target to copy: {}", target);

                /*
                 *  'sticky' may contain both readable and waiting
                 *  ('from') replicas.  To avoid failure/retry,
                 *  choose only the readable.  If there is only
                 *  an incomplete source, then use it tentatively.
                 */
                Set<String> strictlyReadable =
                                verifier.areReadable(sticky, verified);

                if (viableSource == null) {
                    source = locationSelector.selectCopySource(operation,
                                                               strictlyReadable.isEmpty()
                                                                               ? sticky
                                                                               : strictlyReadable);
                }

                LOGGER.trace("source: {}", source);
                type = Type.COPY;
            } else {
                LOGGER.trace("Nothing to do, VOID operation for {}", pnfsId);
                fileOpMap.voidOperation(pnfsId);
                return Type.VOID;
            }
        } catch (LocationSelectionException e) {
            CacheException exception = CacheExceptionUtils.getCacheException(
                            CacheException.DEFAULT_ERROR_CODE,
                            FileTaskCompletionHandler.VERIFY_FAILURE_MESSAGE,
                            pnfsId, Type.VOID, null, e);
            completionHandler.taskFailed(pnfsId, exception);
            return Type.VOID;
        }

        fileOpMap.updateOperation(pnfsId, source, target);

        return type;
    }

    private ProtocolInfo getProtocolInfo() throws URISyntaxException {
        return new HttpProtocolInfo("Http", 1, 1,
                                    new InetSocketAddress("localhost", 0),
                                    null,
                                    null, null,
                                    new URI("http", "localhost",
                                            null, null));
    }

    /**
     * <p>Synchronously sends message to turn a non-sticky replica into
     *    a sticky replica.</p>
     */
    private void promoteToSticky(PnfsId pnfsId, String target)
                    throws CacheException {
        ForceSystemStickyBitMessage msg
                        = new ForceSystemStickyBitMessage(target, pnfsId);

        LOGGER.trace("Sending message to promote to sticky {}.", msg);
        Future<ForceSystemStickyBitMessage> future
                        = pools.send(new CellPath(target), msg);

        try {
            msg = future.get();
            LOGGER.trace("Returned ForceSystemStickyBitMessage {}.", msg);
        } catch (InterruptedException | ExecutionException e) {
            throw CacheExceptionUtils.getCacheException(
                            CacheException.SELECTED_POOL_FAILED,
                            FileTaskCompletionHandler.FAILED_COPY_MESSAGE,
                            pnfsId, Type.SET_STICKY, target, e);
        }

        CacheException exception = msg.getErrorObject() == null ? null :
                        CacheExceptionFactory.exceptionOf(msg);

        if (exception != null && !CacheExceptionUtils.replicaNotFound(exception)) {
            throw CacheExceptionUtils.getCacheException(
                            CacheException.SELECTED_POOL_FAILED,
                            FileTaskCompletionHandler.FAILED_COPY_MESSAGE,
                            pnfsId, Type.SET_STICKY, target, exception);
        }
    }

    /**
     * <p>Synchronously removes from the target location the cache entry of the
     *      pnfsid associated with this task.  This is done via a message
     *      sent to a handler for this purpose on the pool itself.</p>
     */
    private void removeTarget(PnfsId pnfsId, String target)
                    throws CacheException {
        RemoveReplicaMessage msg = new RemoveReplicaMessage(target,
                                                            pnfsId);

        LOGGER.trace("Sending RemoveReplicasMessage {}.", msg);
        ACTIVITY_LOGGER.info("Removing {} from {}", pnfsId, target);
        Future<RemoveReplicaMessage> future = pools.send(new CellPath(target), msg);

        try {
            msg = future.get();
            LOGGER.trace("Returned ReplicationRepRmMessage {}.", msg);
        } catch (InterruptedException | ExecutionException e) {
            throw CacheExceptionUtils.getCacheException(
                            CacheException.SELECTED_POOL_FAILED,
                            FileTaskCompletionHandler.FAILED_REMOVE_MESSAGE,
                            pnfsId, Type.REMOVE, target, e);
        }

        CacheException exception = msg.getErrorObject() == null ? null :
                        CacheExceptionFactory.exceptionOf(msg);

        if (exception != null && !CacheExceptionUtils.replicaNotFound(exception)) {
            throw CacheExceptionUtils.getCacheException(
                            CacheException.SELECTED_POOL_FAILED,
                            FileTaskCompletionHandler.FAILED_REMOVE_MESSAGE,
                            pnfsId, Type.REMOVE, target, exception);
        }
    }

    /**
     * <p>Checks for necessary eviction due to pool tag changes or
     *    constraint change.  This call will automatically set
     *    the offending location as the target for a remove operation,
     *    and will increment the operation count so that there will
     *    be a chance to repeat the operation in order to make a new copy.</p>
     *
     * <p> Note that the extractor algorithm will never remove the last replica,
     *     because a singleton will always satisfy any equivalence relation.
     *     But we short-circuit this check anyway (for location size < 2).</p>
     */
    private boolean shouldEvictALocation(FileOperation operation,
                                         Collection<String> readableLocations,
                                         Collection verified) {
        if (readableLocations.size() < 2) {
            return false;
        }

        Integer sunit = operation.getStorageUnit();
        if (sunit == null) {
            return false;
        }

        StorageUnitConstraints constraints
                        = poolInfoMap.getStorageUnitConstraints(sunit);
        RemoveLocationExtractor extractor
                        = new RemoveLocationExtractor(constraints.getOneCopyPer(),
                                                      poolInfoMap);
        String toEvict = extractor.findALocationToEvict(readableLocations,
                                                        verified,
                                                        verifier);

        if (toEvict != null) {
            operation.setTarget(poolInfoMap.getPoolIndex(toEvict));
            int count = operation.getOpCount();
            operation.setOpCount(++count);
            return true;
        }

        return false;
    }

    /**
     * <p>Called when there are no accessible replicas for the file.</p>
     *
     * <p>If the file's RetentionPolicy is CUSTODIAL, set the count to 1,
     *    to make sure the task completes after this.  Staging is fire-and-forget,
     *    and depends on a new add cache location message being processed
     *    after staging.</p>
     *
     * @return true if file is CUSTODIAL, false otherwise.
     */
    private boolean shouldTryToStage(FileAttributes attributes,
                                     FileOperation operation) {
        if (attributes.getRetentionPolicy() == RetentionPolicy.CUSTODIAL) {
            LOGGER.trace("shouldTryToStage {}, retention policy is CUSTODIAL.",
                         operation.getPnfsId());
            operation.setOpCount(1);
            return true;
        }
        LOGGER.trace("shouldTryToStage {}, retention policy is not CUSTODIAL",
                     operation.getPnfsId());
        return false;
    }
}
