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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
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
import org.dcache.resilience.util.ResilientFileTask;
import org.dcache.resilience.util.StaticSinglePoolList;
import org.dcache.util.CacheExceptionFactory;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.resilience.RemoveReplicaMessage;

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

    private static final ImmutableList<StickyRecord> ONLINE_STICKY_RECORD
                    = ImmutableList.of(
                    new StickyRecord("system", StickyRecord.NON_EXPIRING));

    /**
     * <p>For communication with the {@link ResilientFileTask}.</p>
     */
    public enum Type {
        COPY,
        REMOVE,
        VOID,
        WAIT_FOR_STAGE
    }

    private final PoolSelectionStrategy taskSelectionStrategy
                    = new DegenerateSelectionStrategy();

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

            int actual = attributes.getLocations().size();
            int countable = poolInfoMap.getCountableLocations(attributes.getLocations());

            if (actual <= 1) {
                /*
                 * This is the only copy, or it is not/no longer in the
                 * namespace. In either case, do nothing.
                 */
                LOGGER.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.INACCESSIBLE_FILE,
                                                          pnfsId.toString()),
                             "{}: Repair of broken replicas is not possible, "
                                             + "file currently inaccessible", pnfsId);
                return;
            }

            removeTarget(pnfsId, pool);

            if (countable > 1) {
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
         *  Determine if action needs to be taken (counts).
         */
        if (!data.validateForAction(null, poolInfoMap)) {
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
        if (!data.validateForAction(storageUnit, poolInfoMap)) {
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

        /*
         * Fire-and-forget best effort.
         */
        operation.ensureSticky(poolInfoMap, pools);

        LOGGER.trace("Configuring migration task for {}.", pnfsId);
        StaticSinglePoolList list;

        try {
            list = new StaticSinglePoolList(poolInfoMap.getPoolManagerInfo(operation.getTarget()));
        } catch (NoSuchElementException e) {
            CacheException exception = CacheExceptionUtils.getCacheException(
                            CacheException.NO_POOL_CONFIGURED,
                            "Copy %s, could not get PoolManager info for %s: %s.",
                            pnfsId, poolInfoMap.getPool(operation.getTarget()),
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
                                       pnfsId, e.getMessage(), null));
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
                     *  Figure out what pool group this should be on.
                     *  At this point, if locations is empty, it's a bug.
                     *  If it isn't, the pools should belong to a resilient
                     *  group, or else we wouldn't be processing this.
                     */
                    Integer gIndex = null;

                    for (String loc: locations) {
                        Integer pIndex = poolInfoMap.getPoolIndex(loc);
                        gIndex = poolInfoMap.getResilientPoolGroup(pIndex);
                        if (gIndex != null) {
                            break;
                        }
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
     *      possible processing. Refreshes locations from namespace, and checks
     *      which of those are currently readable.  Sends an alarm if
     *      no operation can occur but should.</p>
     *
     * @return COPY, REMOVE, or VOID if no operation is necessary.
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
            namespace.refreshLocations(attributes);
        } catch (CacheException e) {
            CacheException exception = CacheExceptionUtils.getCacheException(e.getRc(),
                            FileTaskCompletionHandler.VERIFY_FAILURE_MESSAGE,
                            pnfsId, null, e.getCause());
            completionHandler.taskFailed(pnfsId, exception);
            return Type.VOID;
        }

        Collection<String> locations = attributes.getLocations();

        LOGGER.trace("handleVerification {}, locations from namespace: {}",
                     pnfsId, locations);

        /*
         * Somehow, all the cache locations for this file have been removed.
         */
        if (locations.isEmpty()) {
            LOGGER.trace("handleVerification {}, no locations found, "
                                         + "checking to see if "
                                         + "file can be staged.",
                         pnfsId);
            if (shouldTryToStage(attributes, operation)) {
                return Type.WAIT_FOR_STAGE;
            }
            return inaccessibleFileHandler.handleNoLocationsForFile(operation);
        }

        locations = poolInfoMap.getMemberLocations(gindex, locations);

        LOGGER.trace("handleVerification {}, valid group member locations {}",
                     pnfsId, locations);

        /*
         * If all the locations are pools no longer belonging to the group,
         * the operation should be voided.
         */
        if (locations.isEmpty()) {
            fileOpMap.voidOperation(pnfsId);
            return Type.VOID;
        }

        /*
         *  If we have arrived here, we are expecting there to be an
         *  available source file.  So we need the strictly readable
         *  locations, not just "countable" ones.
         */
        Set<String> readableLocations
                        = poolInfoMap.getReadableLocations(locations);

        LOGGER.trace("handleVerification, {}, readable locations {}", pnfsId,
                        readableLocations);

        if (inaccessibleFileHandler.isInaccessible(readableLocations, operation)) {
            LOGGER.trace("handleVerification {}, no readable locations found, "
                                         + "checking to see if "
                                         + "file can be staged.", pnfsId);
            if (shouldTryToStage(attributes, operation)) {
                return Type.WAIT_FOR_STAGE;
            }
            return inaccessibleFileHandler.handleInaccessibleFile(operation);
        }

        if (shouldEvictALocation(operation, readableLocations)) {
            LOGGER.trace("handleVerification, location should be evicted {}",
                         readableLocations);
            return Type.REMOVE;
        }

        LOGGER.trace("handleVerification after eviction check, {}, locations {}",
                        pnfsId, locations);

        return determineTypeFromConstraints(operation,
                                            locations,
                                            readableLocations);
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
     *      If previous operations on this pnfsId have already satisfied them,
     *      the operation should be voided.</p>
     *
     * @return the type of operation which should take place, if any.
     */
    private Type determineTypeFromConstraints(FileOperation operation,
                                              Collection<String> locations,
                                              Set<String> readableLocations) {
        PnfsId pnfsId = operation.getPnfsId();
        Integer gindex = operation.getPoolGroup();
        Integer sindex = operation.getStorageUnit();

        LOGGER.trace("determineTypeFromConstraints {}, group {}, unit {}.",
                     pnfsId, gindex, sindex);

        StorageUnitConstraints constraints
                        = poolInfoMap.getStorageUnitConstraints(sindex);
        /*
         *  Countable means readable OR intentionally excluded locations.
         *  If there are copies missing only from excluded locations,
         *  do nothing.
         */
        int missing = constraints.getRequired()
                        - poolInfoMap.getCountableLocations(locations);

        Collection<String> tags = constraints.getOneCopyPer();

        LOGGER.trace("{}, readable locations {}, required {}, missing {}.",
                     pnfsId, readableLocations, constraints.getRequired(),
                     missing);

        Type type;
        String source = null;
        String target = null;

        try {
            /*
             *  Note that if the operation source or target is preset,
             *  the selection is skipped.
             */
            if (missing < 0) {
                type = Type.REMOVE;
                Integer pool = operation.getTarget();
                if (pool == null || !poolInfoMap.isPoolViable(pool, true)) {
                    target = locationSelector.selectRemoveTarget(operation,
                                    readableLocations, tags);
                }
                LOGGER.trace("target to remove: {}", target);
            } else if (missing > 0) {
                type = Type.COPY;
                Integer pool = operation.getSource();
                if (pool == null || !poolInfoMap.isPoolViable(pool, false)) {
                    source = locationSelector.selectCopySource(operation,
                                                               readableLocations);
                }
                LOGGER.trace("source: {}", source);
                pool = operation.getTarget();
                if (pool == null || !poolInfoMap.isPoolViable(pool, true)) {
                    target = locationSelector.selectCopyTarget(operation, gindex,
                                    readableLocations, tags);
                }
                LOGGER.trace("target to copy: {}", target);
            } else {
                LOGGER.trace("Nothing to do, VOID operation for {}", pnfsId);
                fileOpMap.voidOperation(pnfsId);
                return Type.VOID;
            }
        } catch (LocationSelectionException e) {
            CacheException exception = CacheExceptionUtils.getCacheException(
                            CacheException.DEFAULT_ERROR_CODE,
                            FileTaskCompletionHandler.VERIFY_FAILURE_MESSAGE,
                            pnfsId, null, e);
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
                            pnfsId, target, e);
        }

        CacheException exception = msg.getErrorObject() == null ? null :
                        CacheExceptionFactory.exceptionOf(msg);

        if (exception != null && !CacheExceptionUtils.replicaNotFound(exception)) {
            throw CacheExceptionUtils.getCacheException(
                            CacheException.SELECTED_POOL_FAILED,
                            FileTaskCompletionHandler.FAILED_REMOVE_MESSAGE,
                            pnfsId, target, exception);
        }
    }

    /**
     * <p>Checks for necessary eviction due to pool tag changes or
     *      constraint change.  This call will automatically set
     *      the offending location as the target for a remove operation,
     *      and will increment the operation count so that there will
     *      be a chance to repeat the operation in order to make a new copy.</p>
     */
    private boolean shouldEvictALocation(FileOperation operation,
                                         Collection<String> readableLocations) {
        if (readableLocations.isEmpty()) {
            return false;
        }

        Integer sunit = operation.getStorageUnit();
        if (sunit == null) {
            return false;
        }

        StorageUnitConstraints constraints
                        = poolInfoMap.getStorageUnitConstraints(sunit);
        RemoveLocationExtractor extractor
                        = new RemoveLocationExtractor(
                        constraints.getOneCopyPer(),
                        poolInfoMap);
        String toEvict = extractor.findALocationToEvict(readableLocations);

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
