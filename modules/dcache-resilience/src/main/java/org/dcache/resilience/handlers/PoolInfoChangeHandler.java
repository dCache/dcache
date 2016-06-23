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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import dmg.cells.nucleus.CellMessageReceiver;
import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.poolmanager.SerializablePoolMonitor;
import org.dcache.resilience.data.FileFilter;
import org.dcache.resilience.data.FileOperationMap;
import org.dcache.resilience.data.PoolFilter;
import org.dcache.resilience.data.PoolInfoDiff;
import org.dcache.resilience.data.PoolInfoMap;
import org.dcache.resilience.data.PoolOperationMap;
import org.dcache.resilience.data.PoolStateUpdate;
import org.dcache.resilience.util.MapInitializer;

/**
 * <p>Constitutes the receiver inside the resilience service for
 *      the changes communicated by the PoolManager.</p>
 *
 * <p>If resilience is waiting for initialization, the message contents
 *      (the pool monitor) are conveyed to the {@link MapInitializer}.</p>
 */
public final class PoolInfoChangeHandler implements CellMessageReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(
                    PoolInfoChangeHandler.class);

    private static final String SYNC_ALARM =
                    "Last pool monitor refresh was at %s, elapsed time is "
                                    + "greater than %s %s; resilience is "
                                    + "out of sync with pool monitor.";

    private MapInitializer   initializer;
    private PoolInfoMap      poolInfoMap;
    private PoolOperationMap poolOperationMap;
    private FileOperationMap fileOperationMap;

    private ResilienceMessageHandler resilienceMessageHandler;
    private ExecutorService updateService;
    private ScheduledExecutorService refreshService;
    private ScheduledFuture refreshFuture;

    private volatile boolean enabled = true;
    private long lastRefresh;
    private long refreshTimeout;
    private TimeUnit refreshTimeoutUnit;

    public long getRefreshTimeout() {
        return refreshTimeout;
    }

    public TimeUnit getRefreshTimeoutUnit() {
        return refreshTimeoutUnit;
    }

    public void messageArrived(SerializablePoolMonitor monitor) {
        if (!enabled) {
            return;
        }

        if (initializer.isInitialized()) {
            updateService.submit(() -> reloadAndScan(monitor));
        } else {
            initializer.updatePoolMonitor(monitor);
        }
    }

    /**
     * <p>Invoked in response to the reception of a
     *      {@link SerializablePoolMonitor} message.</p>
     *
     * <p>Does essentially the same thing as the initialization load
     *      of the pool data, with the exception that certain
     *      kinds of changes are further processed for potential task
     *      cancellation and pool (re)scans which the changes may
     *      necessitate.</p>
     *
     * @param poolMonitor the updated PoolMonitor
     */
    public PoolInfoDiff reloadAndScan(PoolMonitor poolMonitor) {
        LOGGER.trace("Comparing current pool info to new psu.");
        PoolInfoDiff diff = poolInfoMap.compare(poolMonitor);

        if (diff.isEmpty()) {
            LOGGER.trace("reloadAndScan, nothing to do.");
            lastRefresh = System.currentTimeMillis();
            return diff;
        }

        LOGGER.trace("Cancelling pool operations for removed pools {}.",
                        diff.getOldPools());
        diff.getOldPools().stream().forEach(
                        this::cancelAndRemoveCurrentPoolOperation);

        LOGGER.trace("Cancelling pool operations for pools "
                                        + "removed from groups {}.",
                        diff.getPoolsRemovedFromPoolGroup());
        diff.getPoolsRemovedFromPoolGroup().keySet().stream()
            .forEach(this::cancelCurrentPoolOperation);

        LOGGER.trace("Applying diff to pool info map.");
        poolInfoMap.apply(diff);

        LOGGER.trace("Removing uninitialized from other sets.");
        diff.getUninitializedPools().stream()
            .forEach((p) -> {
                diff.getPoolsAddedToPoolGroup().removeAll(p);
                diff.getPoolsRemovedFromPoolGroup().removeAll(p);
                diff.getModeChanged().remove(p);
                diff.getTagsChanged().remove(p);
            });

        LOGGER.trace("Adding new pools to the pool operation map.");
        diff.getNewPools().stream().forEach(poolOperationMap::add);

        LOGGER.trace("Scanning pools added to pool groups.");
        diff.getPoolsAddedToPoolGroup().entries().stream()
            .forEach(this::poolAddedToPoolGroup);

        LOGGER.trace("Scanning pools removed from pool groups.");
        diff.getPoolsRemovedFromPoolGroup().entries().stream()
            .forEach(this::poolRemovedFromPoolGroup);

        LOGGER.trace("Scanning pool groups with units whose "
                        + "constraints have changed.");
        diff.getConstraints().keySet().stream()
            .forEach(this::storageUnitModified);

        LOGGER.trace("Alerting change of pool status.");
        diff.getModeChanged().entrySet().stream()
            .map(PoolStateUpdate::new)
            .forEach(resilienceMessageHandler::handleInternalMessage);

        LOGGER.trace("Rescanning the pools with changed tags.");
        diff.getTagsChanged().keySet().stream()
            .filter(poolInfoMap::isInitialized)
            .filter(poolInfoMap::isResilientPool)
            .map(poolInfoMap::getPoolState)
            .forEach((u) -> poolOperationMap.scan(u, true));

        LOGGER.trace("Checking to see if previously uninitialized "
                                     + "pools are now ready.");
        diff.getUninitializedPools().stream()
            .filter(poolInfoMap::isInitialized)
            .filter(poolInfoMap::isResilientPool)
            .map(poolInfoMap::getPoolState)
            .forEach(resilienceMessageHandler::handleInternalMessage);

        poolOperationMap.saveExcluded();
        lastRefresh = System.currentTimeMillis();

        LOGGER.trace("DIFF:\n{}", diff);
        return diff;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public void setInitializer(MapInitializer initializer) {
        this.initializer = initializer;
    }

    public void setFileOperationMap(FileOperationMap fileOperationMap) {
        this.fileOperationMap = fileOperationMap;
    }

    public void setPoolInfoMap(PoolInfoMap poolInfoMap) {
        this.poolInfoMap = poolInfoMap;
    }

    public void setPoolOperationMap(PoolOperationMap poolOperationMap) {
        this.poolOperationMap = poolOperationMap;
    }

    public void setRefreshService(ScheduledExecutorService service) {
        refreshService = service;
    }

    public void setRefreshTimeout(long refreshTimeout) {
        this.refreshTimeout = refreshTimeout;
    }

    public void setRefreshTimeoutUnit(TimeUnit refreshTimeoutUnit) {
        this.refreshTimeoutUnit = refreshTimeoutUnit;
    }

    public void setResilienceMessageHandler(
                    ResilienceMessageHandler resilienceMessageHandler) {
        this.resilienceMessageHandler = resilienceMessageHandler;
    }

    public void setUpdateService(ExecutorService service) {
        updateService = service;
    }

    public synchronized void startWatchdog() {
        if (refreshFuture != null) {
            return;
        }

        refreshFuture = refreshService.scheduleAtFixedRate(
                        this::checkLastRefresh, refreshTimeout, refreshTimeout,
                        refreshTimeoutUnit);
        lastRefresh = System.currentTimeMillis();
    }

    public synchronized void stopWatchdog() {
        if (refreshFuture != null) {
            refreshFuture.cancel(true);
            refreshFuture = null;
        }
    }

    private void cancelAndRemoveCurrentPoolOperation(String pool) {
        cancelCurrentPoolOperation(pool);
        poolOperationMap.remove(pool);
    }

    private void cancelCurrentPoolOperation(String pool) {
        PoolFilter poolFilter = new PoolFilter();
        poolFilter.setPools(pool);
        poolOperationMap.cancel(poolFilter);
        FileFilter fileFilter = new FileFilter();
        fileFilter.setParent(pool);
        fileFilter.setForceRemoval(true);
        fileOperationMap.cancel(fileFilter);
        fileFilter = new FileFilter();
        fileFilter.setSource(pool);
        fileOperationMap.cancel(fileFilter);
        fileFilter = new FileFilter();
        fileFilter.setTarget(pool);
        fileOperationMap.cancel(fileFilter);
    }

    /** Called under synchronization **/
    private void checkLastRefresh() {
        if (System.currentTimeMillis() - lastRefresh
                        > refreshTimeoutUnit.toMillis(refreshTimeout)) {
            String initError = String.format(SYNC_ALARM, new Date(lastRefresh),
                            refreshTimeout, refreshTimeoutUnit);
            LOGGER.error(AlarmMarkerFactory.getMarker(
                            PredefinedAlarm.RESILIENCE_SYNC_FAILURE,
                            "resilience", String.valueOf(lastRefresh)),
                         initError);
        }
    }

    /**
     * <p>Scans the "new" pool if it is resilient,
     *      also making sure all files have the sticky bit.</p>
     */
    private void poolAddedToPoolGroup(Entry<String, String> entry) {
        Integer gindex = poolInfoMap.getGroupIndex(entry.getValue());
        if (poolInfoMap.isResilientGroup(gindex)) {
            String pool = entry.getKey();
            poolOperationMap.add(pool);
            poolOperationMap.update(poolInfoMap.getPoolState(pool));
            scanPool(pool, gindex, null);
        }
    }

    /**
     *  <p>Skips non-resilient pool.</p>
     *
     *  <p>NB:  if we try to scan the pool as DOWN, this means we need
     *          to pass the old group id for the pool, because we cannot
     *          synchronize the scan + copy tasks such as to create a barrier
     *          so that we can remove the pool from the map after
     *          everything completes.</p>
     */
    private void poolRemovedFromPoolGroup(Entry<String, String> entry) {
        Integer gindex = poolInfoMap.getGroupIndex(entry.getValue());
        if (poolInfoMap.isResilientGroup(gindex)) {
            scanPool(entry.getKey(), null, gindex);
        }
    }

    /**
     *  <p>Will skip the grace period wait, but still take into
     *     consideration whether the pool has already been scanned
     *     because it went DOWN, or whether it is EXCLUDED.</p>
     */
    private void scanPool(String pool, Integer addedTo, Integer removedFrom) {
        PoolStateUpdate update = poolInfoMap.getPoolState(pool, addedTo,
                        removedFrom);
        poolInfoMap.updatePoolStatus(update);
        if (poolInfoMap.isInitialized(pool)) {
            poolOperationMap.scan(update, false);
        }
    }

    /**
     *  <p>Will skip the grace period wait, as well as transition checks.</p>
     */
    private void scanPool(String pool, String unit) {
        PoolStateUpdate update = poolInfoMap.getPoolState(pool, unit);
        poolOperationMap.scan(update, true);
    }

    private void scanPoolsInGroup(String poolGroupName, String unit) {
        Integer index = poolInfoMap.getGroupIndex(poolGroupName);
        if (!poolInfoMap.isResilientGroup(index)) {
            return;
        }

        poolInfoMap.getPoolsOfGroup(poolInfoMap.getGroupIndex(poolGroupName))
                   .stream()
                   .map(poolInfoMap::getPool)
                   .filter(poolInfoMap::isInitialized)
                   .forEach((p) -> scanPool(p, unit));
    }

    /*
     * <p>A change to the unit requirements means that all resilient pools which
     *      have files belonging to this storage unit should be scanned.</p>
     */
    private void storageUnitModified(String unit) {
        poolInfoMap.getPoolGroupsFor(unit)
                   .stream()
                   .filter(poolInfoMap::isResilientGroup)
                   .map(poolInfoMap::getGroupName)
                   .forEach((group) -> scanPoolsInGroup(group, unit));
    }
}
