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
package org.dcache.qos.services.scanner.handlers;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import diskCacheV111.poolManager.CostModule;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnit;
import diskCacheV111.poolManager.StorageUnit;
import diskCacheV111.poolManager.StorageUnitInfoExtractor;
import diskCacheV111.pools.PoolV2Mode;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.dcache.poolmanager.PoolInfo;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.qos.data.PoolQoSStatus;
import org.dcache.qos.services.scanner.data.PoolFilter;
import org.dcache.qos.services.scanner.data.PoolOperationMap;
import org.dcache.qos.services.scanner.util.ScannerMapInitializer;
import org.dcache.qos.util.PoolMonitorChangeHandler;

/**
 * Manages changes in pool monitor data for the pool operation map.
 * <p/>
 * Certain kinds of changes are processed for potential task cancellation and pool (re)scans which
 * the changes may necessitate.
 */
public final class PoolOpChangeHandler extends
      PoolMonitorChangeHandler<PoolOpDiff, ScannerMapInitializer> {

    private PoolOperationMap poolOperationMap;

    public synchronized PoolOpDiff reloadAndScan(PoolMonitor newPoolMonitor) {
        LOGGER.trace("Comparing current pool info to new psu.");
        PoolOpDiff diff = compare(poolMonitor(), newPoolMonitor);

        if (diff.isEmpty()) {
            LOGGER.trace("reloadAndScan, nothing to do.");
            lastRefresh = System.currentTimeMillis();
            return diff;
        }

        LOGGER.trace("Cancelling pool operations for removed pools {}.", diff.getOldPools());
        diff.getOldPools().forEach(this::cancelAndRemoveCurrentPoolOperation);

        LOGGER.trace("Cancelling pool operations for pools removed from groups {}.",
              diff.getPoolsRemovedFromPoolGroup());
        diff.getPoolsRemovedFromPoolGroup().keySet()
              .forEach(this::cancelCurrentPoolOperation);

        LOGGER.trace("Removing uninitialized from other sets.");
        diff.getUninitializedPools()
              .forEach((p) -> {
                  diff.getPoolsAddedToPoolGroup().removeAll(p);
                  diff.getPoolsRemovedFromPoolGroup().removeAll(p);
                  diff.getModeChanged().remove(p);
                  diff.getTagsChanged().remove(p);
              });

        PoolSelectionUnit currentPsu = newPoolMonitor.getPoolSelectionUnit();

        LOGGER.trace("Adding new pools to the pool operation map.");
        diff.getNewPools().forEach(poolOperationMap::add);

        LOGGER.trace("Scanning pools added to pool groups.");
        diff.getPoolsAddedToPoolGroup().entries()
              .forEach(g -> scanPoolAddedToPoolGroup(g, currentPsu));

        LOGGER.trace("Scanning pools removed from pool groups.");
        diff.getPoolsRemovedFromPoolGroup().entries()
              .forEach(e -> scanPoolRemovedFromPoolGroup(e, currentPsu));

        LOGGER.trace("Scanning pool groups pointing to units added to unit groups {}.",
              diff.getUnitsAddedToPoolGroup());
        diff.getUnitsAddedToPoolGroup().values()
              .forEach(u -> scanPoolsWithStorageUnitModified(u, currentPsu));

        LOGGER.trace("Scanning pool groups pointing to units removed from unit groups {}.",
              diff.getUnitsRemovedFromPoolGroup());
        diff.getUnitsRemovedFromPoolGroup().values()
              .forEach(u -> scanPoolsWithStorageUnitModified(u, currentPsu));

        LOGGER.trace("Scanning pool groups with units whose "
                    + "constraints have changed; new constraints {}.",
              diff.getConstraintsChanged());
        diff.getConstraintsChanged()
              .forEach(u -> scanPoolsWithStorageUnitModified(u, currentPsu));

        LOGGER.trace("Alerting change of pool status.");
        diff.getModeChanged().entrySet()
              .forEach(e -> poolOperationMap.handlePoolStatusChange(e.getKey(),
                    PoolQoSStatus.valueOf(e.getValue())));

        LOGGER.trace("Rescanning the pool groups whose marker changed.");
        diff.getMarkerChanged().forEach(g -> scanPoolsOfModifiedPoolGroup(g, currentPsu));

        LOGGER.trace("Rescanning the pools with changed tags.");
        diff.getTagsChanged().keySet().stream()
              .map(currentPsu::getPool)
              .forEach(p -> poolOperationMap.scan(p.getName(),
                    null,
                    null,
                    null,
                    p.getPoolMode(),
                    true));

        LOGGER.trace("Checking to see if previously uninitialized pools are now ready.");
        poolOperationMap.saveExcluded();
        lastRefresh = System.currentTimeMillis();

        LOGGER.trace("DIFF:\n{}", diff);

        LOGGER.trace("Swapping pool monitors");
        initializer.updatePoolMonitor(newPoolMonitor);

        poolOperationMap.setCurrentPsu(newPoolMonitor.getPoolSelectionUnit());

        return diff;
    }

    public void setMapInitializer(ScannerMapInitializer initializer) {
        this.initializer = initializer;
    }

    public void setPoolOperationMap(PoolOperationMap poolOperationMap) {
        this.poolOperationMap = poolOperationMap;
    }

    private void cancelAndRemoveCurrentPoolOperation(String pool) {
        cancelCurrentPoolOperation(pool);
        poolOperationMap.remove(pool);
    }

    private void cancelCurrentPoolOperation(String pool) {
        PoolFilter poolFilter = new PoolFilter();
        poolFilter.setPools(pool);
        poolOperationMap.cancel(poolFilter);
    }

    private PoolOpDiff compare(PoolMonitor currentPoolMonitor, PoolMonitor nextPoolMonitor) {
        PoolSelectionUnit nextPsu = nextPoolMonitor.getPoolSelectionUnit();
        PoolSelectionUnit currentPsu = currentPoolMonitor.getPoolSelectionUnit();
        PoolOpDiff diff = new PoolOpDiff();

        LOGGER.trace("Searching for currently uninitialized pools.");
        getUninitializedPools(diff, currentPsu);

        LOGGER.trace("comparing pools");
        Set<String> commonPools = comparePools(diff, currentPsu, nextPsu);

        LOGGER.trace("comparing pool groups");
        Set<String> common = comparePoolGroups(diff, currentPsu, nextPsu);

        LOGGER.trace("adding pools and units to new pool groups");
        addPoolsAndUnitsToNewPoolGroups(diff, nextPsu);

        LOGGER.trace("comparing pools in pool groups");
        comparePoolsInPoolGroups(diff, common, currentPsu, nextPsu);

        LOGGER.trace("find pool group marker changes");
        comparePoolGroupMarkers(diff, common, currentPsu, nextPsu);

        LOGGER.trace("comparing storage units");
        common = compareStorageUnits(diff, currentPsu, nextPsu);

        LOGGER.trace("adding pool groups for new storage units");
        addPoolGroupsForNewUnits(diff, nextPsu);

        LOGGER.trace("comparing storage unit links and constraints");
        compareStorageUnitLinksAndConstraints(diff, common, currentPsu, nextPsu);

        LOGGER.trace("comparing pool mode");
        comparePoolMode(diff, commonPools, nextPsu);

        LOGGER.trace("comparing pool tags");
        comparePoolTags(diff, commonPools, currentPoolMonitor, nextPoolMonitor);

        return diff;
    }

    private void addPoolsAndUnitsToNewPoolGroups(PoolOpDiff diff, PoolSelectionUnit nextPsu) {
        Collection<String> newGroups = diff.getNewGroups();
        for (String group : newGroups) {
            nextPsu.getPoolsByPoolGroup(group)
                  .stream()
                  .map(SelectionPool::getName)
                  .forEach((p) -> diff.poolsAdded.put(p, group));
            StorageUnitInfoExtractor.getStorageUnitsInGroup(group, nextPsu)
                  .stream()
                  .forEach((u) -> diff.unitsAdded.put(group, u.getName()));
        }
    }

    private void addPoolGroupsForNewUnits(PoolOpDiff diff, PoolSelectionUnit nextPsu) {
        Collection<StorageUnit> newUnits = diff.getNewUnits();
        for (StorageUnit unit : newUnits) {
            String name = unit.getName();
            StorageUnitInfoExtractor.getPrimaryGroupsFor(name, nextPsu)
                  .forEach((g) -> diff.unitsAdded.put(g, name));
        }
    }

    private void comparePoolGroupMarkers(PoolOpDiff diff,
          Set<String> common,
          PoolSelectionUnit currentPsu,
          PoolSelectionUnit nextPsu) {
        for (String group : common) {
            SelectionPoolGroup nextPoolGroup = nextPsu.getPoolGroups().get(group);
            SelectionPoolGroup currPoolGroup = currentPsu.getPoolGroups().get(group);
            if (nextPoolGroup.isPrimary() != currPoolGroup.isPrimary()) {
                diff.getOldGroups().add(group);
                diff.getNewGroups().add(nextPoolGroup.getName());

                /*
                 * Only rescan groups whose marker changed.
                 */
                diff.getMarkerChanged().add(group);
            }
        }
    }

    private void comparePoolMode(PoolOpDiff diff, Set<String> commonPools, PoolSelectionUnit psu) {
        /*
         *  First add the info for all new pools to the diff.
         */
        diff.getNewPools().stream()
              .forEach((p) -> {
                  diff.getModeChanged().put(p, psu.getPool(p).getPoolMode());
              });

        /*
         * Now check for differences with current pools that are still valid.
         */
        commonPools.stream()
              .forEach((p) -> {
                  PoolV2Mode newMode = psu.getPool(p).getPoolMode();
                  PoolQoSStatus oldStatus = poolOperationMap.getCurrentStatus(p);
                  PoolQoSStatus newStatus = PoolQoSStatus.valueOf(newMode);
                  if (newStatus != oldStatus) {
                      diff.getModeChanged().put(p, newMode);
                  }
              });
    }

    private void comparePoolsInPoolGroups(PoolOpDiff diff,
          Set<String> common,
          PoolSelectionUnit currentPsu,
          PoolSelectionUnit nextPsu) {
        for (String group : common) {
            Set<String> next = nextPsu.getPoolsByPoolGroup(group)
                  .stream()
                  .map(SelectionPool::getName)
                  .collect(Collectors.toSet());
            Set<String> curr = currentPsu.getPoolsByPoolGroup(group)
                  .stream()
                  .map(SelectionPool::getName)
                  .collect(Collectors.toSet());
            Sets.difference(next, curr).forEach((p) -> diff.poolsAdded.put(p, group));
            Sets.difference(curr, next).stream().filter((p) -> !diff.oldPools.contains(p))
                  .forEach((p) -> diff.poolsRmved.put(p, group));
        }
    }

    private void comparePoolTags(PoolOpDiff diff, Set<String> common, PoolMonitor current,
          PoolMonitor next) {
        CostModule currentCostModule = current.getCostModule();
        CostModule nextCostModule = next.getCostModule();

        diff.getNewPools()
              .forEach(p -> diff.getTagsChanged().put(p, getPoolTags(p, nextCostModule)));

        common.forEach(p -> {
            Map<String, String> newTags = getPoolTags(p, nextCostModule);
            Map<String, String> oldTags = getPoolTags(p, currentCostModule);
            if (oldTags == null || (newTags != null
                  && !oldTags.equals(newTags))) {
                diff.getTagsChanged().put(p, newTags);
            }
        });
    }

    private Set<String> compareStorageUnits(PoolOpDiff diff,
          PoolSelectionUnit currentPsu,
          PoolSelectionUnit nextPsu) {
        Set<String> next = nextPsu.getSelectionUnits().values()
              .stream()
              .filter(StorageUnit.class::isInstance)
              .map(SelectionUnit::getName)
              .collect(Collectors.toSet());
        Set<String> curr = currentPsu.getSelectionUnits().values()
              .stream()
              .filter(StorageUnit.class::isInstance)
              .map(SelectionUnit::getName)
              .collect(Collectors.toSet());
        Sets.difference(next, curr).stream().map(nextPsu::getStorageUnit)
              .forEach(diff.newUnits::add);
        Sets.difference(curr, next).forEach(diff.oldUnits::add);
        return Sets.intersection(next, curr);
    }

    private void compareStorageUnitLinksAndConstraints(PoolOpDiff diff,
          Set<String> common,
          PoolSelectionUnit currentPsu,
          PoolSelectionUnit nextPsu) {
        for (String unit : common) {
            StorageUnit nextUnit = nextPsu.getStorageUnit(unit);
            Set<String> next
                  = ImmutableSet.copyOf(StorageUnitInfoExtractor.getPoolGroupsFor(unit,
                  nextPsu,
                  false));
            StorageUnit currentUnit = currentPsu.getStorageUnit(unit);
            Set<String> curr
                  = ImmutableSet.copyOf(StorageUnitInfoExtractor.getPoolGroupsFor(unit,
                  currentPsu,
                  false));
            Sets.difference(next, curr).forEach(g -> diff.unitsAdded.put(g, unit));
            Sets.difference(curr, next).stream().filter(g -> !diff.oldGroups.contains(g))
                  .forEach(g -> diff.unitsRmved.put(g, unit));

            Integer required = nextUnit.getRequiredCopies();
            int newRequired = required == null ? -1 : required;
            required = currentUnit.getRequiredCopies();
            int oldRequired = required == null ? -1 : required;

            if (newRequired != oldRequired
                  || !nextUnit.getOnlyOneCopyPer().equals(currentUnit.getOnlyOneCopyPer())) {
                diff.constraintsChanged.add(unit);
            }
        }
    }

    private void getUninitializedPools(PoolOpDiff diff, PoolSelectionUnit currentPsu) {
        currentPsu.getPools().values()
              .stream()
              .filter(p -> PoolQoSStatus.valueOf(p.getPoolMode())
                    == PoolQoSStatus.UNINITIALIZED)
              .forEach(p -> diff.getUninitializedPools().add(p.getName()));
    }

    private Set<String> comparePools(PoolOpDiff diff,
          PoolSelectionUnit currentPsu,
          PoolSelectionUnit nextPsu) {
        Set<String> next = nextPsu.getPools().values().stream().map(SelectionPool::getName)
              .collect(Collectors.toSet());
        Set<String> curr = currentPsu.getPools().values().stream().map(SelectionPool::getName)
              .collect(Collectors.toSet());
        Sets.difference(next, curr).forEach(p -> diff.newPools.add(p));
        Sets.difference(curr, next).forEach(p -> diff.oldPools.add(p));
        return Sets.intersection(curr, next);
    }

    private Set<String> comparePoolGroups(PoolOpDiff diff,
          PoolSelectionUnit currentPsu,
          PoolSelectionUnit nextPsu) {
        Set<String> next = nextPsu.getPoolGroups().values()
              .stream()
              .map(SelectionPoolGroup::getName)
              .collect(Collectors.toSet());
        Set<String> curr = currentPsu.getPoolGroups().values()
              .stream()
              .map(SelectionPoolGroup::getName)
              .collect(Collectors.toSet());
        Sets.difference(next, curr).forEach(diff.newGroups::add);
        Sets.difference(curr, next).forEach(diff.oldGroups::add);
        return Sets.intersection(next, curr);
    }

    private Map<String, String> getPoolTags(String pool, CostModule costModule) {
        PoolInfo poolInfo = costModule.getPoolInfo(pool);
        if (poolInfo == null) {
            return null;
        }
        return poolInfo.getTags();
    }

    /**
     * Scans the "new" pool, also making sure all files have the sticky bit.
     */
    private void scanPoolAddedToPoolGroup(Entry<String, String> entry, PoolSelectionUnit psu) {
        String pool = entry.getKey();
        String addedTo = entry.getValue();
        PoolV2Mode mode = psu.getPool(pool).getPoolMode();
        poolOperationMap.add(pool);
        poolOperationMap.updateStatus(pool, PoolQoSStatus.valueOf(mode));
        scanPool(pool, addedTo, null, mode);
    }

    /**
     * We allow the scanner to react to changes in the "primary" status of a pool group.
     */
    private void scanPoolsOfModifiedPoolGroup(String poolGroupName, PoolSelectionUnit psu) {
        psu.getPoolsByPoolGroup(poolGroupName)
              .forEach(p -> scanPool(p.getName(), null, null, p.getPoolMode()));
    }

    /**
     * NB: if we try to scan the pool as DOWN, this means we need to pass the old group id for the
     * pool, because we cannot synchronize the scan + adjustment tasks such as to create a barrier
     * so that we can remove the pool from the map after everything completes.
     */
    private void scanPoolRemovedFromPoolGroup(Entry<String, String> entry, PoolSelectionUnit psu) {
        String pool = entry.getKey();
        String removedFrom = entry.getValue();
        PoolV2Mode mode = psu.getPool(pool).getPoolMode();
        scanPool(entry.getKey(), null, removedFrom, mode);
    }

    /**
     * Will skip the grace period wait, but still take into consideration whether the pool has
     * already been scanned because it went DOWN, or whether it is EXCLUDED.
     */
    private void scanPool(String pool, String addedTo, String removedFrom, PoolV2Mode mode) {
        if (poolOperationMap.isInitialized(mode)) {
            poolOperationMap.scan(pool, addedTo, removedFrom, null, mode, false);
        }
    }

    /**
     * Will skip the grace period wait, as well as transition checks.
     */
    private void scanPool(String pool, String unit, PoolV2Mode mode) {
        poolOperationMap.scan(pool, null, null, unit, mode, true);
    }

    private void scanPoolsInGroup(String poolGroupName, String unit, PoolSelectionUnit psu) {
        psu.getPoolsByPoolGroup(poolGroupName)
              .forEach(p -> scanPool(p.getName(), unit, p.getPoolMode()));

    }

    private void scanPoolsWithStorageUnitModified(String unit, PoolSelectionUnit psu) {
        StorageUnitInfoExtractor.getPoolGroupsFor(unit, psu, false)
              .forEach((group) -> scanPoolsInGroup(group, unit, psu));
    }
}
