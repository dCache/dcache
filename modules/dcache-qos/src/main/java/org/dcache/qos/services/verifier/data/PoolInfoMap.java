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
package org.dcache.qos.services.verifier.data;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;
import diskCacheV111.poolManager.CostModule;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionLink;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnit;
import diskCacheV111.poolManager.StorageUnit;
import diskCacheV111.poolManager.StorageUnitInfoExtractor;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.poolmanager.PoolInfo;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.qos.services.verifier.util.AbstractLocationExtractor;
import org.dcache.qos.services.verifier.util.RandomSelectionStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serves as an index of PoolSelectionUnit-related information, digested into a format which is more
 * convenient for the verifier service.
 * <p/>
 * The relational tables represented by multimaps of indices capture pool, pool group, storage unit
 * and hsm membership. There are also maps which define the constraints for a given storage unit,
 * the tags for a pool, and the live mode and cost information for a pool.
 * <p/>
 * This class also provides methods for determining the primary group of a given pool, the storage
 * groups connected to a given pool group, and whether a pool group can satisfy the constraints
 * defined by the storage units bound to it.
 * <p/>
 * {@link #apply(PoolInfoDiff)} empties and rebuilds pool-related information based on a diff
 * obtained from {@link #compare(PoolMonitor)}.  An empty diff is equivalent to a load operation (at
 * initialization). These methods are called by the map initializer at startup, and thereafter by
 * the PoolMonitorChangeHandler.
 * <p/>
 * This class benefits from read-write synchronization, since there will be many more reads of what
 * is for the most part stable information (note that the periodically refreshed information is
 * synchronized within the PoolInformation object itself; hence changes to those values do not
 * require a write lock; e.g., #updatePoolStatus.)
 * <p/>
 * Class is not marked final for stubbing/mocking purposes.
 */
public class PoolInfoMap {

    /*
     *  When there are no preferential groups to which a pool belongs, qos will
     *  use the entire set of pools to choose from.
     *
     *  Caller does not need to know this name.  Randomized to make sure it does not
     *  clash with a real pool group name.
     */
    public static final String SYSTEM_PGROUP = UUID.randomUUID().toString();

    public static final RateLimiter LIMITER = RateLimiter.create(0.001);

    private static final Logger LOGGER = LoggerFactory.getLogger(PoolInfoMap.class);

    private static final String MISCONFIGURED_POOL_GROUP_ALARM
          = "QoS has detected that pool group {} does not have enough pools to "
          + "meet the requirements of files linked to it.";

    public static void sendPoolGroupMisconfiguredAlarm(String poolGroup) {
        /*
         *  Create a new alarm every hour by keying the alarm to
         *  an hourly timestamp.  Otherwise, the alarm counter will
         *  just be updated for each alarm sent.  The rate limiter
         *  will not alarm more than once every 1000 seconds (once every
         *  15 minutes).
         */
        if (LIMITER.tryAcquire()) {
            LOGGER.warn(AlarmMarkerFactory.getMarker(PredefinedAlarm.POOL_GROUP_ISSUE,
                        Instant.now().truncatedTo(ChronoUnit.HOURS).toString()),
                  MISCONFIGURED_POOL_GROUP_ALARM, poolGroup);
        }
    }

    /*
     *  Uses pool tags as discriminator keys.
     */
    class LocationExtractor extends AbstractLocationExtractor {

        LocationExtractor(Collection<String> onlyOneCopyPer) {
            super(onlyOneCopyPer);
        }

        @Override
        protected Map<String, String> getKeyValuesFor(String location) {
            return getTags(location);
        }
    }

    private final Set<String> pools = new HashSet<>();
    private final Map<String, PrimaryGroupMarker> markers = new HashMap<>();
    private final Map<String, StorageUnitConstraints> constraints = new HashMap<>();
    private final Map<String, PoolInformation> poolInfo = new HashMap<>();
    private final Multimap<String, String> poolGroupToPool = HashMultimap.create();
    private final Multimap<String, String> poolToPoolGroup = HashMultimap.create();
    private final Multimap<String, String> storageToPoolGroup = HashMultimap.create();
    private final Multimap<String, String> poolGroupToStorage = HashMultimap.create();
    private final Multimap<String, String> poolToHsm = HashMultimap.create();
    private final Set<String> readPref0Pools = new HashSet<>();

    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final Lock write = lock.writeLock();
    private final Lock read = lock.readLock();

    private int verifyWarnings = 0;

    /**
     * Called on a dedicated thread. Applies a diff under write lock.
     */
    public void apply(PoolInfoDiff diff) {
        write.lock();
        verifyWarnings = 0;
        try {
            /*
             *  -- Remove stale pools, pool groups and storage units.
             *
             *     Pool removal assumes that the pool has been properly drained
             *     of files first; this is not taken care of here.  Similarly
             *     for the removal of pool groups.
             *
             *     If an old pool group has a changed marker, it is also removed.
             */
            LOGGER.trace("removing stale pools, pool groups and storage units");
            diff.getOldPools().forEach(this::removePool);
            diff.getOldGroups().forEach(this::removeGroup);
            diff.getOldUnits().forEach(this::removeUnit);

            /*
             *  -- Remove pools from current groups.
             *
             *     If an old group has a changed marker, it is readded as new.
             */
            LOGGER.trace("removing pools from pool groups");
            diff.getPoolsRemovedFromPoolGroup().entries()
                  .forEach(this::removeFromPoolGroup);

            /*
             *  -- Remove units from current groups.
             */
            LOGGER.trace("removing units from pool groups");
            diff.getUnitsRemovedFromPoolGroup().entries()
                  .forEach(this::removeStorageUnit);

            /*
             *  -- Add new storage units, pool groups and pools.
             */
            LOGGER.trace("adding new storage units, pool groups and pools");
            diff.getNewUnits().forEach(this::addStorageUnit);
            diff.getNewGroups().forEach(this::addPoolGroup);
            diff.getNewPools().forEach(this::addPool);

            /*
             *  -- Add units to pool groups.
             */
            LOGGER.trace("adding storage units to pool groups");
            diff.getUnitsAddedToPoolGroup().entries()
                  .forEach(this::addUnitToPoolGroup);

            /*
             *  -- Add pools to pool groups.
             */
            LOGGER.trace("adding pools to pool groups");
            diff.getPoolsAddedToPoolGroup().entries()
                  .forEach(this::addPoolToPoolGroups);

            /*
             *  -- Modify constraints.
             */
            LOGGER.trace("modifying storage unit constraints");
            diff.getConstraints().entrySet().forEach(this::updateConstraints);

            /*
             *  -- Add pool information for the new pools.
             *
             *     The new pools are the only ones to have
             *     entries in the cost info map.
             */
            LOGGER.trace("adding live pool information");
            diff.getPoolCost().keySet()
                  .forEach(p -> setPoolInfo(p, diff.getModeChanged().get(p),
                        diff.getTagsChanged().get(p),
                        diff.poolCost.get(p)));

            /*
             *  -- Add HSMs.
             */
            LOGGER.trace("adding hsm pool information");
            diff.getHsmsChanged().entrySet()
                  .forEach(e -> updateHsms(e.getKey(), e.getValue()));

            /*
             *  -- Update the readPref0 pools.
             */
            LOGGER.trace("updating set of pools for which there are no links with read pref = 0");
            readPref0Pools.clear();
            diff.getReadPref0().forEach(readPref0Pools::add);

            LOGGER.trace("verifying new pool groups");
            diff.getNewGroups().forEach(pg -> {
                try {
                    verifyConstraints(pg.getName());
                } catch (IllegalStateException e) {
                    sendPoolGroupMisconfiguredAlarm(pg.getName());
                    ++verifyWarnings;
                }
            });
        } finally {
            write.unlock();
        }
    }

    /**
     * Called on dedicated thread.
     * <p/>
     * Does a diff under read lock.
     * <p/>
     * Gathers new pools, removed pools, new pool groups, removed pool groups, new storage units,
     * removed storage units, modified groups and storage units, and pool information changes.
     *
     * @param poolMonitor received from pool manager message.
     * @return the diff (parsed out into relevant maps and collections).
     */
    public PoolInfoDiff compare(PoolMonitor poolMonitor) {
        PoolSelectionUnit psu = poolMonitor.getPoolSelectionUnit();
        PoolInfoDiff diff = new PoolInfoDiff();
        read.lock();
        try {
            LOGGER.trace("Searching for currently uninitialized pools.");
            getUninitializedPools(diff);

            LOGGER.trace("comparing pools");
            Set<String> commonPools = comparePools(diff, psu);

            LOGGER.trace("comparing pool groups");
            Set<String> commonGroups = comparePoolGroups(diff, psu);

            LOGGER.trace("adding pools and units to new pool groups");
            addPoolsAndUnitsToNewPoolGroups(diff, psu);

            LOGGER.trace("comparing pools in pool groups");
            comparePoolsInPoolGroups(diff, commonGroups, psu);

            LOGGER.trace("find pool group marker changes");
            comparePoolGroupMarkers(diff, commonGroups, psu);

            LOGGER.trace("comparing storage units");
            commonGroups = compareStorageUnits(diff, psu);

            LOGGER.trace("adding pool groups for new storage units");
            addPoolGroupsForNewUnits(diff, psu);

            LOGGER.trace("comparing storage unit links and constraints");
            compareStorageUnitLinksAndConstraints(diff, commonGroups, psu);

            LOGGER.trace("comparing pool info");
            comparePoolInfo(diff, commonPools, poolMonitor);
        } finally {
            read.unlock();
        }

        LOGGER.trace("Diff:\n{}", diff);
        return diff;
    }

    /**
     * Only for testing.
     */
    @VisibleForTesting
    public StorageUnitConstraints getConstraints(String unit) {
        read.lock();
        try {
            return constraints.get(unit);
        } finally {
            read.unlock();
        }
    }

    @VisibleForTesting
    public int verifyWarnings() {
        return verifyWarnings;
    }

    public Set<String> getExcludedLocationNames(Collection<String> members) {
        read.lock();
        try {
            return members.stream()
                  .map(loc -> poolInfo.get(loc))
                  .filter(Objects::nonNull)
                  .filter(PoolInformation::isInitialized)
                  .filter(PoolInformation::isExcluded)
                  .map(PoolInformation::getName)
                  .collect(Collectors.toSet());
        } finally {
            read.unlock();
        }
    }

    public Set<String> getHsmPoolsForStorageUnit(String sunit, Set<String> hsms) {
        read.lock();
        try {
            Predicate<String> hasHsm = p -> poolToHsm.get(p)
                  .stream()
                  .filter(hsms::contains)
                  .count() != 0;

            Stream<String> hsmPools;

            if (sunit == null) {
                hsmPools = pools.stream().filter(hasHsm);
            } else {
                hsmPools = storageToPoolGroup.get(sunit)
                      .stream()
                      .map(poolGroupToPool::get)
                      .flatMap(pools -> pools.stream())
                      .filter(hasHsm);
            }

            return hsmPools.filter(pool -> isPoolViable(pool, true))
                  .collect(Collectors.toSet());
        } finally {
            read.unlock();
        }
    }

    /**
     * Uses pool tags as discriminator keys.
     */
    public AbstractLocationExtractor getLocationExtractor(Collection<String> oneCopyPer) {
        return new LocationExtractor(oneCopyPer);
    }

    public Set<String> getMemberLocations(String group, Collection<String> locations) {
        if (group == null || SYSTEM_PGROUP.equals(group)) {
            return ImmutableSet.copyOf(locations);
        }

        read.lock();
        try {
            Set<String> ofGroup = poolGroupToPool.get(group)
                  .stream()
                  .collect(Collectors.toSet());
            return locations.stream().filter(ofGroup::contains).collect(Collectors.toSet());
        } finally {
            read.unlock();
        }
    }

    /**
     * @param writable location is writable and readable if true, only readable if false.
     * @return all pool group pools which qualify.
     */
    public Set<String> getMemberPools(String pgroup, boolean writable) {
        read.lock();
        try {
            Stream<String> members;
            if (pgroup == null || SYSTEM_PGROUP.equals(pgroup)) {
                members = pools.stream();
            } else {
                members = poolGroupToPool.get(pgroup).stream();
            }
            return ImmutableSet.copyOf(members.filter(p -> viable(p, writable))
                  .collect(Collectors.toSet()));
        } finally {
            read.unlock();
        }
    }

    public PoolManagerPoolInformation getPoolManagerInfo(String pool) {
        read.lock();
        try {
            return new PoolManagerPoolInformation(pool, poolInfo.get(pool).getCostInfo());
        } finally {
            read.unlock();
        }
    }

    public Collection<String> getPoolsOfGroup(String group) {
        read.lock();
        try {
            if (group == null || SYSTEM_PGROUP.equals(group)) {
                return pools.stream().collect(Collectors.toList());
            }
            return poolGroupToPool.get(group);
        } finally {
            read.unlock();
        }
    }

    public Set<String> getReadableLocations(Collection<String> locations) {
        read.lock();
        try {
            return locations.stream()
                  .filter(location -> viable(location, false))
                  .collect(Collectors.toSet());
        } finally {
            read.unlock();
        }
    }

    /**
     * The effective pool group for an operation has to do with whether a location associated with
     * the file belongs to a primary group or not.
     * <p/>
     * For a single location, if it belongs to a set of pool groups, but among them there is a sole
     * primary group, then that primary group must be used.  Otherwise, we always select from among
     * all available pools, designated SYSTEM_GROUP. This is true even if the location belongs to a
     * single pool group.
     * <p/>
     * More simply, selection of targets for copying replicas ignores pool groups except in the case
     * when the file is linked to one and only one primary group.
     * <p/>
     * When the replicas are already multiple and spread out, in order to remain consistent and
     * avoid thrashing between caching and setting the sticky bit, we must make sure we pick the
     * widest group.  That is, if the file was originally in a primary group, but then somehow
     * acquired locations outside that group, we must promote the files group to SYSTEM_PGROUP.  So
     * we examine all of the current file locations.  This reduces to the simple check of a single
     * location's group in the case of the first replica.
     *
     * @param locations all current locations for the file this operation represents.
     * @return the effective pool group for that operation/file.
     */
    public String getEffectivePoolGroup(Collection<String> locations) throws IllegalStateException {
        read.lock();
        try {
            Set<String> groups = locations.stream()
                  .map(this::effectivePoolGroupOf)
                  .collect(Collectors.toSet());
            if (groups.isEmpty() || groups.size() > 1) {
                return SYSTEM_PGROUP;
            }
            return groups.iterator().next();
        } finally {
            read.unlock();
        }
    }

    /**
     * See above.  Single file version.
     *
     * @param pool for which we need the effective pool group.
     * @return the group
     */
    public String getEffectivePoolGroup(String pool) throws IllegalStateException {
        read.lock();
        try {
            return effectivePoolGroupOf(pool);
        } finally {
            read.unlock();
        }
    }

    public Set<String> getStorageUnitsForGroup(String group) {
        read.lock();
        try {
            return poolGroupToStorage.get(group)
                  .stream()
                  .collect(Collectors.toSet());
        } finally {
            read.unlock();
        }
    }

    public Map<String, String> getTags(String pool) {
        PoolInformation info;

        read.lock();
        try {
            info = poolInfo.get(pool);
        } finally {
            read.unlock();
        }

        if (info == null) {
            return ImmutableMap.of();
        }

        Map<String, String> tags = info.getTags();
        if (tags == null) {
            return ImmutableMap.of();
        }

        return tags;
    }

    public Set<String> getValidLocations(Collection<String> locations, boolean writable) {
        read.lock();
        try {
            return locations.stream()
                  .filter(loc -> viable(loc, writable))
                  .collect(Collectors.toSet());
        } finally {
            read.unlock();
        }
    }

    public boolean isReadPref0(String pool) {
        read.lock();
        try {
            return readPref0Pools.contains(pool);
        } finally {
            read.unlock();
        }
    }

    public boolean isPoolDraining(String pool) {
        read.lock();
        try {
            PoolInformation info = poolInfo.get(pool);
            if (info == null || info.getMode() == null) {
                return false;
            }
            return info.getMode().isDisabled(PoolV2Mode.DRAINING);
        } finally {
            read.unlock();
        }
    }

    public boolean isPoolViable(String pool, boolean writable) {
        read.lock();
        try {
            return viable(pool, writable);
        } finally {
            read.unlock();
        }
    }

    public boolean isInitialized(String pool) {
        read.lock();
        try {
            PoolInformation info = poolInfo.get(pool);
            return info != null && info.isInitialized();
        } catch (NoSuchElementException e) {
            return false;
        } finally {
            read.unlock();
        }
    }

    public boolean isEnabled(String pool) {
        read.lock();
        try {
            PoolInformation info = poolInfo.get(pool);
            return info != null && info.getMode().isEnabled();
        } catch (NoSuchElementException e) {
            return false;
        } finally {
            read.unlock();
        }
    }

    /**
     * @Admin
     */
    public String listPoolInfo(PoolInfoFilter poolInfoFilter) {
        final StringBuilder builder = new StringBuilder();
        read.lock();
        try {
            pools.stream()
                  .map(poolInfo::get)
                  .filter(poolInfoFilter::matches)
                  .forEach(name -> builder.append(name).append("\n"));
        } finally {
            read.unlock();
        }
        return builder.toString();
    }

    /**
     * All unit tests are synchronous, so there is no need to lock the map here.
     */
    @VisibleForTesting
    public void setUnitConstraints(String group, Integer required,
          Collection<String> oneCopyPer) {
        constraints.put(group, new StorageUnitConstraints(required, oneCopyPer));
    }

    public void updatePoolInfo(String pool, boolean excluded) {
        write.lock();
        try {
            PoolInformation info = poolInfo.get(pool);
            if (info != null) {
                info.setExcluded(excluded);
            }
        } finally {
            write.unlock();
        }
    }

    /*
     *  Used in testing only.
     */
    @VisibleForTesting
    public void updatePoolMode(String pool, PoolV2Mode mode) {
        write.lock();
        try {
            PoolInformation info = poolInfo.get(pool);
            if (info == null) {
                info = new PoolInformation(pool);
                poolInfo.put(pool, info);
            }
            info.update(mode, null, null);
        } finally {
            write.unlock();
        }
    }

    @VisibleForTesting
        /* Only used by unit test */
    public boolean hasGroup(String group) {
        return markers.get(group) != null;
    }

    @VisibleForTesting
        /* Only used by unit test */
    public boolean hasPool(String pool) {
        return pools.contains(pool);
    }

    @VisibleForTesting
        /* Only used by unit test */
    public boolean hasUnit(String unit) {
        return constraints.containsKey(unit);
    }

    @VisibleForTesting
    /** Called under write lock **/
    void removeGroup(String group) {
        markers.remove(group);
        poolGroupToPool.removeAll(group).forEach(p -> poolToPoolGroup.remove(p, group));
        poolGroupToStorage.removeAll(group).forEach(g -> storageToPoolGroup.remove(g, group));
    }

    @VisibleForTesting
    /** Called under write lock except during unit test**/
    void removePool(String pool) {
        pools.remove(pool);
        poolToPoolGroup.removeAll(pool).forEach(g -> poolGroupToPool.remove(g, pool));
        poolInfo.remove(pool);
        poolToHsm.removeAll(pool);
    }

    @VisibleForTesting
    /** Called under write lock except during unit test **/
    void removeUnit(String unit) {
        constraints.remove(unit);
        storageToPoolGroup.removeAll(unit).forEach(g -> poolGroupToStorage.remove(g, unit));
    }

    /**
     * Called under write lock
     **/
    private void addPool(SelectionPool pool) {
        pools.add(pool.getName());
    }

    /**
     * Called under write lock
     **/
    private void addPoolGroup(SelectionPoolGroup group) {
        markers.put(group.getName(), new PrimaryGroupMarker(group.isPrimary()));
    }

    /**
     * Called under write lock
     **/
    private void addPoolGroupsForNewUnits(PoolInfoDiff diff,
          PoolSelectionUnit psu) {
        Collection<StorageUnit> newUnits = diff.getNewUnits();
        for (StorageUnit unit : newUnits) {
            String name = unit.getName();
            StorageUnitInfoExtractor.getPoolGroupsFor(name, psu, false)
                  .stream()
                  .forEach(g -> diff.unitsAdded.put(g, name));
        }
    }

    /**
     * Called under write lock
     **/
    private void addPoolToPoolGroups(Entry<String, String> entry) {
        String pool = entry.getKey();
        String group = entry.getValue();
        poolGroupToPool.put(group, pool);
        poolToPoolGroup.put(pool, group);
    }

    /**
     * Called under write lock
     **/
    private void addPoolsAndUnitsToNewPoolGroups(PoolInfoDiff diff,
          PoolSelectionUnit psu) {
        Collection<SelectionPoolGroup> newGroups = diff.getNewGroups();
        for (SelectionPoolGroup group : newGroups) {
            String name = group.getName();
            psu.getPoolsByPoolGroup(name)
                  .stream()
                  .map(SelectionPool::getName)
                  .forEach(p -> diff.poolsAdded.put(p, name));
            StorageUnitInfoExtractor.getStorageUnitsInGroup(name, psu)
                  .forEach(u -> diff.unitsAdded.put(name, u.getName()));
        }
    }

    /**
     * Called under write lock
     **/
    private void addStorageUnit(StorageUnit unit) {
        constraints.put(unit.getName(), new StorageUnitConstraints(unit.getRequiredCopies(),
              unit.getOnlyOneCopyPer()));
    }

    /**
     * Called under write lock
     **/
    private void addUnitToPoolGroup(Entry<String, String> entry) {
        String group = entry.getKey();
        String unit = entry.getValue();
        storageToPoolGroup.put(unit, group);
        poolGroupToStorage.put(group, unit);
    }

    /**
     * Called under read lock
     **/
    private Set<String> comparePoolGroups(PoolInfoDiff diff,
          PoolSelectionUnit psu) {
        Set<String> next = psu.getPoolGroups().values()
              .stream()
              .map(SelectionPoolGroup::getName)
              .collect(Collectors.toSet());
        Set<String> curr = poolGroupToPool.keySet();
        Sets.difference(next, curr).stream()
              .map(name -> psu.getPoolGroups().get(name))
              .forEach(diff.newGroups::add);
        Sets.difference(curr, next).forEach(diff.oldGroups::add);
        return Sets.intersection(next, curr);
    }

    /**
     * Called under read lock
     **/
    private void comparePoolInfo(PoolInfoDiff diff,
          Set<String> commonPools,
          PoolMonitor poolMonitor) {
        PoolSelectionUnit psu = poolMonitor.getPoolSelectionUnit();
        CostModule costModule = poolMonitor.getCostModule();

        /*
         *  First add the info for all new pools to the diff.
         */
        diff.getNewPools().stream()
              .map(SelectionPool::getName)
              .forEach(pool -> {
                  diff.getModeChanged().put(pool, getPoolMode(psu.getPool(pool)));
                  diff.getTagsChanged().put(pool, getPoolTags(pool, costModule));
                  diff.getPoolCost().put(pool, getPoolCostInfo(pool, costModule));
                  diff.getHsmsChanged().put(pool, psu.getPool(pool).getHsmInstances());
                  checkReadPrefs(pool, diff.getReadPref0(), psu);
              });

        /*
         *  Now check for differences with current pools that are still valid.
         */
        commonPools.forEach(pool -> {
            PoolInformation info = poolInfo.get(pool);
            PoolV2Mode newMode = getPoolMode(psu.getPool(pool));
            PoolV2Mode oldMode = info.getMode();
            if (oldMode == null ||
                  (newMode != null && !oldMode.equals(newMode))) {
                diff.getModeChanged().put(pool, newMode);
            }

            Map<String, String> newTags = getPoolTags(pool, costModule);
            Map<String, String> oldTags = info.getTags();
            if (oldTags == null ||
                  (newTags != null && !oldTags.equals(newTags))) {
                diff.getTagsChanged().put(pool, newTags);
            }

            /*
             *  Since we are not altering the actual collections inside
             *  the PoolInfoMap, but are simply modifying the PoolInformation
             *  object, and since its own update method is synchronized,
             *  we can take care of the update here while holding a read lock.
             */
            info.update(newMode, newTags, getPoolCostInfo(pool, costModule));

            /*
             *  HSM info may not be present when the pool was added, so
             *  we readd it here.
             */
            diff.getHsmsChanged().put(pool, psu.getPool(pool).getHsmInstances());

            /*
             *  Pool could have been relinked.
             */
            checkReadPrefs(pool, diff.getReadPref0(), psu);
        });

    }

    /**
     * Called under read lock
     **/
    private void checkReadPrefs(String pool, Set<String> readPref0, PoolSelectionUnit psu) {
        List<SelectionLink> links = psu.getPoolGroupsOfPool(pool)
              .stream()
              .map(g -> psu.getLinksPointingToPoolGroup(g.getName()))
              .flatMap(c -> c.stream())
              .collect(Collectors.toList());
        if (links.isEmpty()) {
            return;
        }

        for (SelectionLink link : links) {
            if (link.getPreferences().getReadPref() != 0) {
                return;
            }
        }

        readPref0.add(pool);
    }

    /**
     * Called under read lock
     **/
    private Set<String> comparePools(PoolInfoDiff diff, PoolSelectionUnit psu) {
        Set<String> next = psu.getPools().values().stream()
              .map(SelectionPool::getName)
              .collect(Collectors.toSet());
        Sets.difference(next, pools).stream().map(psu::getPool).forEach(diff.newPools::add);
        Sets.difference(pools, next).forEach(diff.oldPools::add);
        return Sets.intersection(pools, next);
    }

    /**
     * Called under read lock
     **/
    private void comparePoolGroupMarkers(PoolInfoDiff diff, Set<String> common,
          PoolSelectionUnit psu) {
        for (String group : common) {
            SelectionPoolGroup selectionPoolGroup = psu.getPoolGroups().get(group);
            PrimaryGroupMarker marker = markers.get(group);
            if (selectionPoolGroup.isPrimary() != marker.isPrimary()) {
                diff.getOldGroups().add(group);
                diff.getNewGroups().add(selectionPoolGroup);

                /*
                 * Only rescan groups whose marker changed.
                 */
                diff.getMarkerChanged().add(group);
            }
        }
    }

    /**
     * Called under read lock
     **/
    private void comparePoolsInPoolGroups(PoolInfoDiff diff,
          Set<String> common,
          PoolSelectionUnit psu) {
        for (String group : common) {
            Set<String> next = psu.getPoolsByPoolGroup(group)
                  .stream()
                  .map(SelectionPool::getName)
                  .collect(Collectors.toSet());
            Set<String> curr = poolGroupToPool.get(group).stream().collect(Collectors.toSet());
            Sets.difference(next, curr).forEach(pool -> diff.poolsAdded.put(pool, group));
            Sets.difference(curr, next)
                  .stream()
                  .filter(pool -> !diff.oldPools.contains(pool))
                  .forEach(pool -> diff.poolsRmved.put(pool, group));
        }
    }

    /**
     * Called under read lock
     **/
    private void compareStorageUnitLinksAndConstraints(PoolInfoDiff diff,
          Set<String> common,
          PoolSelectionUnit psu) {
        for (String unit : common) {
            StorageUnit storageUnit = psu.getStorageUnit(unit);
            Set<String> next
                  = ImmutableSet.copyOf(
                  StorageUnitInfoExtractor.getPoolGroupsFor(unit, psu, false));
            Set<String> curr = storageToPoolGroup.get(unit)
                  .stream()
                  .collect(Collectors.toSet());
            Sets.difference(next, curr).forEach(group -> diff.unitsAdded.put(group, unit));
            Sets.difference(curr, next)
                  .stream()
                  .filter(group -> !diff.oldGroups.contains(group))
                  .forEach(group -> diff.unitsRmved.put(group, unit));

            Integer required = storageUnit.getRequiredCopies();
            int newRequired = required == null ? -1 : required;

            StorageUnitConstraints constraints = this.constraints.get(unit);
            int oldRequired = !constraints.hasRequirement() ? -1 : constraints.getRequired();

            Set<String> oneCopyPer = ImmutableSet.copyOf(storageUnit.getOnlyOneCopyPer());

            if (newRequired != oldRequired || !oneCopyPer.equals(constraints.getOneCopyPer())) {
                diff.constraints.put(unit, new StorageUnitConstraints(required, oneCopyPer));
            }
        }
    }

    /**
     * Called under read lock
     **/
    private Set<String> compareStorageUnits(PoolInfoDiff diff,
          PoolSelectionUnit psu) {
        Set<String> next = psu.getSelectionUnits().values()
              .stream()
              .filter(StorageUnit.class::isInstance)
              .map(SelectionUnit::getName)
              .collect(Collectors.toSet());
        Set<String> curr = storageToPoolGroup.keySet();
        Sets.difference(next, curr).stream()
              .map(psu::getStorageUnit)
              .forEach(diff.newUnits::add);
        Sets.difference(curr, next).forEach(diff.oldUnits::add);
        return Sets.intersection(next, curr);
    }

    private String effectivePoolGroupOf(String pool) throws IllegalStateException {
        Optional<String> primary = getPrimaryGroup(pool, poolToPoolGroup.get(pool));
        if (primary.isPresent()) {
            return primary.get();
        }
        return SYSTEM_PGROUP;
    }

    private PoolV2Mode getPoolMode(SelectionPool pool) {
        /*
         *  Allow a NULL value
         */
        return pool.getPoolMode();
    }

    private ImmutableMap<String, String> getPoolTags(String pool, CostModule costModule) {
        PoolInfo poolInfo = costModule.getPoolInfo(pool);
        if (poolInfo == null) {
            return null;
        }
        return poolInfo.getTags();
    }

    private PoolCostInfo getPoolCostInfo(String pool, CostModule costModule) {
        /*
         *  Allow a NULL value
         */
        return costModule.getPoolCostInfo(pool);
    }

    /**
     * called under read lock
     **/
    private Optional<String> getPrimaryGroup(String pool, Collection<String> groups)
          throws IllegalStateException {
        Collection<String> primary = groups.stream()
              .filter(group -> markers.get(group).isPrimary())
              .collect(Collectors.toList());

        if (primary.size() > 1) {
            throw new IllegalStateException(String.format(
                  "Pool map is inconsistent; pool %s belongs to "
                        + "more than one primary "
                        + "group: %s.",
                  pool, primary.stream().collect(Collectors.toList())));
        }

        LOGGER.trace("number of primary pool groups for pool {}: {}.", pool, primary.size());

        if (primary.size() == 1) {
            return primary.stream().findFirst();
        }

        return Optional.empty();
    }

    private void getUninitializedPools(PoolInfoDiff diff) {
        pools.stream()
              .filter(pool -> !isInitialized(pool))
              .forEach(diff.uninitPools::add);
    }

    /**
     * Called under write lock
     **/
    private void removeFromPoolGroup(Entry<String, String> entry) {
        String pool = entry.getKey();
        String group = entry.getValue();
        poolGroupToPool.remove(group, pool);
        poolToPoolGroup.remove(pool, group);
    }

    private void removeStorageUnit(Entry<String, String> entry) {
        String unit = entry.getValue();
        String group = entry.getKey();
        storageToPoolGroup.remove(unit, group);
        poolGroupToStorage.remove(group, unit);
    }

    /**
     * Called under write lock
     **/
    private PoolInformation setPoolInfo(String pool,
          PoolV2Mode mode,
          Map<String, String> tags,
          PoolCostInfo cost) {
        PoolInformation entry = poolInfo.getOrDefault(pool, new PoolInformation(pool));
        entry.update(mode, tags, cost);
        poolInfo.put(pool, entry);
        return entry;
    }

    /**
     * Called under write lock
     **/
    private void updateConstraints(Entry<String, StorageUnitConstraints> entry) {
        constraints.put(entry.getKey(), entry.getValue());
    }

    /**
     * Called under write lock
     **/
    private void updateHsms(String pool, Set<String> hsms) {
        if (hsms.isEmpty()) {
            poolToHsm.removeAll(pool);
        } else {
            hsms.stream().forEach(hsm -> poolToHsm.put(pool, hsm));
        }
    }

    /**
     * Called under read lock
     *
     * @param pgroup    pool group.
     * @param extractor configured for the specific tag constraints.
     * @param required  specific to this group or storage unit.
     * @throws IllegalStateException upon encountering the first set of constraints which cannot be
     *                               met.
     */
    private void verify(String pgroup, AbstractLocationExtractor extractor, int required)
          throws IllegalStateException {
        Stream<String> poolsOfGroup;

        if (SYSTEM_PGROUP.equals(pgroup)) {
            poolsOfGroup = pools.stream();
        } else {
            poolsOfGroup = poolGroupToPool.get(pgroup).stream();
        }

        Set<String> members = poolsOfGroup.collect(Collectors.toSet());

        for (int i = 0; i < required; i++) {
            Collection<String> candidates = extractor.getCandidateLocations(members);
            if (candidates.isEmpty()) {
                throw new IllegalStateException("No candidate locations for "
                      + (SYSTEM_PGROUP.equals(pgroup) ?
                      "any pool" : pgroup));
            }
            String selected = RandomSelectionStrategy.SELECTOR.apply(candidates);
            members.remove(selected);
            extractor.addSeenTagsFor(selected);
        }
    }

    /**
     * A coarse-grained verification that the required and tag constraints of the pool group and its
     * associated storage groups can be met. For the default and each storage unit, it attempts to
     * fulfill the  max independent location requirement via the LocationExtractor.
     *
     * @throws IllegalStateException upon encountering the first set of constraints which cannot be
     *                               met.
     */
    private void verifyConstraints(String pgroup) throws IllegalStateException {
        Collection<String> sunits;
        AbstractLocationExtractor extractor;

        read.lock();
        try {
            sunits = poolGroupToStorage.get(pgroup);
            for (String sunit : sunits) {
                StorageUnitConstraints unitConstraints = constraints.get(sunit);
                int required = unitConstraints.getRequired();
                extractor = getLocationExtractor(unitConstraints.getOneCopyPer());
                verify(pgroup, extractor, required);
            }
        } finally {
            read.unlock();
        }
    }

    private boolean viable(String pool, boolean writable) {
        PoolInformation info = poolInfo.get(pool);
        return info != null && info.isInitialized()
              && (writable ? info.canRead() && info.canWrite() : info.canRead());
    }
}