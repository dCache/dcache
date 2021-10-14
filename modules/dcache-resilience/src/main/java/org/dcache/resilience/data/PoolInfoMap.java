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
package org.dcache.resilience.data;

import static org.dcache.util.NonReindexableList.safeGet;
import static org.dcache.util.NonReindexableList.safeIndexOf;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import diskCacheV111.poolManager.CostModule;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnit;
import diskCacheV111.poolManager.StorageUnit;
import diskCacheV111.poolManager.StorageUnitInfoExtractor;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.dcache.poolmanager.PoolInfo;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.resilience.handlers.PoolInfoChangeHandler;
import org.dcache.resilience.util.CopyLocationExtractor;
import org.dcache.resilience.util.MapInitializer;
import org.dcache.resilience.util.RandomSelectionStrategy;
import org.dcache.util.NonReindexableList;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Serves as the central locus of pool-related information.</p>
 *
 * <p>The internal data structures hold a list of pool names and pool groups
 * which will always assign a new index number to a new member even if some of the current members
 * happen to be deleted via a PoolSelectionUnit operation.</p>
 *
 * <p>The reason for doing this is to be able to store most of the pool info
 * associated with a given file or pool operation in progress as index references, allowing the
 * {@link FileOperation} to use 4-byte rather than 8-byte 'references'.</p>
 *
 * <p>The relational tables represented by multimaps of indices
 * capture pool and storage unit membership in pool groups.  There are also three maps which define
 * the resilience constraints for a given storage unit, the tags for a pool, and the live mode and
 * cost information for a pool.</p>
 *
 * <p>This class also provides methods for determining the resilient group
 * of a given pool, the storage groups connected to a given pool group, whether a pool or pool group
 * is resilient, and whether a pool group can satisfy the constraints defined by the storage units
 * bound to it.</p>
 *
 * <p>{@link #apply(PoolInfoDiff)}
 * empties and rebuilds pool-related information based on a diff obtained from {@link
 * #compare(PoolMonitor)}.  An empty diff is equivalent to a load operation (at initialization).
 * These methods are called by the {@link MapInitializer} at startup, and thereafter by the {@link
 * PoolInfoChangeHandler}</p>
 *
 * <p>This class benefits from read-write synchronization, since there will
 * be many more reads of what is for the most part stable information (note that the periodically
 * refreshed information is synchronized within the PoolInformation object itself; hence changes to
 * those values do not require a write lock; e.g., #updatePoolStatus.)</p>
 *
 * <p>Class is not marked final for stubbing/mocking purposes.</p>
 */
public class PoolInfoMap {

    private static final Logger LOGGER = LoggerFactory.getLogger(PoolInfoMap.class);

    /**
     * The NonReindexableList semantics is different on get() and indexOf() in that the former will
     * throw a NoSuchElementException if the list is set not to reference any nulls it may have as
     * placeholders for invalidated indices, and to throw a NoSuchElementException when the element
     * is not in the list.
     * <p>
     * Referencing this map under lock does not, unfortunately, guarantee consistency in this
     * regard, as the FileOperationMap could carry stale references (e.g., after operation cancel).
     * Not catching the NoSuchElementException then becomes problematic.
     * <p>
     * In the interest of safety, all references to the three NonReindexableLists use the static
     * safe methods of the list so that these failures will not provoke uncaught exceptions.
     */
    private final NonReindexableList<String> pools = new NonReindexableList<>();
    private final NonReindexableList<String> groups = new NonReindexableList<>();
    private final NonReindexableList<String> sunits = new NonReindexableList<>();
    private final Map<Integer, ResilienceMarker> markers = new HashMap<>();
    private final Map<Integer, ResilienceMarker> constraints = new HashMap<>();
    private final Map<Integer, PoolInformation> poolInfo = new HashMap<>();
    private final Multimap<Integer, Integer> poolGroupToPool = HashMultimap.create();
    private final Multimap<Integer, Integer> poolToPoolGroup = HashMultimap.create();
    private final Multimap<Integer, Integer> storageToPoolGroup = HashMultimap.create();
    private final Multimap<Integer, Integer> poolGroupToStorage = HashMultimap.create();

    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final Lock write = lock.writeLock();
    private final Lock read = lock.readLock();

    private boolean useRegex = false;

    /**
     * <p>Called on a dedicated thread.</p>
     *
     * <p>Applies a diff under write lock.</p>
     *
     * <p>Will not clear the NonReindexable lists (pools, groups).
     * This is to maintain the same indices for the duration of the life of the JVM, since the other
     * maps may contain live references to pools or groups.</p>
     */
    public void apply(PoolInfoDiff diff) {
        write.lock();
        try {
            /*
             *  -- Remove stale pools, pool groups and storage units.
             *
             *     Pool removal assumes that the pool has been properly drained
             *     of files first; this is not taken care of here.  Similarly
             *     for the removal of pool groups.
             */
            LOGGER.trace("removing stale pools, pool groups and storage units");
            diff.getOldPools().stream().forEach(this::removePool);
            diff.getOldGroups().stream().forEach(this::removeGroup);
            diff.getOldUnits().stream().forEach(this::removeUnit);

            /*
             *  -- Remove pools from current groups.
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
            diff.getNewUnits().stream().forEach(this::addStorageUnit);
            diff.getNewGroups().stream().forEach(this::addPoolGroup);
            diff.getNewPools().stream().forEach(pools::add);

            /*
             *  -- Add units to pool groups.
             */
            LOGGER.trace("adding storage units to pool groups");
            diff.getUnitsAddedToPoolGroup().entries().stream()
                  .forEach(this::addUnitToPoolGroup);

            /*
             *  -- Add pools to pool groups.
             */
            LOGGER.trace("adding pools to pool groups");
            diff.getPoolsAddedToPoolGroup().entries().stream()
                  .forEach(this::addPoolToPoolGroups);

            /*
             *  -- Modify constraints.
             */
            LOGGER.trace("modifying storage unit constraints");
            diff.getConstraints().entrySet().stream()
                  .forEach(this::updateConstraints);

            /*
             *  -- Add pool information for the new pools.
             *
             *     The new pools are the only ones to have
             *     entries in the cost info map.
             */
            LOGGER.trace("adding live pool information");
            diff.getPoolCost().keySet().stream()
                  .forEach((p) ->
                        setPoolInfo(p, diff.getModeChanged().get(p),
                              diff.getTagsChanged().get(p),
                              diff.poolCost.get(p)));
        } finally {
            write.unlock();
        }
    }

    /**
     * <p>Called on dedicated thread.</p>
     *
     * <p>Does a diff under read lock.</p>
     *
     * <p>Gathers new pools, removed pools, new pool groups, removed pool
     * groups, new storage units, removed storage units, modified groups and storage units, and pool
     * information changes.</p>
     *
     * @param poolMonitor received from pool manager message.
     * @return the diff (parsed out into relevant maps and collections).
     */
    public PoolInfoDiff compare(PoolMonitor poolMonitor) {
        read.lock();
        PoolSelectionUnit psu = poolMonitor.getPoolSelectionUnit();
        useRegex = psu.isEnabledRegex();
        PoolInfoDiff diff = new PoolInfoDiff();
        try {
            LOGGER.trace("Searching for currently uninitialized pools.");
            getUninitializedPools(diff);

            LOGGER.trace("comparing pools");
            Set<String> commonPools = comparePools(diff, psu);

            LOGGER.trace("comparing pool groups");
            Set<String> common = comparePoolGroups(diff, psu);

            LOGGER.trace("adding pools and units to new pool groups");
            addPoolsAndUnitsToNewPoolGroups(diff, psu);

            LOGGER.trace("comparing pools in pool groups");
            comparePoolsInPoolGroups(diff, common, psu);

            LOGGER.trace("comparing storage units");
            common = compareStorageUnits(diff, psu);

            LOGGER.trace("adding pool groups for new storage units");
            addPoolGroupsForNewUnits(diff, psu);

            LOGGER.trace("comparing storage unit links and constraints");
            compareStorageUnitLinksAndConstraints(diff, common, psu);

            LOGGER.trace("comparing pool info");
            comparePoolInfo(diff, commonPools, poolMonitor);
        } finally {
            read.unlock();
        }

        LOGGER.trace("Diff:\n{}", diff.toString());
        return diff;
    }

    public int getCountableLocations(Collection<String> locations) {
        read.lock();
        int countable = 0;
        try {
            for (String location : locations) {
                PoolInformation info = poolInfo.get(safeIndexOf(location, pools));
                if (info != null && info.isInitialized() && info.isCountable()) {
                    ++countable;
                }
            }
        } finally {
            read.unlock();
        }
        return countable;
    }

    public Set<String> getExcludedLocationNames(Collection<String> members) {
        read.lock();
        try {
            return members.stream()
                  .map(l -> poolInfo.get(safeIndexOf(l, pools)))
                  .filter(Objects::nonNull)
                  .filter(PoolInformation::isInitialized)
                  .filter(PoolInformation::isExcluded)
                  .map(PoolInformation::getName)
                  .collect(Collectors.toSet());
        } finally {
            read.unlock();
        }
    }

    public String getGroup(Integer group) {
        read.lock();
        try {
            return groups.get(group);
        } finally {
            read.unlock();
        }
    }

    public Integer getGroupIndex(String name) {
        read.lock();
        try {
            return groups.indexOf(name);
        } finally {
            read.unlock();
        }
    }

    public Set<String> getMemberLocations(Integer gindex, Collection<String> locations) {
        read.lock();
        try {
            Set<String> ofGroup = poolGroupToPool.get(gindex)
                  .stream()
                  .map(i -> safeGet(i, pools))
                  .collect(Collectors.toSet());
            return locations.stream().filter(ofGroup::contains).collect(Collectors.toSet());
        } finally {
            read.unlock();
        }
    }

    /**
     * @param writable location is writable if true, readable if false.
     * @return all pool group pools which qualify.
     */
    public Set<String> getMemberPools(Integer gindex, boolean writable) {
        read.lock();
        try {
            return ImmutableSet.copyOf(poolGroupToPool.get(gindex))
                  .stream()
                  .filter(p -> viable(p, writable))
                  .map(p -> safeGet(p, pools))
                  .collect(Collectors.toSet());
        } finally {
            read.unlock();
        }
    }

    public String getPool(Integer pool) {
        read.lock();
        try {
            return safeGet(pool, pools);
        } finally {
            read.unlock();
        }
    }

    public Stream<String> getResilientPoolGroupsFor(String storageUnit) {
        read.lock();
        try {
            Integer uindex = safeIndexOf(storageUnit, sunits);
            if (uindex == null) {
                return Stream.empty();
            }

            return storageToPoolGroup.get(uindex)
                  .stream()
                  .filter(g -> markers.get(g).isResilient())
                  .map(g -> safeGet(g, groups))
                  .unordered();
        } finally {
            read.unlock();
        }
    }

    public Integer getPoolIndex(String name) {
        read.lock();
        try {
            return safeIndexOf(name, pools);
        } finally {
            read.unlock();
        }
    }

    public Set<Integer> getPoolIndices(Collection<String> locations) {
        read.lock();
        try {
            return locations.stream()
                  .map(p -> safeIndexOf(p, pools))
                  .collect(Collectors.toSet());
        } finally {
            read.unlock();
        }
    }

    public PoolInformation getPoolInformation(Integer index) {
        read.lock();
        try {
            return poolInfo.get(index);
        } finally {
            read.unlock();
        }
    }

    public PoolManagerPoolInformation getPoolManagerInfo(Integer pool) {
        read.lock();
        try {
            return new PoolManagerPoolInformation(safeGet(pool, pools),
                  poolInfo.get(pool).getCostInfo());
        } finally {
            read.unlock();
        }
    }

    public PoolStateUpdate getPoolState(String pool) {
        return getPoolState(pool, null, null, null);
    }

    public PoolStateUpdate getPoolState(String pool, String storageUnit) {
        return getPoolState(pool, null, null, storageUnit);
    }

    public PoolStateUpdate getPoolState(String pool, Integer addedTo,
          Integer removedFrom) {
        return getPoolState(pool, addedTo, removedFrom, null);
    }

    public PoolStateUpdate getPoolState(String pool, Integer addedTo,
          Integer removedFrom,
          String storageUnit) {
        PoolInformation info = getPoolInformation(getPoolIndex(pool));
        if (info != null) {
            return new PoolStateUpdate(pool, info.getMode(), addedTo,
                  removedFrom, storageUnit);
        }

        /*
         * No information.  Treat it as UNINITIALIZED.
         */
        return new PoolStateUpdate(pool, null, addedTo, removedFrom, storageUnit);
    }

    public Set<String> getPools(Collection<Integer> indices) {
        read.lock();
        try {
            return indices.stream().map(i -> safeGet(i, pools)).collect(Collectors.toSet());
        } finally {
            read.unlock();
        }
    }

    public Collection<Integer> getPoolsOfGroup(Integer group) {
        read.lock();
        try {
            return poolGroupToPool.get(group);
        } finally {
            read.unlock();
        }
    }

    public Set<String> getReadableLocations(Collection<String> locations) {
        read.lock();
        try {
            return locations.stream()
                  .filter((l) -> viable(safeIndexOf(l, pools), false))
                  .collect(Collectors.toSet());
        } finally {
            read.unlock();
        }
    }

    public Integer getResilientPoolGroup(Integer pool) {
        read.lock();
        try {
            Set<Integer> rgroups
                  = poolToPoolGroup.get(pool)
                  .stream()
                  .filter(g -> markers.get(g).isResilient())
                  .collect(Collectors.toSet());

            if (rgroups.size() == 0) {
                return null;
            }

            if (rgroups.size() > 1) {
                throw new IllegalStateException(String.format(
                      "Pool map is inconsistent; pool %s belongs to "
                            + "more than one resilient "
                            + "group: %s.",
                      safeGet(pool, pools),
                      rgroups.stream()
                            .map(g -> safeGet(g, groups))
                            .collect(Collectors.toSet())));
            }

            return rgroups.iterator().next();
        } finally {
            read.unlock();
        }
    }

    public Set<String> getResilientPools() {
        read.lock();
        try {
            return pools.stream()
                  .filter(this::resilient)
                  .collect(Collectors.toSet());
        } finally {
            read.unlock();
        }
    }

    public StorageUnitConstraints getStorageUnitConstraints(Integer unit) {
        ResilienceMarker marker = constraints.get(unit);

        if (marker != null && !(marker instanceof StorageUnitConstraints)) {
            String message = "Index " + unit + " does not correspond "
                  + "to a storage unit";
            throw new NoSuchElementException(message);
        }

        return (StorageUnitConstraints) marker;
    }

    public Integer getStorageUnitIndex(FileAttributes attributes) {
        String classKey = attributes.getStorageClass();
        String unitKey = classKey;
        String hsm = attributes.getHsm();
        if (hsm != null) {
            unitKey += ("@" + hsm);
        }

        read.lock();
        try {
            return sunits.indexOf(unitKey);
        } catch (NoSuchElementException e) {
            return resolveStorageUnitIndex(classKey, unitKey);
        } finally {
            read.unlock();
        }
    }

    public Collection<Integer> getStorageUnitsFor(String poolGroup) {
        Integer gindex = getGroupIndex(poolGroup);
        if (gindex == null) {
            return ImmutableList.of();
        }

        read.lock();
        try {
            return poolGroupToStorage.get(gindex);
        } finally {
            read.unlock();
        }
    }

    public Map<String, String> getTags(Integer pool) {
        PoolInformation info = getPoolInformation(pool);
        if (info == null) {
            return ImmutableMap.of();
        }

        Map<String, String> tags = info.getTags();
        if (tags == null) {
            return ImmutableMap.of();
        }

        return tags;
    }

    public String getUnit(Integer index) {
        read.lock();
        try {
            return safeGet(index, sunits);
        } finally {
            read.unlock();
        }
    }

    public Integer getUnitIndex(String name) {
        read.lock();
        try {
            return safeIndexOf(name, sunits);
        } finally {
            read.unlock();
        }
    }

    public Set<Integer> getValidLocations(Collection<Integer> locations,
          boolean writable) {
        read.lock();
        try {
            return locations.stream()
                  .filter((i) -> viable(i, writable))
                  .collect(Collectors.toSet());
        } finally {
            read.unlock();
        }
    }

    public boolean hasPool(String pool) {
        read.lock();
        try {
            return pools.contains(pool);
        } finally {
            read.unlock();
        }
    }

    public boolean isValidPoolIndex(Integer index) {
        read.lock();
        try {
            return pools.get(index) != null;
        } catch (NoSuchElementException e) {
            return false;
        } finally {
            read.unlock();
        }
    }

    public boolean isPoolViable(Integer pool, boolean writable) {
        read.lock();
        try {
            return viable(pool, writable);
        } finally {
            read.unlock();
        }
    }

    public boolean isResilientGroup(Integer gindex) {
        read.lock();
        try {
            return markers.get(gindex).isResilient();
        } finally {
            read.unlock();
        }
    }

    public boolean isResilientPool(String pool) {
        read.lock();
        try {
            return resilient(pool);
        } finally {
            read.unlock();
        }
    }

    public boolean isInitialized(String pool) {
        read.lock();
        try {
            PoolInformation info = getPoolInformation(pools.indexOf(pool));
            return info != null && info.isInitialized();
        } catch (NoSuchElementException e) {
            return false;
        } finally {
            read.unlock();
        }
    }

    public String listPoolInfo(PoolInfoFilter poolInfoFilter) {
        final StringBuilder builder = new StringBuilder();
        read.lock();
        try {
            pools.stream()
                  .map(i -> safeIndexOf(i, pools))
                  .map(poolInfo::get)
                  .filter(poolInfoFilter::matches)
                  .forEach((i) -> builder.append(i).append("\n"));
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
        constraints.put(sunits.indexOf(group),
              new StorageUnitConstraints(required, oneCopyPer));
    }

    public void updatePoolStatus(PoolStateUpdate update) {
        read.lock();
        try {
            poolInfo.get(safeIndexOf(update.pool, pools)).updateState(update);
        } finally {
            read.unlock();
        }
    }

    /**
     * <p>A coarse-grained verification that the required and tag constraints
     * of the pool group and its associated storage groups can be met. For the default and each
     * storage unit, it attempts to fulfill the max independent location requirement via the {@link
     * CopyLocationExtractor}.</p>
     *
     * @throws IllegalStateException upon encountering the first set of constraints which cannot be
     *                               met.
     */
    public void verifyConstraints(Integer pgindex)
          throws IllegalStateException {
        Collection<Integer> storageGroups;
        CopyLocationExtractor extractor;

        read.lock();
        try {
            storageGroups = poolGroupToStorage.get(pgindex);
            for (Integer index : storageGroups) {
                StorageUnitConstraints unitConstraints
                      = (StorageUnitConstraints) constraints.get(index);
                int required = unitConstraints.getRequired();
                extractor = new CopyLocationExtractor(unitConstraints.getOneCopyPer(), this);
                verify(pgindex, extractor, required);
            }
        } finally {
            read.unlock();
        }
    }

    @VisibleForTesting
    /** Called under write lock **/
    void removeGroup(String group) {
        int index = safeIndexOf(group, groups);
        groups.remove(index);
        markers.remove(index);
        poolGroupToPool.removeAll(index)
              .stream()
              .forEach((pindex) -> poolToPoolGroup.remove(pindex,
                    index));
        poolGroupToStorage.removeAll(index)
              .stream()
              .forEach((gindex) -> storageToPoolGroup.remove(
                    gindex, index));
    }

    @VisibleForTesting
    /** Called under write lock except during unit test**/
    void removePool(String pool) {
        int pindex = safeIndexOf(pool, pools);
        pools.remove(pindex);
        poolToPoolGroup.removeAll(pindex).stream()
              .forEach((g) -> poolGroupToPool.remove(g, pindex));
        poolInfo.remove(pindex);
    }

    @VisibleForTesting
    /** Called under write lock except during unit test **/
    void removeUnit(String unit) {
        int index = safeIndexOf(unit, sunits);
        sunits.remove(index);
        constraints.remove(index);
        storageToPoolGroup.removeAll(index).stream()
              .forEach((gindex) -> poolGroupToStorage.remove(gindex, index));
    }

    /**
     * Called under write lock
     **/
    private void addPoolGroup(SelectionPoolGroup group) {
        String name = group.getName();
        groups.add(name);
        markers.put(groups.indexOf(name), new ResilienceMarker(group.isResilient()));
    }

    /**
     * Called under write lock
     **/
    private void addPoolGroupsForNewUnits(PoolInfoDiff diff,
          PoolSelectionUnit psu) {
        Collection<StorageUnit> newUnits = diff.getNewUnits();
        for (StorageUnit unit : newUnits) {
            String name = unit.getName();
            StorageUnitInfoExtractor.getResilientGroupsFor(name, psu)
                  .stream()
                  .forEach((g) -> diff.unitsAdded.put(g, name));
        }
    }

    /**
     * Called under write lock
     **/
    private void addPoolToPoolGroups(Entry<String, String> entry) {
        Integer pindex = safeIndexOf(entry.getKey(), pools);
        Integer gindex = safeIndexOf(entry.getValue(), groups);
        poolGroupToPool.put(gindex, pindex);
        poolToPoolGroup.put(pindex, gindex);
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
                  .forEach((p) -> diff.poolsAdded.put(p, name));
            StorageUnitInfoExtractor.getStorageUnitsInGroup(name, psu)
                  .stream()
                  .forEach((u) -> diff.unitsAdded.put(name, u.getName()));
        }
    }

    /**
     * Called under write lock
     **/
    private void addStorageUnit(StorageUnit unit) {
        String name = unit.getName();
        sunits.add(name);
        constraints.put(sunits.indexOf(name),
              new StorageUnitConstraints(unit.getRequiredCopies(),
                    unit.getOnlyOneCopyPer()));
    }

    /**
     * Called under write lock
     **/
    private void addUnitToPoolGroup(Entry<String, String> entry) {
        Integer gindex = safeIndexOf(entry.getKey(), groups);
        Integer sindex = safeIndexOf(entry.getValue(), sunits);
        storageToPoolGroup.put(sindex, gindex);
        poolGroupToStorage.put(gindex, sindex);
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
        Set<String> curr = poolGroupToPool.keySet()
              .stream()
              .map(this::getGroup)
              .collect(Collectors.toSet());
        Sets.difference(next, curr).stream()
              .map((name) -> psu.getPoolGroups().get(name))
              .forEach(diff.newGroups::add);
        Sets.difference(curr, next).stream()
              .forEach(diff.oldGroups::add);
        return Sets.intersection(next, curr);
    }

    /**
     * Called under read lock
     **/
    private void comparePoolInfo(PoolInfoDiff diff, Set<String> commonPools,
          PoolMonitor poolMonitor) {
        PoolSelectionUnit psu = poolMonitor.getPoolSelectionUnit();
        CostModule costModule = poolMonitor.getCostModule();

        /*
         *  First add the info for all new pools to the diff.
         */
        diff.getNewPools().stream()
              .forEach((p) -> {
                  diff.getModeChanged().put(p, getPoolMode(psu.getPool(p)));
                  diff.getTagsChanged().put(p, getPoolTags(p, costModule));
                  diff.getPoolCost().put(p, getPoolCostInfo(p, costModule));
              });

        /*
         * Now check for differences with current pools that are still valid.
         */
        commonPools.stream()
              .forEach((p) -> {
                  PoolInformation info = poolInfo.get(getPoolIndex(p));
                  PoolV2Mode newMode = getPoolMode(psu.getPool(p));
                  PoolV2Mode oldMode = info.getMode();
                  if (oldMode == null || (newMode != null
                        && !oldMode.equals(newMode))) {
                      diff.getModeChanged().put(p, newMode);
                  }

                  ImmutableMap<String, String> newTags
                        = getPoolTags(p, costModule);
                  ImmutableMap<String, String> oldTags = info.getTags();
                  if (oldTags == null || (newTags != null
                        && !oldTags.equals(newTags))) {
                      diff.getTagsChanged().put(p, newTags);
                  }

                  /*
                   * Since we are not altering the actual collections inside
                   * the PoolInfoMap, but are simply modifying the PoolInformation
                   * object, and since its own update method is synchronized,
                   * we can take care of the update here while holding a read lock.
                   */
                  info.update(newMode, newTags, getPoolCostInfo(p, costModule));
              });

    }

    /**
     * Called under read lock
     **/
    private Set<String> comparePools(PoolInfoDiff diff,
          PoolSelectionUnit psu) {
        Set<String> next = psu.getPools().values()
              .stream()
              .map(SelectionPool::getName)
              .collect(Collectors.toSet());
        Set<String> curr = ImmutableSet.copyOf(pools);
        Sets.difference(next, curr).stream().forEach(diff.newPools::add);
        Sets.difference(curr, next).stream().forEach(diff.oldPools::add);
        return Sets.intersection(curr, next);
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
            Set<String> curr = poolGroupToPool.get(safeIndexOf(group, groups))
                  .stream()
                  .map(i -> safeGet(i, pools))
                  .collect(Collectors.toSet());
            Sets.difference(next, curr)
                  .stream()
                  .forEach((p) -> diff.poolsAdded.put(p, group));
            Sets.difference(curr, next)
                  .stream()
                  .filter((p) -> !diff.oldPools.contains(p))
                  .forEach((p) -> diff.poolsRmved.put(p, group));
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
            int index = safeIndexOf(unit, sunits);
            Set<String> next
                  = ImmutableSet.copyOf(StorageUnitInfoExtractor.getPoolGroupsFor(unit, psu,
                  false));
            Set<String> curr = storageToPoolGroup.get(index)
                  .stream()
                  .map(i -> safeGet(i, groups))
                  .collect(Collectors.toSet());
            Sets.difference(next, curr)
                  .stream()
                  .forEach((group) -> diff.unitsAdded.put(group, unit));
            Sets.difference(curr, next)
                  .stream()
                  .filter((group) -> !diff.oldGroups.contains(group))
                  .forEach((group) -> diff.unitsRmved.put(group, unit));

            Integer required = storageUnit.getRequiredCopies();
            int newRequired = required == null ? -1 : required;

            StorageUnitConstraints constraints
                  = (StorageUnitConstraints) this.constraints.get(index);

            int oldRequired = !constraints.isResilient() ? -1 : constraints.getRequired();

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
        Set<String> curr = storageToPoolGroup.keySet()
              .stream()
              .map(i -> safeGet(i, sunits))
              .collect(Collectors.toSet());
        Sets.difference(next, curr)
              .stream()
              .map(psu::getStorageUnit)
              .forEach(diff.newUnits::add);
        Sets.difference(curr, next).stream().forEach(diff.oldUnits::add);
        return Sets.intersection(next, curr);
    }

    private PoolV2Mode getPoolMode(SelectionPool pool) {
        /*
         *  Allow a NULL value
         */
        return pool.getPoolMode();
    }

    private ImmutableMap<String, String> getPoolTags(String pool,
          CostModule costModule) {
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

    private void getUninitializedPools(PoolInfoDiff diff) {
        pools.stream()
              .filter((p) -> !isInitialized(p))
              .forEach(diff.uninitPools::add);
    }

    /**
     * Called under write lock
     **/
    private void removeFromPoolGroup(Entry<String, String> entry) {
        Integer pindex = safeIndexOf(entry.getKey(), pools);
        Integer gindex = safeIndexOf(entry.getValue(), groups);
        poolGroupToPool.remove(gindex, pindex);
        poolToPoolGroup.remove(pindex, gindex);
    }

    private void removeStorageUnit(Entry<String, String> entry) {
        Integer sindex = safeIndexOf(entry.getValue(), sunits);
        Integer pindex = safeIndexOf(entry.getKey(), groups);
        storageToPoolGroup.remove(sindex, pindex);
        poolGroupToStorage.remove(pindex, sindex);
    }

    private boolean resilient(String pool) {
        try {
            return getResilientPoolGroup(pools.indexOf(pool)) != null;
        } catch (NoSuchElementException e) {
            return false;
        }
    }

    /**
     * <p>This method is an alternate search for storage unit.
     * It first attempts to match units by interpreting the class names as a regex. If that fails,
     * it tries first the class key, then universal key.</p>
     *
     * @param classKey the storage class of the unit
     * @param unitKey  the full name of the storage unit
     */
    private Integer resolveStorageUnitIndex(String classKey, String unitKey)
          throws NoSuchElementException {
        Integer universalCoverage = null;
        Integer classCoverage = null;
        Integer specific = null;

        read.lock();
        try {
            for (String unit : sunits) {
                if (unit.equals("*@*")) {
                    universalCoverage = sunits.indexOf(unit);
                } else if (unit.equals("*@" + classKey)) {
                    classCoverage = sunits.indexOf(unit);
                } else if (useRegex && Pattern.matches(unit, unitKey)) {
                    specific = sunits.indexOf(unit);
                    break;
                }
            }
        } finally {
            read.unlock();
        }

        if (specific != null) {
            return specific;
        }

        if (classCoverage != null) {
            return classCoverage;
        }

        if (universalCoverage != null) {
            return universalCoverage;
        }

        /*
         *  Preserve behavior of the NonReindexable list
         */
        throw new NoSuchElementException(String.valueOf(unitKey));
    }

    /**
     * Called under write lock
     **/
    private void setPoolInfo(String pool,
          PoolV2Mode mode,
          ImmutableMap<String, String> tags,
          PoolCostInfo cost) {
        Integer pindex = safeIndexOf(pool, pools);
        PoolInformation entry = new PoolInformation(pool, pindex);
        entry.update(mode, tags, cost);
        poolInfo.put(pindex, entry);
    }

    /**
     * Called under write lock
     **/
    private void updateConstraints(Entry<String, StorageUnitConstraints> entry) {
        constraints.put(safeIndexOf(entry.getKey(), sunits), entry.getValue());
    }

    /**
     * Called under read lock
     *
     * @param index     of pool group.
     * @param extractor configured for the specific tag constraints.
     * @param required  specific to this group or storage unit.
     * @throws IllegalStateException upon encountering the first set of constraints which cannot be
     *                               met.
     */
    private void verify(Integer index,
          CopyLocationExtractor extractor,
          int required) throws IllegalStateException {
        Set<String> members = poolGroupToPool.get(index).stream()
              .map(i -> safeGet(i, pools))
              .collect(Collectors.toSet());
        for (int i = 0; i < required; i++) {
            Collection<String> candidates
                  = extractor.getCandidateLocations(members);
            if (candidates.isEmpty()) {
                throw new IllegalStateException(getGroup(index));
            }
            String selected = RandomSelectionStrategy.SELECTOR.apply(candidates);
            members.remove(selected);
            extractor.addSeenTagsFor(selected);
        }
    }

    private boolean viable(Integer pool, boolean writable) {
        PoolInformation info = poolInfo.get(pool);
        return info != null && info.isInitialized()
              && (writable ? info.canRead() && info.canWrite()
              : info.canRead());
    }
}