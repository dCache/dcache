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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.poolManager.StorageUnit;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolV2Mode;
import org.dcache.poolmanager.PoolMonitor;

/**
 * <p>Container for recording changes to the internal maps based on
 *      a PoolMonitor update.  Records pools, pool groups and storage units
 *      that have been added or removed, and modifications to the pool set of
 *      a pool group, to storage unit constraints, and to the tags, mode and
 *      cost for new or current pools.</p>
 *
 * <p>A note on the handling of pool cost:  cost is always updated, but
 *      this is done during the compare phase (under read lock) for pools that
 *      are already present.  Hence the cost map does not figure into the
 *      'empty' diff condition.</p>
 *
 * @see PoolInfoMap#comparePoolInfo(PoolInfoDiff, Set, PoolMonitor)
 */
public final class PoolInfoDiff {
    final Collection<String> newPools = new ArrayList<>();
    final Collection<String> oldPools = new ArrayList<>();
    final Collection<String> uninitPools = new ArrayList<>();
    final Collection<SelectionPoolGroup> newGroups = new ArrayList<>();
    final Collection<String> oldGroups = new ArrayList<>();
    final Collection<StorageUnit> newUnits = new ArrayList<>();
    final Collection<String> oldUnits = new ArrayList<>();

    /*
     *  (pool, poolGroup)
     */
    final Multimap<String, String> poolsAdded = HashMultimap.create();

    /*
     *  (pool, poolGroup)
     */
    final Multimap<String, String> poolsRmved = HashMultimap.create();

    /*
     *  (pgroup, unit)
     */
    final Multimap<String, String> unitsAdded = HashMultimap.create();

    /*
     *  (pgroup, unit)
     */
    final Multimap<String, String> unitsRmved = HashMultimap.create();

    /*
     *  (unit, constraints)
     */
    final Map<String, StorageUnitConstraints> constraints = new HashMap<>();

    /*
     *  (pool, mode)
     */
    private final Map<String, PoolV2Mode> modeChanged = new HashMap<>();

    /*
     *  (pool, tags)
     */
    private final Map<String, ImmutableMap<String, String>> tagsChanged
                    = new HashMap<>();

    /*
     *  (pool, cost)
     */
    final Map<String, PoolCostInfo> poolCost = new HashMap<>();

    public Map<String, StorageUnitConstraints> getConstraints() {
        return constraints;
    }

    public Map<String, PoolV2Mode> getModeChanged() {
        return modeChanged;
    }

    public Collection<SelectionPoolGroup> getNewGroups() {
        return newGroups;
    }

    public Collection<String> getNewPools() {
        return newPools;
    }

    public Collection<StorageUnit> getNewUnits() {
        return newUnits;
    }

    public Collection<String> getOldGroups() {
        return oldGroups;
    }

    public Collection<String> getOldPools() {
        return oldPools;
    }

    public Collection<String> getOldUnits() {
        return oldUnits;
    }

    public Map<String, PoolCostInfo> getPoolCost() {
        return poolCost;
    }

    public Multimap<String, String> getPoolsAddedToPoolGroup() {
        return poolsAdded;
    }

    public Multimap<String, String> getPoolsRemovedFromPoolGroup() {
        return poolsRmved;
    }

    public Map<String, ImmutableMap<String, String>> getTagsChanged() {
        return tagsChanged;
    }

    public Collection<String> getUninitializedPools() {
        return uninitPools;
    }

    public Multimap<String, String> getUnitsAddedToPoolGroup() {
        return unitsAdded;
    }

    public Multimap<String, String> getUnitsRemovedFromPoolGroup() {
        return unitsRmved;
    }

    public boolean isEmpty() {
        return newPools.isEmpty() && oldPools.isEmpty() && uninitPools.isEmpty()
                        && newGroups.isEmpty() && oldGroups.isEmpty()
                        && newUnits.isEmpty() && oldUnits.isEmpty()
                        && poolsAdded.isEmpty() && poolsRmved.isEmpty()
                        && unitsAdded.isEmpty() && unitsRmved.isEmpty()
                        && constraints.isEmpty() && tagsChanged.isEmpty()
                        && modeChanged.isEmpty();
    }

    public String toString() {
        return String.format("New Pools:            %s\n" +
                                        "Old Pools:            %s\n" +
                                        "Uninitialized Pools:  %s\n" +
                                        "New Groups:           %s\n" +
                                        "Old Groups:           %s\n" +
                                        "New Units:            %s\n" +
                                        "Old Units:            %s\n" +
                                        "Pools Added:          %s\n" +
                                        "Pools Removed:        %s\n" +
                                        "Units Added:          %s\n" +
                                        "Units Removed:        %s\n" +
                                        "Constraints changed:  %s\n" +
                                        "Mode changed:         %s\n" +
                                        "Tags changed:         %s\n",
                        newPools, oldPools, uninitPools,
                        newGroups, oldGroups,
                        newUnits, oldUnits,
                        poolsAdded, poolsRmved,
                        unitsAdded, unitsRmved,
                        constraints, modeChanged,
                        tagsChanged);
    }
}
