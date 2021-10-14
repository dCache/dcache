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
package diskCacheV111.poolManager;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionLink;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnitGroup;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * <p>Utilities for finding storage unit info relevant to pool group
 * operations.</p>
 */
public final class StorageUnitInfoExtractor {

    /**
     * For backward compatibility.
     */
    public static Collection<String> getResilientGroupsFor(String unitName,
          PoolSelectionUnit psu) {
        return getPrimaryGroupsFor(unitName, psu);
    }

    /**
     * @param primaryOnly return only primary groups.
     * @return all the pool groups to which this storage unit is linked.
     */
    public static Collection<String> getPoolGroupsFor(String unitName,
          PoolSelectionUnit psu,
          boolean primaryOnly) {
        return psu.getPoolGroups().values().stream()
              .filter((g) -> (primaryOnly ? g.isPrimary() : true) &&
                    hasStorageUnit(g.getName(), unitName, psu))
              .map(SelectionPoolGroup::getName)
              .collect(Collectors.toList());
    }

    /**
     * @return all the primary pool groups to which this storage unit is linked.
     */
    public static Collection<String> getPrimaryGroupsFor(String unitName, PoolSelectionUnit psu) {
        return getPoolGroupsFor(unitName, psu, true);
    }

    /**
     * @return all the storage units linked to this pool group.
     */
    public static Collection<StorageUnit> getStorageUnitsInGroup(String name,
          PoolSelectionUnit psu) {
        Collection<StorageUnit> units = new ArrayList<>();
        psu.getLinksPointingToPoolGroup(name).stream()
              .map(SelectionLink::getUnitGroupsTargetedBy)
              .flatMap(Collection::stream)
              .map(SelectionUnitGroup::getMemberUnits)
              .flatMap(Collection::stream)
              .filter(StorageUnit.class::isInstance)
              .map(StorageUnit.class::cast)
              .forEach(units::add);
        return units;
    }

    /**
     * For backward compatibility.
     */
    public static boolean hasResilientStorageUnit(String poolGroup, PoolSelectionUnit psu) {
        return hasPrimaryStorageUnit(poolGroup, psu);
    }

    /**
     * @return true if the pool group has at least one storage group associated with it via a link,
     * and the storage group is primary.
     */
    public static boolean hasPrimaryStorageUnit(String poolGroup, PoolSelectionUnit psu) {
        return getStorageUnitsInGroup(poolGroup, psu)
              .stream()
              .filter((u) -> u.getRequiredCopies() != null)
              .findAny().isPresent();
    }

    private static boolean hasStorageUnit(String poolGroup,
          String storageUnit,
          PoolSelectionUnit psu) {
        return getStorageUnitsInGroup(poolGroup, psu).stream()
              .filter((sunit) -> sunit.getName().equals(storageUnit))
              .findAny().isPresent();
    }

    private StorageUnitInfoExtractor() {
    }
}
