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

import static org.dcache.qos.data.QoSAction.CACHE_REPLICA;
import static org.dcache.qos.data.QoSAction.COPY_REPLICA;
import static org.dcache.qos.data.QoSAction.FLUSH;
import static org.dcache.qos.data.QoSAction.NOTIFY_INACCESSIBLE;
import static org.dcache.qos.data.QoSAction.NOTIFY_MISSING;
import static org.dcache.qos.data.QoSAction.NOTIFY_OUT_OF_SYNC;
import static org.dcache.qos.data.QoSAction.PERSIST_REPLICA;
import static org.dcache.qos.data.QoSAction.POOL_SELECTION_FAILURE;
import static org.dcache.qos.data.QoSAction.UNSET_PRECIOUS_REPLICA;
import static org.dcache.qos.data.QoSAction.VOID;
import static org.dcache.qos.data.QoSAction.WAIT_FOR_STAGE;
import static org.dcache.qos.services.verifier.data.PoolInfoMap.SYSTEM_PGROUP;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;
import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.dcache.cells.CellStub;
import org.dcache.qos.data.FileQoSRequirements;
import org.dcache.qos.data.QoSAction;
import org.dcache.qos.services.verifier.data.PoolInfoMap;
import org.dcache.qos.services.verifier.data.VerifiedLocations;
import org.dcache.qos.services.verifier.data.VerifyOperation;
import org.dcache.qos.services.verifier.util.EvictingLocationExtractor;
import org.dcache.qos.services.verifier.util.LocationSelectionException;
import org.dcache.qos.services.verifier.util.LocationSelector;
import org.dcache.qos.util.RepositoryReplicaVerifier;
import org.dcache.util.CacheExceptionFactory;
import org.dcache.vehicles.qos.ReplicaStatusMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the parent class for the logic engine which verifies replica requirements.
 * <p/>
 * The principal method checks whether a standing operation requires (further) action (#verify).
 * <p/>
 * Specific implementations are required to provide a way for selecting new locations, and a way for
 * determining which locations can be cached.
 */
public abstract class FileStatusVerifier {

    public static final String VERIFY_FAILURE_MESSAGE = "Processing for %s failed during verify. %s%s";

    private static final Logger LOGGER = LoggerFactory.getLogger(FileStatusVerifier.class);

    protected CellStub pools;
    protected PoolInfoMap poolInfoMap;

    public void setPools(CellStub pools) {
        this.pools = pools;
    }

    public void setPoolInfoMap(PoolInfoMap poolInfoMap) {
        this.poolInfoMap = poolInfoMap;
    }

    public QoSAction verify(FileQoSRequirements requirements, VerifyOperation operation)
          throws InterruptedException {
        /*
         *  If the parent source is DRAINING, we increase the required number of disk copies
         *  by one if the disk requirement is non-zero.
         *
         *  NOTE:  we do not need to force the source to be the parent, because the parent
         *         pool is still readable.  In the case where a file has a single copy
         *         resident on the parent, the parent will be automatically chosen.
         */
        String parent = operation.getParent();
        if (parent!= null && poolInfoMap.isPoolDraining(parent)) {
            int disk = requirements.getRequiredDisk();
            if (disk > 0) { /* do not create a persistent copy in the case of cached-only replicas */
                requirements.setRequiredDisk(disk + 1);
            }
        }

        VerifiedLocations locations = classifyLocations(requirements, operation.getPoolGroup());
        Optional<QoSAction> optional;

        if (requirements.getRequiredDisk() > 0) {
            optional = checkForEmptyDiskLocations(requirements, locations, operation);
            if (optional.isPresent()) {
                operation.setNeeded(1);
                return optional.get();
            }

            optional = checkPoolToNamespaceSync(locations);
            if (optional.isPresent()) {
                return optional.get();
            }

            optional = checkForInaccessibleLocations(requirements, locations, operation);
            if (optional.isPresent()) {
                operation.setNeeded(1);
                return optional.get();
            }
        }

        boolean verifyHsm = false;

        switch (operation.getMessageType()) {
            case ADD_CACHE_LOCATION:
            case CORRUPT_FILE:
                /*
                 *  Do not try to interfere with the flushing of a file that is new
                 *  or a file reported as broken.  Not taking action on the first
                 *  is imperative as the tape URI may not yet be set and determining
                 *  available HSM pools is based on the tape instance part (the authority)
                 *  of the URI.
                 */
                break;
            default:
                verifyHsm = true;
                optional = checkForFlush(requirements, locations, operation);
                if (optional.isPresent()) {
                    return optional.get();
                }
        }

        Collection<String> current = locations.getCurrentDiskLocations();
        if (current.isEmpty()) {
            if (requirements.getRequiredTape() == 0) {
                /*
                 *  This is a NEARLINE REPLICA file that is lost.
                 */
                return NOTIFY_MISSING;
            }

            /*
             *  This is a NEARLINE CUSTODIAL file with no cached copies presently on disk.
             */
            return VOID;
        }

        optional = checkForLocationsToCache(requirements, locations, operation, verifyHsm);
        if (optional.isPresent()) {
            return optional.get();
        }

        optional = checkForLocationsToEvict(requirements, locations, operation);
        if (optional.isPresent()) {
            return optional.get();
        }

        optional = checkForLocationAdjustment(requirements, locations, operation);
        if (optional.isPresent()) {
            return optional.get();
        }

        operation.setNeeded(0);
        return QoSAction.VOID;
    }

    protected abstract LocationSelector getLocationSelector();

    protected abstract EvictingLocationExtractor getEvictingLocationExtractor(
          Set<String> partitionKeys);

    private Optional<QoSAction> checkForEmptyDiskLocations(FileQoSRequirements requirements,
          VerifiedLocations locations,
          VerifyOperation operation) {
        /*
         *  Somehow, all the locations for this file have been removed from the namespace.
         */
        if (locations.getCurrentDiskLocations().isEmpty()) {
            LOGGER.debug("checkForEmptyDiskLocations {}, needs {} disk locations, no namespace "
                        + "locations found, checking to see if file can be staged.",
                  requirements.getPnfsId(), requirements.getRequiredDisk());
            if (shouldTryToStage(requirements, locations)) {
                operation.setNeeded(1);
                return Optional.of(WAIT_FOR_STAGE);
            }
            return Optional.of(NOTIFY_MISSING);
        }

        return Optional.empty();
    }

    private Optional<QoSAction> checkPoolToNamespaceSync(VerifiedLocations locations) {
        /*
         *  This should happen very rarely, since qos itself does not set the files to removed,
         *  but rather caches them (removes the system sticky flag).
         */
        Set<String> readable = locations.getReadable();
        Set<String> exist = locations.getExist();
        if (readable.size() != exist.size()) {
            LOGGER.debug("checkPoolToNamespaceSync {}, namespace has {} locations, pools {}; "
                        + "out of sync.",
                  locations.getPnfsId(), readable.size(), exist.size());
            return Optional.of(NOTIFY_OUT_OF_SYNC);
        }
        return Optional.empty();
    }

    private Optional<QoSAction> checkForInaccessibleLocations(FileQoSRequirements requirements,
          VerifiedLocations locations,
          VerifyOperation operation) {
        /*
         *  While cached copies are excluded from the replica count, we allow them to be included
         *  as readable sources.
         */
        if (locations.getViable().size() == 0) {
            LOGGER.debug("handleVerification {}, no valid readable locations found, "
                  + "checking to see if file can be staged.", requirements.getPnfsId());
            if (shouldTryToStage(requirements, locations)) {
                operation.setNeeded(1);
                return Optional.of(WAIT_FOR_STAGE);
            }
            return Optional.of(NOTIFY_INACCESSIBLE);
        }

        return Optional.empty();
    }

    private Optional<QoSAction> checkForFlush(FileQoSRequirements requirements,
          VerifiedLocations locations,
          VerifyOperation operation) {
        switch (operation.getMessageType()) {
            case CORRUPT_FILE:
            case ADD_CACHE_LOCATION:
                /*
                 *  Do not try to interfere with the flushing of a file that is new
                 *  or a file reported as broken.  Not taking action on the first
                 *  is imperative as the tape URI may not yet be set and determining
                 *  available HSM pools is based on the tape instance part (the authority)
                 *  of the URI.
                 */
                return Optional.empty();
        }

        int required = requirements.getRequiredTape();
        int current = locations.getCurrentTapeLocations().size();
        int missingTapeLocations = required - current;
        LOGGER.debug("{}, checking for flush, missing tape locations {}.", requirements.getPnfsId(),
              missingTapeLocations);
        if (missingTapeLocations > 0) {
            PnfsId pnfsId = requirements.getPnfsId();
            Set<String> potentialSources = locations.getViable();
            if (potentialSources == null || potentialSources.isEmpty()) {
                if (current == 0) {
                    return Optional.of(NOTIFY_MISSING);
                }
                /*
                 *  Pools are down/offline that have cached copies.
                 *  Try to stage one of the tape copies.
                 */
                return Optional.of(WAIT_FOR_STAGE);
            }

            Set<String> potentialTargets = findHsmLocations(operation, requirements, locations);
            LOGGER.debug("{}, potential flush targets {}.", pnfsId, potentialTargets);

            /*
             *  This is an error condition, but we still might be able to do
             *  other work in response to this transition, so we emit a warning
             *  and continue.
             */
            if (potentialTargets.isEmpty()) {
                LOGGER.warn("{}, should be flushed: {}.", pnfsId,
                      CacheExceptionFactory.exceptionOf(CacheException.NO_POOL_CONFIGURED,
                            "No HSM-backed location is available").toString());
                return Optional.empty();
            }

            Set<String> precious = locations.getPrecious();
            int preciousOnHsm = Sets.intersection(precious, potentialTargets).size();

            LOGGER.debug("{}, precious locations {}, {} already on flush targets.",
                  pnfsId, precious, preciousOnHsm);

            if (preciousOnHsm < missingTapeLocations) {
                Optional<String> toPrecious
                      = Sets.intersection(Sets.difference(potentialSources, precious),
                            potentialTargets)
                      .stream().findAny();
                String source = toPrecious.orElse(potentialSources.stream().findAny().orElse(null));
                String target = toPrecious.orElse(potentialTargets.stream().findAny().orElse(null));
                operation.setSource(source);
                operation.setTarget(target);
                operation.setNeeded(1);
                LOGGER.debug("{}, requesting flush from {} on {}.", pnfsId, source, target);
                return Optional.of(FLUSH);
            }
        }

        return Optional.empty();
    }

    private Optional<QoSAction> checkForLocationsToCache(FileQoSRequirements requirements,
          VerifiedLocations locations,
          VerifyOperation operation,
          boolean verifyHsm) {
        PnfsId pnfsId = requirements.getPnfsId();
        if (verifyHsm) {
            /*
             *  First check to see if there are floating precious files.
             *  We consider only precious locations which are not on HSM pools.
             *  Precious files which were on non-hsm pools but which needed to go to tape
             *  will have been caught by the check for flush which always precedes
             *  this call.
             */
            Set<String> hsmPools = findHsmLocations(operation, requirements, locations);
            Set<String> toCache = Sets.difference(locations.getPrecious(), hsmPools);

            if (!toCache.isEmpty()) {
                String target = toCache.iterator().next();
                LOGGER.debug("handleVerification, {}, precious replica found on non-HSM pool; "
                      + "updating operation to cache: {}", pnfsId, target);
                operation.setTarget(target);
                operation.setNeeded(1);
                return Optional.of(UNSET_PRECIOUS_REPLICA);
            }
        }

        /*
         *  Check for disk required = 0.  If there are sticky replicas, make the first
         *  one target and return CACHE_REPLICA.
         */
        if (requirements.getRequiredDisk() == 0) {
            Set<String> persistent = locations.getPersistent();
            if (!persistent.isEmpty()) {
                String target = persistent.iterator().next();
                LOGGER.debug("handleVerification, {}, no persistent replicas required; "
                      + "updating operation with first sticky target to cache: {}", pnfsId, target);
                operation.setTarget(target);
                operation.setNeeded(1);
                return Optional.of(CACHE_REPLICA);
            }

            LOGGER.debug("handleVerification, {}, no persistent replicas required, "
                        + "nothing to cache; responses {}",
                  pnfsId, locations.getReplicaStatus());
            operation.setNeeded(0);
            return Optional.of(VOID);
        }

        return Optional.empty();
    }

    private Optional<QoSAction> checkForLocationsToEvict(FileQoSRequirements requirements,
          VerifiedLocations locations,
          VerifyOperation operation) {
        Optional<QoSAction> action = checkForChangeToPoolGroup(locations, operation);
        if (action.isPresent()) {
            return action;
        }

        return checkForChangeToPoolTags(requirements, locations, operation);
    }

    private Optional<QoSAction> checkForLocationAdjustment(FileQoSRequirements requirements,
          VerifiedLocations locations,
          VerifyOperation operation) {
        PnfsId pnfsId = requirements.getPnfsId();

        Set<String> occupied = locations.getOccupied();      // replicas in any state
        Set<String> persistent = locations.getPersistent();  // replicas with system sticky bit
        Set<String> cached = locations.getCached();          // replicas without system sticky bit

        Collection<ReplicaStatusMessage> verified = locations.getReplicaStatus();
        Set<String> partitionKeys = requirements.getPartitionKeys();

        /*
         *  number of member pools manually excluded by admins
         */
        int excluded = locations.getExcluded().size();
        int required = requirements.getRequiredDisk();
        int missing = required - persistent.size();

        /*
         *  First compute the missing replicas on the basis of just the readable
         *  replicas.  If this is positive, recompute by adding in all the
         *  excluded locations.  If these satisfy the requirement, void
         *  the operation.  Do not allow caching in this case, since this
         *  would imply decreasing already deficient locations.
         */
        if (missing > 0) {
            missing -= excluded;
            if (missing < 0) {
                missing = 0;
            }
        }

        operation.setNeeded(Math.abs(missing));

        LOGGER.debug("{}, checkForLocationAdjustment; required {}, excluded {}, missing {}.",
              pnfsId, required, excluded, missing);

        String source;
        String target;
        LocationSelector locationSelector = getLocationSelector();

        try {
            /*
             *  Note that if the operation source or target is preset,
             *  and the location is valid, the selection is skipped.
             */
            if (missing < 0) {
                target = operation.getTarget();
                if (target == null || !poolInfoMap.isPoolViable(target, true)
                      || !RepositoryReplicaVerifier.isRemovable(target, verified)) {
                    Set<String> removable
                          = RepositoryReplicaVerifier.areRemovable(persistent, verified);
                    target = locationSelector.selectTargetToCache(requirements,
                          persistent,
                          removable,
                          partitionKeys);
                }

                operation.setTarget(target);
                LOGGER.debug("target to remove: {}", target);

                return Optional.of(CACHE_REPLICA);
            } else if (missing > 0) {
                String viableSource = operation.getSource();

                if (viableSource != null && !poolInfoMap.isPoolViable(viableSource, false)) {
                    viableSource = null;
                }

                target = operation.getTarget();

                String pgroup = operation.getPoolGroup();

                if (target == null) {
                    /*
                     *  See if we can avoid a copy by promoting an existing
                     *  non-sticky replica to sticky.
                     *
                     *  If the source pool is actually a non-sticky replica,
                     *  choose that first.
                     */
                    if (viableSource != null) {
                        source = viableSource;
                        if (cached.contains(source)) {
                            operation.setTarget(viableSource);
                            LOGGER.debug("promoting source to sticky: {}", source);
                            return Optional.of(PERSIST_REPLICA);
                        }
                    }

                    /*
                     *  The pool group may have changed.  Make sure all the
                     *  cached copies are actually still in the pool group.
                     */
                    cached = Sets.intersection(cached, locations.getMembers());
                    target = locationSelector.selectTargetToPersist(requirements,
                          persistent,
                          cached,
                          partitionKeys);

                    if (target != null) {
                        operation.setTarget(target);
                        LOGGER.debug("target to promote to sticky: {}", target);
                        return Optional.of(PERSIST_REPLICA);
                    }

                    target = locationSelector.selectCopyTarget(operation,
                          pgroup,
                          occupied,
                          partitionKeys);
                } else if (!poolInfoMap.isPoolViable(target, true)) {
                    target = locationSelector.selectCopyTarget(operation,
                          pgroup,
                          occupied,
                          partitionKeys);
                }

                LOGGER.debug("target to copy: {}", target);

                /*
                 *  viable may contain both readable and waiting ('from') replicas.
                 *  To avoid failure/retry, choose only the readable.
                 */
                Set<String> strictlyReadable
                      = RepositoryReplicaVerifier.areReadable(locations.getViable(), verified);
                if (viableSource == null) {
                    source = locationSelector.selectCopySource(operation, strictlyReadable);
                } else {
                    source = viableSource;
                }

                LOGGER.debug("source: {}", source);
                operation.setSource(source);
                operation.setTarget(target);
                return Optional.of(COPY_REPLICA);
            } else {
                LOGGER.debug("Nothing to do, VOID operation for {}", pnfsId);
                return Optional.of(VOID);
            }
        } catch (LocationSelectionException e) {
            LOGGER.debug("{} Failed location selection, pool group was {}: {}",
                  pnfsId, operation.getPoolGroup(), e.getMessage());
            return Optional.of(POOL_SELECTION_FAILURE);
        }
    }

    private Optional<QoSAction> checkForChangeToPoolGroup(VerifiedLocations locations,
          VerifyOperation operation) {
        PnfsId pnfsId = locations.getPnfsId();
        String pgroup = operation.getPoolGroup();
        if (pgroup == null) {
            return Optional.empty();
        }

        Set<String> members
              = poolInfoMap.getMemberLocations(pgroup, locations.getCurrentDiskLocations());
        locations.setMembers(members);

        LOGGER.debug("handleVerification {}, valid group member locations {}", pnfsId, members);

        /*
         *  If all the locations are pools no longer belonging to the group,
         *  the operation should be voided.  This usually indicates that
         *  the pools have been moved out of the group (primary) or that
         *  the pools have been entirely removed from the system.
         *
         *  Note that having checked for empty locations above means there
         *  are still replicas of this file in the system.  But if this
         *  pool group is primary, and not SYSTEM (all pools), then the
         *  situation is probably indicative of an attempt to drain the
         *  pools of the group.  It is best here not to take any action
         *  on the other replicas.
         */
        if (members.isEmpty()) {
            operation.setNeeded(0);
            return Optional.of(VOID);
        }

        /*
         *  Before we attempt any evictions, verify that requirements are met by the pool group.
         */
        if (!SYSTEM_PGROUP.equals(pgroup)) {
            Set<String> persistent = locations.getPersistent();
            if (shouldEvictALocation(pgroup, operation, persistent)) {
                LOGGER.debug("handleVerification, a replica should be evicted from {} "
                      + "because of pool group status change.", persistent);
                operation.setNeeded(1);
                return Optional.of(CACHE_REPLICA);
            }
        }

        return Optional.empty();
    }

    private Optional<QoSAction> checkForChangeToPoolTags(FileQoSRequirements requirements,
          VerifiedLocations locations,
          VerifyOperation operation) {
        PnfsId pnfsId = requirements.getPnfsId();
        Set<String> persistent = locations.getPersistent();

        /*
         *  Tagging of the pools may have changed and/or the requirements on
         *  the storage class may have changed.  If this is the case,
         *  the files may need to be redistributed.  This begins by choosing
         *  a location to evict.  When all evictions are done, the new copies
         *  are made from the remaining replica.  Again, this operation is
         *  only done on persistent (sticky) replicas.
         */
        if (shouldEvictALocation(operation,
              persistent,
              locations.getReplicaStatus(),
              requirements.getPartitionKeys())) {
            LOGGER.debug("handleVerification, a replica should be evicted from {}", persistent);
            operation.setNeeded(1);
            return Optional.of(CACHE_REPLICA);
        }

        LOGGER.debug("handleVerification after eviction check, {}, valid replicas {}",
              pnfsId, persistent);
        return Optional.empty();
    }

    private VerifiedLocations classifyLocations(FileQoSRequirements requirements, String poolGroup)
          throws InterruptedException {
        PnfsId pnfsId = requirements.getPnfsId();
        VerifiedLocations qoSLocations = new VerifiedLocations(pnfsId);

        StorageInfo storageInfo = requirements.getAttributes().getStorageInfo();
        List<URI> uriList = storageInfo.locations();

        Collection<String> tapeLocs = new HashSet();
        if (uriList == null) {
            if (storageInfo.isStored()) {
                tapeLocs.add("legacy-placeholder-location.");
            }
        } else {
            uriList.stream().map(URI::toString).forEach(tapeLocs::add);
        }
        qoSLocations.setCurrentTapeLocations(tapeLocs);

        Collection<String> locations = requirements.getAttributes().getLocations();
        qoSLocations.setCurrentDiskLocations(locations);

        LOGGER.debug("classifyLocations {}, namespace locations {}", pnfsId, locations);

        if (locations.isEmpty()) {
            return qoSLocations;
        }

        /*
         * Verify all the locations. The pools are sent a message which returns
         * whether the copy exists, is waiting, readable, removable, and has
         * a sticky bit owned by system.
         */
        Collection<ReplicaStatusMessage> status
              = RepositoryReplicaVerifier.verifyLocations(pnfsId, locations, pools);
        qoSLocations.setReplicaStatus(status);

        LOGGER.debug("classifyLocations {}, verified replicas: {}", pnfsId, status);

        /*
         *  Determine that the readable locations from the namespace actually exist.
         *  This is crucial for the counts.
         */
        Set<String> readable = poolInfoMap.getReadableLocations(locations);
        qoSLocations.setReadable(readable);

        Set<String> exist = RepositoryReplicaVerifier.exist(readable, status);
        qoSLocations.setExist(exist);

        LOGGER.debug("classifyLocations {}, readable replicas {}, verified replicas {}",
              pnfsId, readable, exist);

        Set<String> broken = RepositoryReplicaVerifier.getBroken(status);
        qoSLocations.setBroken(broken);

        Set<String> viable = Sets.difference(exist, broken);
        qoSLocations.setViable(viable);

        /*
         *  Find the persistent (sticky) locations.
         */
        Set<String> persistent = RepositoryReplicaVerifier.areSticky(viable, status);
        qoSLocations.setPersistent(persistent);
        Set<String> members = poolInfoMap.getMemberLocations(poolGroup, locations);
        qoSLocations.setMembers(members);

        LOGGER.debug(
              "classifyLocations {}, namespace locations which are members pool group {}: {}",
              pnfsId, poolGroup, members);

        /*
         *  Find the locations in the namespace that are actually occupied.
         *  This is an optimization so that we can choose a new pool from the
         *  group without failing and retrying the migration with a new target.
         *
         *  In effect, this means eliminating the phantom locations from
         *  the namespace.  We do this by adding back into the verified
         *  locations the offline replica locations.
         */
        qoSLocations.setOccupied(Sets.union(exist, Sets.difference(members, readable)));

        /*
         *  Find the cached (non-sticky) locations.
         *  Partition the sticky locations between usable and excluded.
         */
        qoSLocations.setCached(Sets.difference(viable, persistent));
        Set<String> excluded
              = RepositoryReplicaVerifier.areSticky(poolInfoMap.getExcludedLocationNames(members),
              status);
        qoSLocations.setPersistent(Sets.difference(persistent, excluded));
        qoSLocations.setExcluded(excluded);

        LOGGER.debug("classifyLocations {}: member replicas with a sticky replica "
              + "but which have been manually excluded: {}.", pnfsId, excluded);

        /*
         *  Compute the viable locations which are precious.
         */
        Set<String> precious = RepositoryReplicaVerifier.arePrecious(viable, status);
        qoSLocations.setPrecious(precious);

        LOGGER.debug("classifyLocations {}: viable replicas which are precious: {}.",
              pnfsId, precious);

        return qoSLocations;
    }

    /*
     *  Find all pools that are backed by an HSM which would be eligible as a flush
     *  target for this file.
     *
     *  If the storage info has no URIs for location, an attempt is made to match
     *  against the storage type.  Otherwise, the storage instances for the
     *  list of URIs is used.
     */
    private Set<String> findHsmLocations(VerifyOperation operation,
          FileQoSRequirements requirements,
          VerifiedLocations locations) {
        Set<String> hsmLocations = locations.getHsm();
        if (hsmLocations != null) {
            return hsmLocations;
        }

        /*
         *  Storage info should be present in the attributes.
         */
        List<URI> storageLocations = requirements.getAttributes().getStorageInfo().locations();
        Set<String> hsms;

        if (storageLocations.isEmpty()) {
            hsms = ImmutableSet.of(requirements.getAttributes().getHsm());
        } else {
            hsms = storageLocations.stream().map(URI::getAuthority).collect(Collectors.toSet());
        }

        String unit = operation.getStorageUnit();

        LOGGER.debug("{}, checking for potential HSM locations (storage unit {}, hsms {}).",
              operation.getPnfsId(), unit, hsms);

        hsmLocations = poolInfoMap.getHsmPoolsForStorageUnit(unit, hsms);
        locations.setHsm(hsmLocations);

        return hsmLocations;
    }

    /*
     *  The current file operation is under the aegis of a particular pool group,
     *  which is either a primary group, or SYSTEM (all).  If it is not the latter,
     *  and there are persistent (sticky) replicas on pools in other pool groups,
     *  we want to move them into the primary group.
     *
     *  This can happen, for instance, if a pool group to which the initial write
     *  of the file was linked was not originally marked as primary, but was
     *  subsequently updated to be primary.
     *
     *  Note that the method will not remove the last persistent replica.
     */
    private boolean shouldEvictALocation(String pgroup,
          VerifyOperation operation,
          Collection<String> persistent) {
        if (persistent.size() < 2) {
            return false;
        }

        for (String location : persistent) {
            if (!pgroup.equals(poolInfoMap.getEffectivePoolGroup(location))) {
                operation.setTarget(location);
                return true;
            }
        }

        return false;
    }

    /*
     *  Checks for necessary eviction due to pool tag changes or constraint change.
     *  This call will automatically set the offending location as the target for a
     *  caching operation.
     *
     *  Note that the extractor algorithm will not cache the last replica, because a singleton
     *  will always satisfy any equivalence relation. But we short-circuit this check anyway.
     */
    private boolean shouldEvictALocation(VerifyOperation operation,
          Collection<String> persistent,
          Collection<ReplicaStatusMessage> verified,
          Set<String> partitionKeys) {
        if (persistent.size() < 2) {
            return false;
        }

        EvictingLocationExtractor extractor = getEvictingLocationExtractor(partitionKeys);
        Optional<String> toEvict = extractor.findALocationToEvict(persistent, verified);

        if (toEvict.isPresent()) {
            operation.setTarget(toEvict.get());
            return true;
        }

        return false;
    }

    /*
     *  Called when there are no accessible replicas for the file.
     *
     *  If the file is required to be on disk and it is also on tape, we attempt to stage.
     *  Staging is fire-and-forget. Since the replica is required to be on disk, the
     *  verification after the staging will make sure that it also has the correct
     *  number of replicas.
     */
    private static boolean shouldTryToStage(FileQoSRequirements requirements,
          VerifiedLocations locations) {
        if (requirements.getRequiredDisk() > 0 && !locations.getCurrentTapeLocations().isEmpty()) {
            LOGGER.debug("shouldTryToStage {}, file is on tape and is also required to be on disk.",
                  requirements.getPnfsId());
            return true;
        }
        LOGGER.debug("shouldTryToStage {}, file not on tape", requirements.getPnfsId());
        return false;
    }
}
