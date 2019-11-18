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

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Set;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.PnfsId;

import org.dcache.cells.CellStub;
import org.dcache.resilience.db.LocalNamespaceAccess;
import org.dcache.resilience.db.NamespaceAccess;
import org.dcache.resilience.db.ScanSummary;
import org.dcache.resilience.handlers.FileOperationHandler;
import org.dcache.resilience.handlers.ResilienceMessageHandler;
import org.dcache.resilience.util.ExceptionMessage;
import org.dcache.resilience.util.ReplicaVerifier;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.resilience.ReplicaStatusMessage;

import static org.dcache.resilience.data.MessageType.*;

/**
 * <p>A transient encapsulation of pertinent configuration data regarding
 *      a file location.</p>
 *
 * <p>Implements verification/validation methods to determine if the update
 *      should be registered in the operation map or not.</p>
 *
 * @see FileOperationMap#register(FileUpdate)
 * @see FileOperationHandler#handleLocationUpdate(FileUpdate)
 * @see ResilienceMessageHandler#updatePnfsLocation(FileUpdate)
 * @see LocalNamespaceAccess#handleQuery(Connection, ScanSummary)
 */
public final class FileUpdate {
    private static final Logger LOGGER = LoggerFactory.getLogger(
                    FileUpdate.class);

    /**
     * @return <code>null</code> if AccessLatency is not ONLINE, or if the file
     * is not found or there are no locations for it and the
     * message being processed is for a clear cache location; otherwise
     * the file attribute set required to process resilience.
     */
    public static FileAttributes getAttributes(PnfsId pnfsId, String pool,
                                               MessageType messageType,
                                               NamespaceAccess namespace)
                    throws CacheException {
        try {
            FileAttributes attributes = namespace.getRequiredAttributes(pnfsId);
            if (attributes == null) {
                throw new FileNotFoundCacheException(String.format("No attributes "
                    + "returned for %s", pnfsId));
            }

            LOGGER.trace("Got required attributes for {}.", pnfsId);

            if (attributes.getLocations().isEmpty()) {
                if (messageType == CLEAR_CACHE_LOCATION) {
                    LOGGER.trace("ClearCacheLocationMessage for {}; "
                                                 + "no current locations; "
                                                 + "file probably deleted "
                                                 + "from namespace.",
                                 pnfsId);
                    return null;
                }

                if (messageType != ADD_CACHE_LOCATION) {
                    /*
                     * Since the scan began or the broken file reported,
                     * the file has been removed.
                     */
                    throw new FileNotFoundCacheException
                                    (String.format("File no longer found: %s"
                                                    , pnfsId));
                }

                /*
                 *  May be due to a race between PnfsManager and resilience
                 *  to process the message into/from the namespace.
                 *
                 *  We can assume here that this is a new file, so
                 *  we just add the originating location to the attribute
                 *  location list.
                 */
                LOGGER.trace("{} has no locations yet.", pnfsId);
                Collection<String> singleLoc = new ArrayList<>();

                /*
                 *  Pool can now be <code>null</code>
                 *  but only if message type is QOS_MODIFIED.
                 */
                if (pool == null ) {
                   if (messageType != QOS_MODIFIED) {
                       throw new CacheException(
                                       CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                       String.format("resilience File update for %s, "
                                                                     + "messageType %s, "
                                                                     + "has no pool location!",
                                                     pnfsId, messageType));
                   }
                   return null;
                }

                singleLoc.add(pool);
                attributes.setLocations(singleLoc);
            }

            LOGGER.debug("After call to namespace, {} has locations {}.",
                         pnfsId,
                         attributes.getLocations());
            return attributes;
        } catch (FileNotFoundCacheException e) {
            LOGGER.debug("{}; {} has likely been deleted from the namespace.",
                         e.getMessage(),
                         pnfsId);
            return null;
        }
    }

    public final PnfsId  pnfsId;
    public final MessageType     type;

    private final boolean isFullScan;

    public String          pool;

    private Integer        group;
    private FileAttributes attributes;
    private int            poolIndex;
    private Integer        unitIndex;
    private Integer        count;
    private boolean        fromReload;

    @VisibleForTesting
    public FileUpdate(PnfsId pnfsId,
                      String pool,
                      MessageType type,
                      Integer poolIndex,
                      Integer groupIndex,
                      Integer unitIndex,
                      FileAttributes attributes) {
        this(pnfsId, pool, type, groupIndex, true);
        this.poolIndex = poolIndex;
        this.unitIndex = unitIndex;
        this.attributes = attributes;
    }

    public FileUpdate(PnfsId pnfsId, String pool, MessageType type, boolean full) {
        this(pnfsId, pool, type, null, full);
    }

    /**
     * @param pnfsId of the file.
     * @param pool   either the source of the message, or the pool being scanned.
     * @param type   CORRUPT_FILE, CLEAR_CACHE_LOCATION, ADD_CACHE_LOCATION,
     *               QOS_MODIFIED, POOL_STATUS_DOWN, or POOL_STATUS_UP.
     * @param group  of the pool, if action is not NONE or MODIFY
     *               (can be <code>null</code>).
     * @param full   if true, set the op count to the computed difference
     *               between required and readable; otherwise,
     *               set the op count to 1.
     */
    public FileUpdate(PnfsId pnfsId, String pool, MessageType type,
                      Integer group, boolean full) {
        this.pnfsId = pnfsId;
        this.pool = pool;
        this.type = type;
        this.group = group;
        fromReload = false;
        isFullScan = full;
    }

    public FileAttributes getAttributes() {
        return attributes;
    }

    public Integer getCount() {
        return count;
    }

    public Integer getGroup() {
        return group;
    }

    public int getPoolIndex() {
        return poolIndex;
    }

    public long getSize() { return attributes.getSize(); }

    public Integer getUnitIndex() {
        return unitIndex;
    }

    public Integer getSourceIndex() {
        return type == CORRUPT_FILE ||
               type == CLEAR_CACHE_LOCATION ? null : poolIndex;
    }

    public boolean isParent() {
        return type == POOL_STATUS_DOWN || type == POOL_STATUS_UP;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public void setFromReload(boolean fromReload) {
        this.fromReload = fromReload;
    }

    public String toString() {
        return String.format(
                        "(%s)(%s)(%s)(parent %s)(source %s)"
                                        + "(group %s)(count %s)",
                        pnfsId, pool, type, isParent(), getSourceIndex(),
                        group, count);
    }

    public boolean validateAttributes(NamespaceAccess access)
                    throws CacheException {
        LOGGER.trace("validateAttributes for {}", this);
        attributes = getAttributes(pnfsId, pool, type, access);
        LOGGER.trace("validateAttributes, {}", attributes);
        return attributes != null;
    }

    public boolean validateForAction(Integer storageUnit,
                                     PoolInfoMap poolInfoMap,
                                     ReplicaVerifier verifier,
                                     CellStub pools) {
        /*
         * Storage unit is not recorded in checkpoint, so it should
         * be set here.
         */
        try {
            unitIndex = poolInfoMap.getStorageUnitIndex(attributes);
        } catch (NoSuchElementException e) {
            LOGGER.error("validateForAction, cannot handle {}: {}.",
                         pnfsId, new ExceptionMessage(e));
            return false;
        }

        LOGGER.trace("validateForAction {} got unit from attributes {}.",
                     pnfsId, unitIndex);

        /*
         *  Check to see if this is from a reload of the checkpoint record.
         *  If so, the operation count should be non-null.
         */
        if (fromReload) {
            LOGGER.trace("validateForAction, data was reloaded, restoredCount {}",
                         count);
            return count > 0;
        }

        StorageUnitConstraints constraints
                        = poolInfoMap.getStorageUnitConstraints(unitIndex);

        /*
         *  Ignore all files belonging to non-resilient groups.  This
         *  no longer means simply a requirement of 1, but rather
         *  having no requirement set.
         */
        if (!constraints.isResilient()) {
            LOGGER.trace("validateForAction, storage unit was not resilient: "
                                         + "required = {}",
                         constraints.getRequired());
            return false;
        }

        Collection<String> locations
                        = poolInfoMap.getMemberLocations(group,
                                                         attributes.getLocations());
        /*
         * This indicates that all the locations for the file do not belong
         * to the given pool group.  This could happen if all locations
         * that were once part of the group are removed.  In this case,
         * the operation is a NOP.
         */
        if (locations.isEmpty()) {
            LOGGER.trace("validateForAction, no file locations for {}.", pnfsId);
            return false;
        }

        int required = constraints.getRequired();

        /*
         * Files may be in need of migration even if the correct number
         * exist.  Force the file operation into the table if the
         * storage unit matches the modified one, or if this is a periodic
         * or admin initiated scan.
         *
         * In the former case, the scan was triggered by a change in storage
         * unit requirements.  This could be from an altered number of replicas,
         * or from a change in tag partitioning; even if the required number
         * of copies exist, they may need to be removed and recopied if the
         * tags have changed.
         *
         * The count must thus be the minimum necessary for the worst case
         * scenario -- that is, remove all but one replica and recopy to
         * the required number.  If it turns out this number is more than
         * what is actually needed, the file operation will void itself at
         * that point and quit.
         */
        if (storageUnit == ScanSummary.ALL_UNITS
                        || unitIndex.equals(storageUnit)) {
            /*
             * The maximum number of steps required to redistribute all files
             * would be (N - 1) removes + (required - 1) copies, where N
             * is the max of required and current locations.
             */
            count = Math.max(required, locations.size()) + required - 2;
            return true;
        }

        Collection<ReplicaStatusMessage> verified;

        /*
         * Verify the locations. The pools are sent a message which returns
         * whether the copy exists, is readable, is removable, and has
         * the necessary sticky flag owned by system.
         */
        try {
            verified = verifier.verifyLocations(pnfsId, locations, pools);
            LOGGER.trace("validateForAction, verified locations for {}: {}.",
                         pnfsId,
                         locations);
        } catch (InterruptedException e) {
            LOGGER.warn("validateForAction: replica verification for "
                                        + "{} was interrupted; "
                                        + "cancelling operation.", pnfsId);
            return false;
        }

        /*
         *  Preliminary determination of viable replicas:
         *  eliminate non-readable, non-sticky.
         *
         *  We do not skip broken files here to give resilience
         *  a chance to catch them during the operation.
         *
         *  We also do not check for consistency of namespace here,
         *  since that potential issue only presents itself when there
         *  is a prior action by resilience taken on the file (pnfsid)
         *  in question.
         */
        Set<String> valid = poolInfoMap.getReadableLocations(locations);
        valid = verifier.areSticky(valid, verified);

        /*
         * If the access latency is NEARLINE, set required to 0.
         * We may need to cache all sticky replicas because
         * of a QoS transition.  AccessLatency had better be a defined
         * attribute, or something is wrong.
         */
        if (AccessLatency.NEARLINE.equals(attributes.getAccessLatency())) {
            LOGGER.trace("validateForAction, access latency is NEARLINE, "
                                         + "setting count for {} to 0.",
                         pnfsId);
            required = 0;
        }

        count = required - valid.size();

        LOGGER.debug("validateForAction ({} needs {} replicas, locations {}, "
                                     + "{} valid; difference = {}.",
                     pnfsId, required,
                     locations, valid, count);

        if (count == 0) {
            LOGGER.debug("{}, requirements are already met.", pnfsId);
            return false;
        }

        /*
         * Multiple copies per update are set only when the file is a new
         * entry in the namespace. A pool status change or clear cache
         * location message will trigger only a single migration or single
         * remove.
         */
        count = isFullScan ? Math.abs(count) : 1;

        LOGGER.trace("validateForAction, computed count as {}", count);
        return true;
    }

    public boolean verifyPoolGroup(PoolInfoMap poolInfoMap) {
        poolIndex = poolInfoMap.getPoolIndex(pool);
        if (group != null) {
            return poolInfoMap.isResilientGroup(group);
        }

        group = poolInfoMap.getResilientPoolGroup(poolIndex);
        return group != null;
    }
}
