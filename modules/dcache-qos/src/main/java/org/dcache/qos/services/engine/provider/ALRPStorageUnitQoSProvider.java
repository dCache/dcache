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
package org.dcache.qos.services.engine.provider;

import static diskCacheV111.util.AccessLatency.NEARLINE;
import static diskCacheV111.util.AccessLatency.ONLINE;
import static diskCacheV111.util.RetentionPolicy.CUSTODIAL;
import static diskCacheV111.util.RetentionPolicy.REPLICA;
import static org.dcache.qos.data.QoSMessageType.CLEAR_CACHE_LOCATION;
import static org.dcache.qos.data.QoSMessageType.QOS_MODIFIED;
import static org.dcache.qos.data.QoSMessageType.SYSTEM_SCAN;
import static org.dcache.qos.data.QoSMessageType.VALIDATE_ONLY;

import com.google.common.annotations.VisibleForTesting;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.StorageUnit;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import dmg.cells.nucleus.CellMessageReceiver;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.poolmanager.SerializablePoolMonitor;
import org.dcache.qos.QoSException;
import org.dcache.qos.data.FileQoSRequirements;
import org.dcache.qos.data.FileQoSUpdate;
import org.dcache.qos.data.QoSMessageType;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standard provisioning of (fixed) file requirements.  Uses access latency, retention policy, and
 * storage unit attributes (required, onlyOneCopyPer) to determine the number and distribution of
 * persistent disk locations and whether the file should be on tape or not (currently limited to a
 * single tape location).
 * <p/>
 * This will eventually be replaced by a more sophisticated "rule-engine" implementation which will
 * permit time-bound transitions and overriding of the default requirements established by a file's
 * storage class.
 * <p/>
 * Not marked final for testing purposes.
 */
public class ALRPStorageUnitQoSProvider implements QoSRequirementsProvider, CellMessageReceiver {

    public static final Set<FileAttribute> REQUIRED_QOS_ATTRIBUTES
          = Collections.unmodifiableSet(EnumSet.of(FileAttribute.PNFSID,
          FileAttribute.ACCESS_LATENCY,
          FileAttribute.RETENTION_POLICY,
          FileAttribute.STORAGEINFO,
          FileAttribute.CHECKSUM,
          FileAttribute.SIZE,
          FileAttribute.TYPE,
          FileAttribute.CACHECLASS,
          FileAttribute.HSM,
          FileAttribute.FLAGS,
          FileAttribute.LOCATIONS,
          FileAttribute.ACCESS_TIME));

    private static final Logger LOGGER = LoggerFactory.getLogger(ALRPStorageUnitQoSProvider.class);

    private CellStub pnfsManager;
    private PoolMonitor poolMonitor;

    public synchronized void messageArrived(SerializablePoolMonitor poolMonitor) {
        setPoolMonitor(poolMonitor);
    }

    /**
     * Exposed for testing purposes.
     */
    @VisibleForTesting
    public synchronized void setPoolMonitor(PoolMonitor poolMonitor) {
        this.poolMonitor = poolMonitor;
    }

    @Override
    public FileQoSRequirements fetchRequirements(FileQoSUpdate update) throws QoSException {
        FileQoSRequirements descriptor = initialize(update);
        if (descriptor == null) {
            /*
             *  Should only happen when a CLEAR CACHE LOCATION finds no locations.
             */
            return null;
        }

        FileAttributes attributes = descriptor.getAttributes();
        AccessLatency accessLatency = attributes.getAccessLatency();
        RetentionPolicy retentionPolicy = attributes.getRetentionPolicy();

        String unitKey = attributes.getStorageClass() + "@" + attributes.getHsm();
        StorageUnit storageUnit = poolSelectionUnit().getStorageUnit(unitKey);
        if (storageUnit == null) {
            throw new QoSException(unitKey + " does not correspond to a storage unit; "
                  + "cannot retrieve requirements for " + descriptor.getPnfsId());
        }

        Integer required = storageUnit.getRequiredCopies();
        List<String> onlyOneCopyPer = storageUnit.getOnlyOneCopyPer();

        if (retentionPolicy == CUSTODIAL) {
            /*
             *  REVISIT -- currently we support only one tape location.
             */
            descriptor.setRequiredTape(1);
        } else {
            descriptor.setRequiredTape(0);
        }

        if (accessLatency == ONLINE) {
            /*
             *  REVISIT -- current override of file AL based on storage unit
             *  REVISIT -- eventually we will want to override the storage unit default for a given file
             */
            descriptor.setRequiredDisk(required == null ? 1 : required);
            if (onlyOneCopyPer != null) {
                descriptor.setPartitionKeys(new HashSet<>(onlyOneCopyPer));
            }
        } else {
            descriptor.setRequiredDisk(0);
        }

        LOGGER.debug("fetchRequirements for {}, returning {}.", update, descriptor);

        return descriptor;
    }

    /*
     *  REVISIT For now, we do not handle changes to number or partitioning of copies.
     */
    @Override
    public void handleModifiedRequirements(FileQoSRequirements newRequirements)
          throws QoSException {
        PnfsId pnfsId = newRequirements.getPnfsId();

        LOGGER.debug("handleModifiedRequirements for {}.", pnfsId);

        /*
         *  Check immediately for unsupported changes.   Currently, this only involves the move
         *  from CUSTODIAL TO REPLICA.
         */
        FileAttributes currentAttributes = newRequirements.getAttributes();
        if (currentAttributes == null || !currentAttributes.isDefined(
              FileAttribute.RETENTION_POLICY)) {
            currentAttributes = fetchAttributes(pnfsId);
        }

        if (currentAttributes.getRetentionPolicy() == CUSTODIAL
              && newRequirements.getRequiredTape() == 0) {
            throw new QoSException("Unsupported transition from tape to disk: "
                  + "QoS currently does not support removal of tape locations.");
        }

        FileAttributes modifiedAttributes = new FileAttributes();
        if (newRequirements.getRequiredDisk() > 0) {
            modifiedAttributes.setAccessLatency(ONLINE);
        } else {
            modifiedAttributes.setAccessLatency(NEARLINE);
        }

        if (newRequirements.getRequiredTape() > 0) {
            modifiedAttributes.setRetentionPolicy(CUSTODIAL);
        } else {
            modifiedAttributes.setRetentionPolicy(REPLICA);
        }

        try {
            pnfsHandler().setFileAttributes(pnfsId, modifiedAttributes);
        } catch (CacheException e) {
            throw new QoSException("Failed to set attributes for " + newRequirements.getPnfsId(),
                  e);
        }
    }

    public void setPnfsManager(CellStub pnfsManager) {
        this.pnfsManager = pnfsManager;
    }

    /*
     *  This is exposed for overriding when testing
     */
    @VisibleForTesting
    protected FileAttributes fetchAttributes(PnfsId pnfsId) throws QoSException {
        try {
            LOGGER.debug("fetchAttributes for {}.", pnfsId);
            return pnfsHandler().getFileAttributes(pnfsId, REQUIRED_QOS_ATTRIBUTES);
        } catch (CacheException e) {
            throw new QoSException(String.format("No attributes returned for %s", pnfsId), e);
        }
    }

    private FileQoSRequirements initialize(FileQoSUpdate update) throws QoSException {
        PnfsId pnfsId = update.getPnfsId();
        QoSMessageType messageType = update.getMessageType();

        LOGGER.debug("initialize {}.", update);

        if (VALIDATE_ONLY == messageType) {
            /*
             *  Do not revalidate the attributes.
             */
            return new FileQoSRequirements(pnfsId, fetchAttributes(pnfsId));
        }

        FileAttributes attributes;
        try {
            attributes = validateAttributes(update);
        } catch (QoSException e) {
            if (update.getMessageType() == CLEAR_CACHE_LOCATION) {
                attributes = null;
            } else {
                throw e;
            }
        }

        if (attributes == null) {
            return null;
        }

        return new FileQoSRequirements(pnfsId, attributes);
    }

    private PnfsHandler pnfsHandler() {
        PnfsHandler pnfsHandler = new PnfsHandler(pnfsManager);
        pnfsHandler.setSubject(Subjects.ROOT);
        pnfsHandler.setRestriction(Restrictions.none());
        return pnfsHandler;
    }

    private synchronized PoolSelectionUnit poolSelectionUnit() throws QoSException {
        if (poolMonitor == null) {
            throw new QoSException("QoSRequirementsProvider: pool monitor not yet available.");
        }

        return poolMonitor.getPoolSelectionUnit();
    }

    /*
     *  Return <code>null</code> if the file is not found or there are no locations
     *  for it and the message being processed is for a clear cache location; otherwise
     *  the file attribute set required to process resilience.
     */
    private FileAttributes validateAttributes(FileQoSUpdate update) throws QoSException {
        PnfsId pnfsId = update.getPnfsId();
        QoSMessageType messageType = update.getMessageType();
        FileAttributes attributes = fetchAttributes(pnfsId);

        LOGGER.debug("validateAttributes, got required attributes for {}.", pnfsId);

        if (messageType == SYSTEM_SCAN || messageType == QOS_MODIFIED) {
            /*
             *  The pool location will be undefined here.
             *  The namespace locations may be empty for QOS_MODIFIED if
             *  it is a tape to disk+tape transition where there are currently
             *  no replicas on disk.
             */
            return attributes;
        }

        if (attributes.getLocations().isEmpty()) {
            if (messageType == CLEAR_CACHE_LOCATION) {
                LOGGER.debug("ClearCacheLocationMessage for {}; no current locations.", pnfsId);
                return null;
            }

            String pool = update.getPool();

            /*
             *  Scan activities and add cache location should not have a null pool.
             */
            if (pool == null) {
                throw new QoSException(String.format("QoS file update for %s, messageType %s, "
                      + "has no pool location!", pnfsId, messageType));
            }

            /*
             *  May be due to a race with PnfsManager to process the message into/from the namespace.
             *  We just add the originating location to the attribute location list.
             */
            LOGGER.debug("{} has no attribute locations yet, adding origination location {}.",
                  pnfsId, pool);
            Collection<String> singleLoc = new ArrayList<>();
            singleLoc.add(pool);
            attributes.setLocations(singleLoc);
        }

        LOGGER.debug("After call to namespace, {} has locations {}.", pnfsId,
              attributes.getLocations());
        return attributes;
    }
}
