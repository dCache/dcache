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

import static org.dcache.qos.util.QoSPermissionUtils.canModifyQos;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsId;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.security.auth.Subject;
import org.dcache.namespace.FileAttribute;
import org.dcache.poolmanager.SerializablePoolMonitor;
import org.dcache.qos.QoSDiskSpecification;
import org.dcache.qos.QoSException;
import org.dcache.qos.QoSPolicy;
import org.dcache.qos.QoSState;
import org.dcache.qos.QoSStorageMediumSpecification;
import org.dcache.qos.data.FileQoSRequirements;
import org.dcache.qos.data.FileQoSUpdate;
import org.dcache.qos.services.engine.data.db.JdbcQoSEngineDao;
import org.dcache.qos.services.engine.util.QoSPolicyCache;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * This provider tries to derive requirements from the QoSPolicy of the file.
 * If that is not defined, it falls back to the delegated provider.
 */
public class PolicyBasedQoSProvider extends ALRPStorageUnitQoSProvider {
    public static final Set<FileAttribute> QOS_ATTRIBUTES
          = Collections.unmodifiableSet(EnumSet.of(FileAttribute.QOS_STATE,
          FileAttribute.QOS_POLICY));

    private static final Logger LOGGER = LoggerFactory.getLogger(PolicyBasedQoSProvider.class);

    private QoSPolicyCache policyCache;
    private JdbcQoSEngineDao engineDao;

    public synchronized void messageArrived(SerializablePoolMonitor poolMonitor) {
        setPoolMonitor(poolMonitor);
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
        if (attributes.isDefined(FileAttribute.QOS_POLICY) && attributes.getQosPolicy() == null) {
            /*
             * This is a lazily discovered change, so
             * as a matter of consistency it calls for removal
             * of the pnfsid from the engine's tracking tables.
             */
            engineDao.delete(update.getPnfsId());
            return super.fetchRequirements(update, descriptor);
        }

        return fetchRequirements(update, descriptor);
    }

    @Override
    public FileQoSRequirements fetchRequirements(FileQoSUpdate update, FileQoSRequirements descriptor)
          throws QoSException {
        FileAttributes attributes = descriptor.getAttributes();
        String name = attributes.getQosPolicy();

        if (name == null) {
            return super.fetchRequirements(update, descriptor);
        }

        Optional<QoSPolicy> policy = policyCache.getPolicy(name);
        if (policy.isEmpty()) {
            throw new QoSException("no policy named " + attributes.getQosPolicy() + " found.");
        }

        int stateIndex =
              attributes.isUndefined(FileAttribute.QOS_STATE) ? 0 : attributes.getQosState();
        QoSState qosState = policy.get().getStates().get(stateIndex);

        /*
         *  REVISIT   FileQoSRequirements currently only allows for 1 disk and 1 tape requirement.
         *            This may need to be changed in the future to support (a) different kinds
         *            of disk, and (b) tape locations on different HSM instances.
         */
        int disk = 0;
        int tape = 0;
        Set<String> partitionKeys = new HashSet<>();

        for (QoSStorageMediumSpecification m: qosState.getMedia()) {
            switch (m.getStorageMedium()) {
                case DISK:
                    disk += m.getNumberOfCopies();
                    List<String> keys = ((QoSDiskSpecification) m).getPartitionKeys();
                    if (keys != null) {
                        partitionKeys.addAll(((QoSDiskSpecification) m).getPartitionKeys());
                    }
                    break;
                case HSM:
                    tape += m.getNumberOfCopies();
                    break;
                default:
                    throw new QoSException("unrecognized storage element type "
                          + m.getStorageMedium());
            }
        }

        descriptor.setRequiredDisk(disk);
        descriptor.setRequiredTape(tape);
        if (!partitionKeys.isEmpty()) {
            descriptor.setPartitionKeys(partitionKeys);
        }

        LOGGER.debug("fetchRequirements for {}, returning {}.", update, descriptor);

        return descriptor;
    }

    @Override
    public void handleModifiedRequirements(FileQoSRequirements newRequirements, Subject subject)
          throws CacheException, QoSException {
        PnfsId pnfsId = newRequirements.getPnfsId();

        LOGGER.debug("handleModifiedRequirements for {}.", pnfsId);

        /*
         *  Double-check for the policy attribute.
         */
        FileAttributes currentAttributes = newRequirements.getAttributes();
        if (currentAttributes == null || !currentAttributes.isDefined(FileAttribute.QOS_POLICY)) {
            currentAttributes = fetchAttributes(pnfsId);
        }

        FileAttributes modifiedAttributes = new FileAttributes();

        if (currentAttributes.isDefined(FileAttribute.QOS_POLICY)
            && !newRequirements.hasRequiredQoSPolicy()) {
            modifiedAttributes.setQosPolicy(null);  // remove the policy from the stored state
            modifyRequirements(pnfsId, currentAttributes, modifiedAttributes, newRequirements,
                  subject);
            return;
        }

        if (newRequirements.hasRequiredQoSPolicy()) {
            modifiedAttributes.setQosPolicy(newRequirements.getRequiredQoSPolicy());
            modifiedAttributes.setQosState(newRequirements.getRequiredQoSStateIndex());
        }

        if (canModifyQos(subject, isEnableRoles(), currentAttributes)) {
            pnfsHandler().setFileAttributes(pnfsId, modifiedAttributes);
        } else {
            throw new PermissionDeniedCacheException("User does not have permissions to set "
                  + "attributes for " + newRequirements.getPnfsId());
        }
    }

    @Required
    public void setEngineDao(JdbcQoSEngineDao engineDao) {
        this.engineDao = engineDao;
    }

    @Required
    public void setCache(QoSPolicyCache policyCache) {
        this.policyCache = policyCache;
    }

    protected FileAttributes fetchAttributes(PnfsId pnfsId) throws QoSException {
        FileAttributes required = super.fetchAttributes(pnfsId);
        try {
            LOGGER.debug("fetch qos attributes for {}.", pnfsId);

            FileAttributes qosAttributes = pnfsHandler().getFileAttributes(pnfsId,
                  QOS_ATTRIBUTES);
            required.setQosPolicy(qosAttributes.getQosPolicyIfPresent().orElse(null));
            required.setQosState(qosAttributes.getQosStateIfPresent().orElse(0));
        } catch (CacheException e) {
            LOGGER.debug("qos attribute for {} are not defined; returning required attributes only.", pnfsId);
        }

        return required;
    }


}
