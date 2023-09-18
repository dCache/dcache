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
package org.dcache.qos.services.engine.util;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Throwables;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.NoRouteToCellException;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import javax.security.auth.Subject;
import org.dcache.auth.Subjects;
import org.dcache.cells.CellStub;
import org.dcache.qos.QoSPolicy;
import org.dcache.vehicles.qos.PnfsManagerAddQoSPolicyMessage;
import org.dcache.vehicles.qos.PnfsManagerGetQoSPolicyMessage;
import org.dcache.vehicles.qos.PnfsManagerInvalidateQoSPolicyMessage;
import org.dcache.vehicles.qos.PnfsManagerListQoSPoliciesMessage;
import org.dcache.vehicles.qos.PnfsManagerRmQoSPolicyMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 *   Serves as a write-through cache for QoS Policies.   The cache is populated
 *   lazily from the namespace via messaging.  Updates to the policies in the
 *   namespace (e.g., from the RESTful services) first modify the policy here,
 *   then pass it on to the namespace.
 *
 *   Read and list are open access; update and delete require root.
 */
public class QoSPolicyCache implements CellMessageReceiver{

    private static final Logger LOGGER = LoggerFactory.getLogger(QoSPolicyCache.class);

    class PolicyLoader implements CacheLoader<String, QoSPolicy> {

        @Override
        public QoSPolicy load(String name) throws Exception {
            PnfsManagerGetQoSPolicyMessage msg = new PnfsManagerGetQoSPolicyMessage(name);
            msg.setSubject(Subjects.ROOT);
            msg = pnfs.sendAndWait(msg);
            if (msg.getReturnCode() == 0) {
                return msg.getPolicy();
            }
            LOGGER.error("policy cache loader failed to get {}: {}", name, msg.getErrorObject());
            return null;
        }
    }

    private LoadingCache<String, QoSPolicy> policyCache;
    private CellStub pnfs;
    private long expiry;
    private TimeUnit expiryUnit;
    private long capacity;

    @Required
    public void setPnfs(CellStub pnfs) {
        this.pnfs = pnfs;
    }

    @Required
    public void setExpiry(long expiry) {
        this.expiry = expiry;
    }

    @Required
    public void setExpiryUnit(TimeUnit expiryUnit) {
        this.expiryUnit = expiryUnit;
    }

    @Required
    public void setCapacity(long capacity) {
        this.capacity = capacity;
    }

    public void initialize() {
        policyCache = Caffeine.newBuilder()
              .expireAfterAccess(expiry, expiryUnit)
              .maximumSize(capacity)
              .build(new PolicyLoader());
    }

    public PnfsManagerGetQoSPolicyMessage messageArrived(PnfsManagerGetQoSPolicyMessage message)
          throws CacheException {
        Optional<QoSPolicy> policy = getPolicy(message.getPolicyName());
        if (policy.isPresent()) {
            message.setPolicy(policy.get());
            message.setSucceeded();
        } else {
            message.setFailed(CacheException.RESOURCE, "could not get policy.");
        }
        return message;
    }

    public PnfsManagerRmQoSPolicyMessage messageArrived(PnfsManagerRmQoSPolicyMessage message) throws CacheException {
        Subject subject = message.getSubject();
        if (!Subjects.isRoot(subject) && !Subjects.hasAdminRole(subject)) {
            throw new PermissionDeniedCacheException("not authorized to remove policies.");
        }
        removePolicy(message.getPolicyName());
        message.setSucceeded();
        return message;
    }

    /**
     * Updates to an existing policy are not allowed as they would require a complicated transactional
     * handling of files already having this policy in various states.
     */
    public PnfsManagerAddQoSPolicyMessage messageArrived(PnfsManagerAddQoSPolicyMessage message)
          throws CacheException {
        Subject subject = message.getSubject();
        if (!Subjects.isRoot(subject) && !Subjects.hasAdminRole(subject)) {
            throw new PermissionDeniedCacheException("not authorized to update policies.");
        }

        QoSPolicy policy = message.getPolicy();
        try {
            PnfsManagerAddQoSPolicyMessage msg = new PnfsManagerAddQoSPolicyMessage(policy);
            msg.setSubject(Subjects.ROOT);
            pnfs.sendAndWait(msg);
        } catch (CacheException e) {
            String rootError = String.valueOf(Throwables.getRootCause(e));
            LOGGER.error("problem updating {}: {}.", policy.getName(), rootError);
            message.setFailed(e.getRc(), rootError);
        } catch (InterruptedException | NoRouteToCellException e) {
            LOGGER.error("problem updating {}: {}.", policy.getName(), e.toString());
            message.setFailed(CacheException.TIMEOUT, e.toString());
        }

        return message;
    }

    public PnfsManagerListQoSPoliciesMessage messageArrived(PnfsManagerListQoSPoliciesMessage message) {
        try {
            message = pnfs.sendAndWait(message);
            message.setSucceeded();
        } catch (CacheException e) {
            message.setFailed(e.getRc(), e);
        } catch (NoRouteToCellException e) {
            message.setFailed(CacheException.SERVICE_UNAVAILABLE, e);
        } catch (InterruptedException e) {
            message.setFailed(CacheException.TIMEOUT, e);
        }
        return message;
    }

    public Optional<QoSPolicy> getPolicy(String name) {
        try {
            return Optional.ofNullable(policyCache.get(name));
        } catch (CompletionException e) {
            LOGGER.error("problem getting {}: {}.", name, String.valueOf(Throwables.getRootCause(e)));
            return Optional.empty();
        }
    }

    public void removePolicy(String name) {
        policyCache.invalidate(name);
        try {
            PnfsManagerRmQoSPolicyMessage msg = new PnfsManagerRmQoSPolicyMessage(name);
            msg.setSubject(Subjects.ROOT);
            msg = pnfs.sendAndWait(msg);
            /*
             *  do this asynchronously, as the update may take a while.
             */
            pnfs.send(new PnfsManagerInvalidateQoSPolicyMessage(msg.getPolicyId()));
        } catch (CacheException e) {
            LOGGER.error("problem removing {}: {}.", name, String.valueOf(Throwables.getRootCause(e)));
        } catch (InterruptedException | NoRouteToCellException e) {
            LOGGER.error("problem removing {}: {}.", name, e.toString());
        }
    }
}
