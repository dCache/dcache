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
package org.dcache.services.bulk.activity;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.RateLimiter;
import diskCacheV111.poolManager.PoolManagerAware;
import diskCacheV111.util.NamespaceHandlerAware;
import diskCacheV111.util.PnfsHandler;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessageSender;
import dmg.cells.nucleus.EnvironmentAware;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.cells.CellStub;
import org.dcache.pinmanager.PinManagerAware;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.poolmanager.PoolMonitorAware;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkServiceException;
import org.dcache.services.bulk.activity.plugin.qos.QoSEngineAware;
import org.dcache.services.bulk.activity.plugin.qos.QoSResponseReceiver;
import org.dcache.services.bulk.activity.retry.BulkTargetRetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * Creates activities on the basis of activity mappings.
 * <p>
 * For each activity (such as pinning, deletion, etc.), there must be an SPI
 * provider which creates
 * the class implementing the activity API contract.
 */
public final class BulkActivityFactory implements CellMessageSender, EnvironmentAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(BulkActivityFactory.class);

    private final Map<String, BulkActivityProvider> providers = Collections.synchronizedMap(
            new HashMap<>());

    private Map<String, BulkTargetRetryPolicy> retryPolicies;
    private Map<String, RateLimiter> rateLimiters;
    private Map<String, String> rateLimiterActivityIndex;
    private Map<String, Object> environment;

    private CellStub pnfsManager;
    private CellStub pinManager;
    private CellStub poolManager;
    private CellStub qosEngine;
    private PoolMonitor poolMonitor;
    private QoSResponseReceiver qoSResponseReceiver;
    private CellEndpoint endpoint;

    private boolean initialized;

    /**
     * Generates an instance of the plugin-specific activity to be used by the
     * request jobs.
     *
     * @param request     being serviced.
     * @param subject     of user who submitted the request.
     * @param restriction for user who submitted the request.
     * @return fully configured instance of the specific activity.
     * @throws BulkServiceException
     */
    public BulkActivity createActivity(BulkRequest request, Subject subject,
          Restriction restriction) throws BulkServiceException {
        String activity = request.getActivity();
        BulkActivityProvider provider = providers.get(activity);
        if (provider == null) {
            throw new BulkServiceException(
                  "cannot create " + activity + "; no such activity.");
        }

        LOGGER.debug("creating instance of activity {} for request {}.", activity,
              request.getUid());

        BulkActivity bulkActivity = provider.createActivity();
        bulkActivity.setSubject(subject);
        bulkActivity.setRestriction(restriction);
        String rateLimiterType = rateLimiterActivityIndex.get(activity);
        if (rateLimiterType != null) {
            bulkActivity.setRateLimiter(rateLimiters.get(rateLimiterType));
        }

        BulkTargetRetryPolicy retryPolicy = retryPolicies.get(activity);
        if (retryPolicy != null) {
            bulkActivity.setRetryPolicy(retryPolicy);
        }

        configureEndpoints(bulkActivity);
        bulkActivity.configure(request.getArguments());

        return bulkActivity;
    }

    public Map<String, BulkActivityProvider> getProviders() {
        return ImmutableMap.copyOf(providers);
    }

    public void initialize() {
        if (!initialized) {
            ServiceLoader<BulkActivityProvider> serviceLoader
                  = ServiceLoader.load(BulkActivityProvider.class);
            for (BulkActivityProvider provider : serviceLoader) {
                provider.configure(environment);
                providers.put(provider.getActivity(), provider);
            }
            initialized = true;
        }
    }

    /**
     * @param activity of the request.
     * @return true if there is a provider present for it.
     */
    public boolean isValidActivity(String activity) {
        return providers.containsKey(activity);
    }

    @Override
    public void setCellEndpoint(CellEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    @Required
    public void setPinManager(CellStub pinManager) {
        this.pinManager = pinManager;
    }

    @Required
    public void setPnfsManager(CellStub pnfsManager) {
        this.pnfsManager = pnfsManager;
    }

    @Required
    public void setPoolManager(CellStub poolManager) {
        this.poolManager = poolManager;
    }

    @Required
    public void setPoolMonitor(PoolMonitor poolMonitor) {
        this.poolMonitor = poolMonitor;
    }

    @Required
    public void setQosEngine(CellStub qosEngine) {
        this.qosEngine = qosEngine;
    }

    @Required
    public void setQoSResponseReceiver(QoSResponseReceiver qoSResponseReceiver) {
        this.qoSResponseReceiver = qoSResponseReceiver;
    }

    @Required
    public void setRateLimiters(Map<String, Double> rates) {
        rateLimiters = rates.entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, e -> RateLimiter.create(e.getValue())));
    }

    @Required
    public void setRateLimiterActivityIndex(Map<String, String> rateLimiterActivityIndex) {
        this.rateLimiterActivityIndex = rateLimiterActivityIndex;
    }

    @Required
    public void setRetryPolicies(Map<String, BulkTargetRetryPolicy> retryPolicies) {
        this.retryPolicies = retryPolicies;
    }

    @Override
    public void setEnvironment(Map<String, Object> environment) {
        this.environment = environment;
    }

    private void configureEndpoints(BulkActivity activity) {
        if (activity instanceof NamespaceHandlerAware) {
            PnfsHandler pnfsHandler = new PnfsHandler(pnfsManager);
            Subject subject = activity.getSubject();
            Restriction restriction = activity.getRestriction();
            if (Subjects.hasAdminRole(subject)) {
                pnfsHandler.setSubject(Subjects.ROOT);
                pnfsHandler.setRestriction(Restrictions.none());
            } else {
                pnfsHandler.setSubject(subject);
                pnfsHandler.setRestriction(restriction);
            }
            ((NamespaceHandlerAware) activity).setNamespaceHandler(pnfsHandler);
        }

        if (activity instanceof PinManagerAware) {
            ((PinManagerAware) activity).setPinManager(pinManager);
        }

        if (activity instanceof PoolManagerAware) {
            ((PoolManagerAware) activity).setPoolManager(poolManager);
        }

        if (activity instanceof PoolMonitorAware) {
            ((PoolMonitorAware) activity).setPoolMonitor(poolMonitor);
        }

        if (activity instanceof QoSEngineAware) {
            QoSEngineAware qoSEngineAware = (QoSEngineAware) activity;
            qoSEngineAware.setQoSEngine(qosEngine);
            qoSEngineAware.setQoSResponseReceiver(qoSResponseReceiver);
        }

        if (activity instanceof CellMessageSender) {
            ((CellMessageSender) activity).setCellEndpoint(endpoint);
        }
    }
}
