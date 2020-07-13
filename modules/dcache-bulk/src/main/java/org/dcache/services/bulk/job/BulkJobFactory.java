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
package org.dcache.services.bulk.job;

import com.google.common.collect.ImmutableMap;
import diskCacheV111.util.PnfsHandler;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.CellMessageSender;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import javax.security.auth.Subject;
import org.dcache.auth.attributes.Restriction;
import org.dcache.cells.CellStub;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkServiceException;
import diskCacheV111.util.NamespaceHandlerAware;
import org.dcache.pinmanager.PinManagerAware;
import org.dcache.services.bulk.PingServiceAware;
import diskCacheV111.poolManager.PoolManagerAware;
import org.dcache.poolmanager.PoolMonitorAware;
import org.dcache.services.bulk.handlers.BulkJobCompletionHandler;
import org.dcache.vehicles.FileAttributes;
import org.springframework.beans.factory.annotation.Required;

/**
 * Creates jobs on the basis of activity mappings.
 * <p>
 * For each activity (such as pinning, deletion, etc.), there must be an SPI provider which creates
 * the correct class of job and also returns the properly constructed request and expansion job for
 * this activity.
 */
public class BulkJobFactory implements CellLifeCycleAware, CellMessageSender {

  private final Map<String, BulkJobProvider> providers = Collections.synchronizedMap(new HashMap<>());

  private CellStub pnfsManager;
  private CellStub pinManager;
  private CellStub poolManager;
  private CellStub pingService;
  private PoolMonitor poolMonitor;
  private CellEndpoint endpoint;

  @Override
  public void setCellEndpoint(CellEndpoint endpoint) {
    this.endpoint = endpoint;
  }

  @Required
  public void setPinManager(CellStub pinManager) {
    this.pinManager = pinManager;
  }

  @Required
  public void setPingService(CellStub pingService) {
    this.pingService = pingService;
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

  @Override
  public void afterStart() {
    ServiceLoader<BulkJobProvider> serviceLoader
        = ServiceLoader.load(BulkJobProvider.class);
    for (BulkJobProvider provider : serviceLoader) {
      providers.put(provider.getActivity(), provider);
    }
  }

  public Map<String, BulkJobProvider> getProviders() {
    return ImmutableMap.copyOf(providers);
  }

  public BulkRequestJob createRequestJob(BulkRequest request,
                                         Subject subject,
                                         Restriction restriction)
      throws BulkServiceException {
    String activity = request.getActivity();
    BulkJobProvider provider = providers.get(activity);
    if (provider == null) {
      throw new BulkServiceException("cannot create BulkRequestJob; no such activity: " + activity);
    }

    BulkRequestJob job = provider.createRequestJob(request);
    job.setSubject(subject);
    job.setRestriction(restriction);
    configureEndpoints(job);
    return job;
  }

  public TargetExpansionJob createTargetExpansionJob(String target,
                                                     FileAttributes attributes,
                                                     MultipleTargetJob parent)
      throws BulkServiceException {
    BulkRequest request = parent.getRequest();
    BulkJobKey parentKey = parent.getKey();
    String activity = request.getActivity();
    BulkJobProvider provider = providers.get(activity);
    if (provider == null) {
      throw new BulkServiceException("cannot create TargetExpansionJob; no such activity: "
                                      + activity);
    }

    TargetExpansionJob job = provider.createExpansionJob(parentKey, request);
    configureTarget(target, attributes, job);
    configurePermissionsFromParent(parent, job);
    configureEndpoints(job);
    configureHandler(parent, job);
    return job;
  }

  public SingleTargetJob createSingleTargetJob(String target,
                                               BulkJobKey parentKey,
                                               FileAttributes attributes,
                                               MultipleTargetJob parent)
      throws BulkServiceException {
    BulkJobProvider provider = providers.get(parent.getActivity());
    if (provider == null) {
      throw new BulkServiceException("cannot create SingleTargetJob; "
          + "no such activity: "
          + parent.getActivity());
    }

    SingleTargetJob job = provider.createJob(BulkJobKey.newKey(parentKey.getRequestId()), parentKey);
    configureTarget(target, attributes, job);
    configurePermissionsFromParent(parent, job);
    configureEndpoints(job);
    configureHandler(parent, job);
    BulkRequest request = parent.getRequest();
    job.setArguments(request.getArguments());
    job.setPath(MultipleTargetJob.computeFsPath(request.getTargetPrefix(), target));
    return job;
  }

  private void configureEndpoints(BulkJob job) {
    if (job instanceof NamespaceHandlerAware) {
      PnfsHandler pnfsHandler = new PnfsHandler(pnfsManager);
      pnfsHandler.setRestriction(job.getRestriction());
      pnfsHandler.setSubject(job.getSubject());
      ((NamespaceHandlerAware) job).setNamespaceHandler(pnfsHandler);
    }

    if (job instanceof PinManagerAware) {
      ((PinManagerAware) job).setPinManager(pinManager);
    }

    if (job instanceof PoolManagerAware) {
      ((PoolManagerAware) job).setPoolManager(poolManager);
    }

    if (job instanceof PoolMonitorAware) {
      ((PoolMonitorAware) job).setPoolMonitor(poolMonitor);
    }

    if (job instanceof PingServiceAware) {
      ((PingServiceAware) job).setPingService(pingService);
    }

    if (job instanceof CellMessageSender) {
      ((CellMessageSender) job).setCellEndpoint(endpoint);
    }
  }

  private void configureHandler(MultipleTargetJob parent, BulkJob job) {
    BulkJobCompletionHandler handler = parent.getCompletionHandler();
    job.setCompletionHandler(handler);
    handler.addChild(job);
  }

  private void configurePermissionsFromParent(MultipleTargetJob parent, BulkJob job) {
    job.setSubject(parent.getSubject());
    job.setRestriction(parent.getRestriction());
  }

  private void configureTarget(String target, FileAttributes attributes, BulkJob job) {
    job.setTarget(target);
    job.setAttributes(attributes);
  }
}
