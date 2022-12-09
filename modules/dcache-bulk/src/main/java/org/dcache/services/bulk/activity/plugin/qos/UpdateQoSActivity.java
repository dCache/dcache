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
package org.dcache.services.bulk.activity.plugin.qos;

import static com.google.common.util.concurrent.Uninterruptibles.getUninterruptibly;
import static org.dcache.services.bulk.activity.plugin.qos.UpdateQoSActivityProvider.TARGET_QOS;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NamespaceHandlerAware;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.NoRouteToCellException;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import org.dcache.cells.CellStub;
import org.dcache.qos.QoSException;
import org.dcache.qos.data.FileQoSRequirements;
import org.dcache.qos.remote.clients.RemoteQoSRequirementsClient;
import org.dcache.services.bulk.BulkServiceException;
import org.dcache.services.bulk.activity.BulkActivity;
import org.dcache.services.bulk.util.BulkRequestTarget;
import org.dcache.services.bulk.util.BulkRequestTarget.State;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.qos.QoSTransitionCompletedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transition a single file from its current QoS to a new one.
 * <p>
 * Communicates with the QoSEngine.
 */
public class UpdateQoSActivity extends BulkActivity<QoSTransitionCompletedMessage> implements
      QoSEngineAware, NamespaceHandlerAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateQoSActivity.class);

    private QoSResponseReceiver responseReceiver;
    private CellStub qosEngine;
    private PnfsHandler pnfsHandler;
    private String targetQos;

    public UpdateQoSActivity(String name, TargetType targetType) {
        super(name, targetType);
    }

    @Override
    public synchronized void cancel(BulkRequestTarget target) {
        RemoteQoSRequirementsClient client = new RemoteQoSRequirementsClient();
        client.setRequirementsService(qosEngine);
        PnfsId pnfsId = target.getAttributes().getPnfsId();
        try {
            client.fileQoSRequirementsModifiedCancelled(pnfsId);
        } catch (QoSException e) {
            LOGGER.error("fileQoSRequirementsModifiedCancelled failed: {}, {}.", pnfsId,
                  e.getMessage());
        }
        responseReceiver.cancel(pnfsId.toString());
        super.cancel(target);
    }

    @Override
    public ListenableFuture<QoSTransitionCompletedMessage> perform(String rid, long tid,
          FsPath path, FileAttributes attributes) throws BulkServiceException {
        if (targetQos == null) {
            return Futures.immediateFailedFuture(new IllegalArgumentException("no target qos given."));
        }

        if (attributes == null) {
            try {
                attributes = pnfsHandler.getFileAttributes(path, MINIMALLY_REQUIRED_ATTRIBUTES);
            } catch (CacheException e) {
                throw new BulkServiceException("failed to retrieve file attributes", e);
            }
        }

        PnfsId pnfsId = attributes.getPnfsId();
        FileQoSRequirements requirements = new FileQoSRequirements(pnfsId, attributes);
        if (targetQos.contains("disk")) {
            requirements.setRequiredDisk(1);
        }

        if (targetQos.contains("tape")) {
            requirements.setRequiredTape(1);
        }

        QoSTransitionFuture future = responseReceiver.register(pnfsId.toString());

        RemoteQoSRequirementsClient client = new RemoteQoSRequirementsClient();
        client.setRequirementsService(qosEngine);

        try {
            client.fileQoSRequirementsModified(requirements);
        } catch (CacheException | InterruptedException | NoRouteToCellException e) {
            return Futures.immediateFailedFuture(e);
        }

        return future;
    }

    @Override
    protected void configure(Map<String, String> arguments) {
        if (arguments == null) {
            return;
        }
        targetQos = arguments.get(TARGET_QOS.getName());
    }

    @Override
    public void handleCompletion(BulkRequestTarget target,
          ListenableFuture<QoSTransitionCompletedMessage> future) {
        QoSTransitionCompletedMessage message;
        try {
            message = getUninterruptibly(future);
            Object error = message.getErrorObject();
            if (error != null) {
                target.setErrorObject(error);
            } else {
                target.setState(State.COMPLETED);
            }
        } catch (CancellationException e) {
            /*
             *  CANCELLED is set elsewhere
             */
        } catch (ExecutionException e) {
            target.setErrorObject(e.getCause());
        }
    }

    @Override
    public void setQoSEngine(CellStub qosEngine) {
        this.qosEngine = qosEngine;
    }

    @Override
    public void setQoSResponseReceiver(QoSResponseReceiver responseReceiver) {
        this.responseReceiver = responseReceiver;
    }

    @Override
    public void setNamespaceHandler(PnfsHandler pnfsHandler) {
        this.pnfsHandler = pnfsHandler;
    }
}



