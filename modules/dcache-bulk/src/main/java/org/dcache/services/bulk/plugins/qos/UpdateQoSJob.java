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
package org.dcache.services.bulk.plugins.qos;

import static org.dcache.services.bulk.plugins.qos.UpdateQoSJobProvider.TARGET_QOS;

import diskCacheV111.poolManager.PoolManagerAware;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.NamespaceHandlerAware;
import diskCacheV111.util.PnfsHandler;
import dmg.cells.nucleus.NoRouteToCellException;
import java.net.URISyntaxException;
import java.util.concurrent.Executor;
import org.dcache.cells.CellStub;
import org.dcache.pinmanager.PinManagerAware;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.poolmanager.PoolMonitorAware;
import org.dcache.qos.QoSReplyHandler;
import org.dcache.qos.QoSTransitionEngine;
import org.dcache.services.bulk.job.BulkJobKey;
import org.dcache.services.bulk.job.SingleTargetJob;
import org.dcache.util.NetworkUtils;

/**
 * Transition as single file from its current QoS to a new one.
 */
public class UpdateQoSJob extends SingleTargetJob implements NamespaceHandlerAware,
      PinManagerAware,
      PoolManagerAware,
      PoolMonitorAware {

    private CellStub pinManager;
    private CellStub poolManager;
    private PnfsHandler pnfsHandler;
    private PoolMonitor poolMonitor;

    private QoSReplyHandler replyHandler;

    public UpdateQoSJob(BulkJobKey key, BulkJobKey parentKey, String activity) {
        super(key, parentKey, activity);
    }

    @Override
    public synchronized boolean cancel() {
        replyHandler.cancel();
        return super.cancel();
    }

    @Override
    public void setPinManager(CellStub pinManager) {
        this.pinManager = pinManager;
    }

    @Override
    public void setNamespaceHandler(PnfsHandler pnfsHandler) {
        this.pnfsHandler = pnfsHandler;
    }

    @Override
    public void setPoolManager(CellStub poolManager) {
        this.poolManager = poolManager;
    }

    @Override
    public void setPoolMonitor(PoolMonitor poolMonitor) {
        this.poolMonitor = poolMonitor;
    }

    @Override
    public void pollWaiting() {
        if (replyHandler != null) {
            replyHandler.handlePinReply();
        }
    }

    @Override
    protected void doRun() {
        if (arguments == null) {
            setError(new IllegalArgumentException("no target qos given."));
            return;
        }

        String targetQos = arguments.get(TARGET_QOS.getName());

        if (targetQos == null) {
            setError(new IllegalArgumentException("no target qos given."));
            return;
        }

        QoSTransitionEngine engine = new QoSTransitionEngine(poolManager,
              poolMonitor,
              pnfsHandler,
              pinManager);
        replyHandler = new QoSJobReplyHandler(executorService);
        engine.setReplyHandler(replyHandler);

        try {
            engine.adjustQoS(path, targetQos, NetworkUtils.getCanonicalHostName());
        } catch (URISyntaxException | CacheException | NoRouteToCellException e) {
            setError(e);
            return;
        } catch (InterruptedException e) {
            errorObject = e;
            setState(State.CANCELLED);
            completionHandler.jobInterrupted(this);
            LOGGER.trace("doRun interrupted.");
            return;
        }

        if (replyHandler.done()) {
            setState(State.COMPLETED);
        } else {
            setState(State.WAITING);
        }
    }

    private class QoSJobReplyHandler extends QoSReplyHandler {

        public QoSJobReplyHandler(Executor executor) {
            super(executor);
        }

        @Override
        protected void migrationFailure(Object error) {
            LOGGER.error("QoS migration failed: {}, {}.", migrationReply.getPnfsId(),
                  error.toString());
            if (!done()) {
                cancel();
            }
            setError(error);
        }

        @Override
        protected void migrationSuccess() {
            LOGGER.debug("QoS migration succeeded: {}, {}.",
                  migrationReply.getPnfsId(),
                  migrationReply.getPool());
            if (done()) {
                setState(State.COMPLETED);
            }
        }

        @Override
        protected void pinFailure(Object error) {
            LOGGER.error("QoS pin failed: {}, {}.", pinReply.getPnfsId(), error.toString());
            if (!done()) {
                cancel();
            }
            setError(error);
        }

        @Override
        protected void pinSuccess() {
            LOGGER.debug("QoS pinned: {}, {}.", pinReply.getPnfsId(), pinReply.getPinId());

            if (done()) {
                setState(State.COMPLETED);
            }
        }
    }
}
