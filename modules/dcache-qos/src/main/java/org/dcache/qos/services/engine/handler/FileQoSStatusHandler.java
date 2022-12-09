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
package org.dcache.qos.services.engine.handler;

import static org.dcache.qos.data.QoSAction.VOID;
import static org.dcache.qos.data.QoSMessageType.ADD_CACHE_LOCATION;
import static org.dcache.qos.data.QoSMessageType.CLEAR_CACHE_LOCATION;
import static org.dcache.qos.data.QoSMessageType.CORRUPT_FILE;
import static org.dcache.qos.data.QoSMessageType.QOS_MODIFIED;
import static org.dcache.qos.data.QoSMessageType.QOS_MODIFIED_CANCELED;
import static org.dcache.qos.services.engine.util.QoSEngineCounters.QOS_ACTION_COMPLETED;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.NoRouteToCellException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import org.dcache.cells.CellStub;
import org.dcache.cells.MessageReply;
import org.dcache.qos.QoSException;
import org.dcache.qos.data.FileQoSRequirements;
import org.dcache.qos.data.FileQoSUpdate;
import org.dcache.qos.data.QoSAction;
import org.dcache.qos.listeners.QoSActionCompletedListener;
import org.dcache.qos.listeners.QoSRequirementsListener;
import org.dcache.qos.listeners.QoSVerificationListener;
import org.dcache.qos.services.engine.util.QoSEngineCounters;
import org.dcache.qos.vehicles.QoSVerificationRequest;
import org.dcache.vehicles.qos.QoSRequirementsRequestMessage;
import org.dcache.vehicles.qos.QoSTransitionCompletedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the gateway to requirements dispatching.  For now, this is a simple
 * pass-through to the listeners providing the file requirements and forwarding notifications to the
 * verification service.
 */
public final class FileQoSStatusHandler implements CellInfoProvider, QoSActionCompletedListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileQoSStatusHandler.class);

    private QoSRequirementsListener requirementsListener;
    private QoSVerificationListener verificationListener;
    private CellStub qosTransitionTopic;
    private ExecutorService executor;
    private QoSEngineCounters counters;

    @Override
    public void getInfo(PrintWriter pw) {
        StringBuilder builder = new StringBuilder();
        counters.appendRunning(builder);
        counters.appendCounts(builder);
        pw.print(builder.toString());
    }

    public void handleAddCacheLocation(PnfsId pnfsId, String pool) {
        counters.increment(ADD_CACHE_LOCATION.name());
        executor.execute(() -> {
            try {
                LOGGER.debug("handleAddCacheLocation calling fileQoSStatusChanged for {} on {}.",
                      pnfsId, pool);
                fileQoSStatusChanged(new FileQoSUpdate(pnfsId, pool, ADD_CACHE_LOCATION));
            } catch (QoSException e) {
                LOGGER.error("handleAddCacheLocation failed for {} on {}: {}.", pnfsId, pool,
                      e.toString());
            }
        });
    }

    public void handleBrokenFile(PnfsId pnfsId, String pool) {
        counters.increment(CORRUPT_FILE.name());
        executor.execute(() -> {
            try {
                LOGGER.debug("handleBrokenFile calling fileQoSStatusChanged for {} on {}.", pnfsId,
                      pool);
                fileQoSStatusChanged(new FileQoSUpdate(pnfsId, pool, CORRUPT_FILE));
            } catch (QoSException e) {
                LOGGER.error("handleBrokenFile failed for {} on {}: {}.", pnfsId, pool,
                      e.toString());
            }
        });
    }

    public void handleClearCacheLocation(PnfsId pnfsId, String pool) {
        counters.increment(CLEAR_CACHE_LOCATION.name());
        executor.execute(() -> {
            try {
                LOGGER.debug("handleClearCacheLocation calling fileQoSStatusChanged for {} on {}.",
                      pnfsId, pool);
                fileQoSStatusChanged(new FileQoSUpdate(pnfsId, pool, CLEAR_CACHE_LOCATION));
            } catch (QoSException e) {
                /*
                 *  The file was very likely deleted.  Log this only for informational purposes.
                 */
                LOGGER.debug("handleClearCacheLocation for {} on {}: {}.", pnfsId, pool,
                      e.toString());
            }
        });
    }

    public void handleQoSModification(FileQoSRequirements requirements) {
        counters.increment(QOS_MODIFIED.name());
        PnfsId pnfsId = requirements.getPnfsId();
        executor.execute(() -> {
            try {
                LOGGER.debug("handleQoSModification calling fileQoSRequirementsModified for {}.",
                      pnfsId);
                requirementsListener.fileQoSRequirementsModified(requirements);
                LOGGER.debug("handleQoSModification calling fileQoSStatusChanged for {}, {}.",
                      pnfsId, QOS_MODIFIED);
                fileQoSStatusChanged(new FileQoSUpdate(pnfsId, null, QOS_MODIFIED));
            } catch (QoSException | CacheException | NoRouteToCellException | InterruptedException e) {
                LOGGER.error("Failed to handle QoS requirements for {}: {}.",
                      requirements.getPnfsId(), e.getMessage());
                handleActionCompleted(pnfsId, VOID, e.toString());
            }
        });
    }

    public void handleQoSModificationCancelled(PnfsId pnfsId) {
        counters.increment(QOS_MODIFIED_CANCELED.name());
        executor.execute(() -> {
            try {
                LOGGER.debug(
                      "handleQoSModificationCancelled notifying verification listener to cancel {}.",
                      pnfsId);
                verificationListener.fileQoSVerificationCancelled(pnfsId);
            } catch (QoSException e) {
                LOGGER.error("Failed to handle QoS requirements for {}: {}.", pnfsId, e.toString());
            }
        });
    }

    public void handleRequirementsRequestReply(MessageReply<QoSRequirementsRequestMessage> reply,
          QoSRequirementsRequestMessage message) {
        executor.execute(() -> {
            try {
                LOGGER.debug(
                      "handleRequirementsRequestReply calling fileQoSRequirementsRequested for {}.",
                      message.getUpdate());
                message.setRequirements(
                      requirementsListener.fileQoSRequirementsRequested(message.getUpdate()));
                reply.reply(message);
            } catch (QoSException e) {
                reply.fail(message, e);
            } catch (Exception e) {
                reply.fail(message, e);
            }
        });
    }

    public void handleActionCompleted(PnfsId pnfsId, QoSAction action, Serializable error) {
        fileQoSActionCompleted(pnfsId, action, error);
    }

    public void setQoSEngineCounters(QoSEngineCounters counters) {
        this.counters = counters;
    }

    public void setQosTransitionTopic(CellStub qosTransitionTopic) {
        this.qosTransitionTopic = qosTransitionTopic;
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public void setRequirementsListener(QoSRequirementsListener requirementsListener) {
        this.requirementsListener = requirementsListener;
    }

    public void setVerificationListener(QoSVerificationListener verificationListener) {
        this.verificationListener = verificationListener;
    }

    @Override
    public void fileQoSActionCompleted(PnfsId pnfsId, QoSAction action, Serializable error) {
        counters.increment(QOS_ACTION_COMPLETED);
        qosTransitionTopic.notify(new QoSTransitionCompletedMessage(pnfsId, action, error));
    }

    private void fileQoSStatusChanged(FileQoSUpdate update) throws QoSException {
        FileQoSRequirements requirements = requirementsListener.fileQoSRequirementsRequested(
              update);
        QoSVerificationRequest request = new QoSVerificationRequest();
        request.setUpdate(update);
        request.setRequirements(requirements);
        verificationListener.fileQoSVerificationRequested(request);
    }
}
