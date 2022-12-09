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
package org.dcache.qos.remote.receivers;

import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsAddCacheLocationMessage;
import diskCacheV111.vehicles.PnfsClearCacheLocationMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import java.util.function.Consumer;
import org.dcache.cells.MessageReply;
import org.dcache.qos.services.engine.handler.FileQoSStatusHandler;
import org.dcache.qos.util.MessageGuard;
import org.dcache.qos.util.MessageGuard.Status;
import org.dcache.vehicles.CorruptFileMessage;
import org.dcache.vehicles.qos.QoSActionCompleteMessage;
import org.dcache.vehicles.qos.QoSCancelRequirementsModifiedMessage;
import org.dcache.vehicles.qos.QoSRequirementsModifiedMessage;
import org.dcache.vehicles.qos.QoSRequirementsRequestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements message reception for remote requirements engine.
 * <p/>
 * Supports the reprocessing of backlogged messages received while the service is manually
 * disabled.
 * <p/>
 * Note that the message guard for cache location messages is set to enforce EXTERNAL, meaning only
 * messages not bearing a QOS session id.  This is to prevent the redundant reprocessing of cache
 * location changes triggered by the QOS engine itself.
 */
public final class QoSRequirementsReceiver implements CellMessageReceiver, Consumer<Message> {

    private static final Logger ACTIVITY_LOGGER = LoggerFactory.getLogger("org.dcache.qos-log");

    private FileQoSStatusHandler fileStatusHandler;
    private MessageGuard messageGuard;

    @Override
    public void accept(Message message) {
        if (message instanceof CorruptFileMessage) {
            messageArrived((CorruptFileMessage) message);
        } else if (message instanceof PnfsAddCacheLocationMessage) {
            messageArrived((PnfsAddCacheLocationMessage) message);
        } else if (message instanceof PnfsClearCacheLocationMessage) {
            messageArrived((PnfsClearCacheLocationMessage) message);
        } else if (message instanceof QoSRequirementsRequestMessage) {
            messageArrived((QoSRequirementsRequestMessage) message);
        } else if (message instanceof QoSRequirementsModifiedMessage) {
            messageArrived((QoSRequirementsModifiedMessage) message);
        } else if (message instanceof QoSActionCompleteMessage) {
            messageArrived((QoSActionCompleteMessage) message);
        }
    }

    public void messageArrived(CorruptFileMessage message) {
        ACTIVITY_LOGGER.info("Received notice that file {} on pool {} is corrupt.",
              message.getPnfsId(), message.getPool());
        if (messageGuard.getStatus("CorruptFileMessage", message)
              != Status.EXTERNAL) {
            return;
        }
        fileStatusHandler.handleBrokenFile(message.getPnfsId(), message.getPool());
    }

    public void messageArrived(PnfsAddCacheLocationMessage message) {
        ACTIVITY_LOGGER.info("Received notice that pool {} received file {}.",
              message.getPoolName(), message.getPnfsId());
        if (messageGuard.getStatus("PnfsAddCacheLocationMessage", message)
              != Status.EXTERNAL) {
            return;
        }
        fileStatusHandler.handleAddCacheLocation(message.getPnfsId(), message.getPoolName());
    }

    public void messageArrived(PnfsClearCacheLocationMessage message) {
        ACTIVITY_LOGGER.info("Received notice that pool {} cleared file {}.",
              message.getPoolName(), message.getPnfsId());
        if (messageGuard.getStatus("PnfsClearCacheLocationMessage", message)
              != Status.EXTERNAL) {
            return;
        }
        fileStatusHandler.handleClearCacheLocation(message.getPnfsId(), message.getPoolName());
    }

    public MessageReply<QoSRequirementsRequestMessage> messageArrived(
          QoSRequirementsRequestMessage message) {
        MessageReply<QoSRequirementsRequestMessage> reply = new MessageReply<>();
        if (messageGuard.getStatus("QoSRequirementsRequestMessage", message)
              == Status.DISABLED) {
            return reply;
        }
        fileStatusHandler.handleRequirementsRequestReply(reply, message);
        return reply;
    }

    public QoSRequirementsModifiedMessage messageArrived(QoSRequirementsModifiedMessage message) {
        if (messageGuard.getStatus("QoSRequirementsModifiedMessage", message)
              == Status.DISABLED) {
            return message;
        }
        fileStatusHandler.handleQoSModification(message.getRequirements());
        return message;
    }

    public void messageArrived(QoSCancelRequirementsModifiedMessage message) {
        if (messageGuard.getStatus("QoSCancelRequirementsModifiedMessage", message)
              == Status.DISABLED) {
            return;
        }
        fileStatusHandler.handleQoSModificationCancelled(message.getPnfsId());
    }

    public void messageArrived(QoSActionCompleteMessage message) {
        if (messageGuard.getStatus("QoSActionCompleteMessage", message)
              == Status.DISABLED) {
            return;
        }

        message.getCompletedQoSActions()
              .forEach(action -> fileStatusHandler.handleActionCompleted(action.getPnfsId(),
                    action.getAction(),
                    action.getError()));
    }

    public void setMessageGuard(MessageGuard messageGuard) {
        this.messageGuard = messageGuard;
    }

    public void setFileStatusHandler(FileQoSStatusHandler fileStatusHandler) {
        this.fileStatusHandler = fileStatusHandler;
    }
}
