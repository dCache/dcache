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
package org.dcache.qos.services.adjuster.handlers;

import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellMessageReceiver;
import java.util.concurrent.ExecutorService;
import org.dcache.pool.migration.PoolMigrationCopyFinishedMessage;
import org.dcache.qos.QoSException;
import org.dcache.qos.data.QoSAction;
import org.dcache.qos.data.QoSAdjustmentStatus;
import org.dcache.qos.listeners.QoSVerificationListener;
import org.dcache.qos.services.adjuster.data.QoSAdjusterTaskMap;
import org.dcache.qos.services.adjuster.util.QoSAdjusterTask;
import org.dcache.qos.vehicles.QoSAdjustmentRequest;
import org.dcache.qos.vehicles.QoSAdjustmentResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registers the request in the map as a new task.  Also serves to relay completion messages to the
 * task from the migration module.  Relays completion of the task to the verification endpoint.
 */
public final class QoSAdjusterTaskHandler implements CellMessageReceiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(QoSAdjusterTaskHandler.class);

    private QoSAdjusterTaskMap taskMap;
    private ExecutorService taskService;
    private QoSVerificationListener verificationListener;

    public void handleAdjustmentRequest(QoSAdjustmentRequest request) {
        LOGGER.debug("handleAdjustmentRequest for {}, {}", request.getPnfsId(),
              request.getAction());
        taskService.submit(() -> {
            LOGGER.debug("handleAdjustmentRequest, registering request for {}, {}",
                  request.getPnfsId(), request.getAction());
            taskMap.register(request);
        });
    }

    public void handleAdjustmentCancelled(PnfsId pnfsId) {
        LOGGER.debug("handleAdjustmentCancelled for {}", pnfsId);
        taskService.submit(() -> {
            LOGGER.debug("handleAdjustmentCancelled, calling cancel on taskMap for {}", pnfsId);
            taskMap.cancel(pnfsId);
        });
    }

    public void notifyAdjustmentCompleted(QoSAdjusterTask task) {
        QoSAdjustmentStatus status;

        if (task.isCancelled()) {
            status = QoSAdjustmentStatus.CANCELLED;
        } else if (task.getException() != null) {
            status = QoSAdjustmentStatus.FAILED;
        } else {
            status = QoSAdjustmentStatus.COMPLETED;
        }

        PnfsId pnfsId = task.getPnfsId();
        QoSAction action = task.getAction();
        Throwable error = task.getException();

        QoSAdjustmentResponse response = new QoSAdjustmentResponse();
        response.setAction(action);
        response.setPnfsId(pnfsId);
        response.setStatus(status);
        response.setError(error);

        try {
            LOGGER.debug("notifying adjustment completed for {}, {}, {}: {}.",
                  pnfsId, action, status, error);
            verificationListener.fileQoSAdjustmentCompleted(response);
        } catch (QoSException e) {
            LOGGER.error("could not notify adjustment completed for {}, {}, {}, {}: {}.",
                  pnfsId, action, status, error, e.toString());
        }
    }

    public void messageArrived(PoolMigrationCopyFinishedMessage message) {
        taskService.submit(() -> taskMap.updateTask(message));
    }

    public void setTaskMap(QoSAdjusterTaskMap taskMap) {
        this.taskMap = taskMap;
    }

    public void setTaskService(ExecutorService taskService) {
        this.taskService = taskService;
    }

    public void setVerificationListener(QoSVerificationListener verificationListener) {
        this.verificationListener = verificationListener;
    }
}
