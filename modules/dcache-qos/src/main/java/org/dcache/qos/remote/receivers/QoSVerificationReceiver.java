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

import dmg.cells.nucleus.CellMessageReceiver;
import org.dcache.qos.data.FileQoSUpdate;
import org.dcache.qos.services.verifier.handlers.VerifyOperationHandler;
import org.dcache.qos.util.MessageGuard;
import org.dcache.qos.util.MessageGuard.Status;
import org.dcache.qos.vehicles.QoSAdjustmentResponse;
import org.dcache.qos.vehicles.QoSAdjustmentResponseMessage;
import org.dcache.qos.vehicles.QoSLocationExcludedMessage;
import org.dcache.qos.vehicles.QoSScannerVerificationCancelledMessage;
import org.dcache.qos.vehicles.QoSScannerVerificationRequest;
import org.dcache.qos.vehicles.QoSScannerVerificationRequestMessage;
import org.dcache.qos.vehicles.QoSVerificationCancelledMessage;
import org.dcache.qos.vehicles.QoSVerificationRequest;
import org.dcache.qos.vehicles.QoSVerificationRequestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements message reception for remote verification service.
 * </p>
 * If disabled manually, messages will be dropped.
 */
public final class QoSVerificationReceiver implements CellMessageReceiver {

    private static final Logger ACTIVITY_LOGGER = LoggerFactory.getLogger("org.dcache.qos-log");

    private static final String ADJUSTMENT_RESP_MSG = QoSAdjustmentResponseMessage.class.getName();
    private static final String LOCATION_EXCL_MST = QoSLocationExcludedMessage.class.getName();
    private static final String VERIFICATION_REQ_MSG = QoSVerificationRequestMessage.class.getName();
    private static final String SCAN_VERIFICATION_REQ_MSG = QoSScannerVerificationRequestMessage.class.getName();
    private static final String SCAN_VERIFICATION_CANCL_MSG = QoSScannerVerificationCancelledMessage.class.getName();
    private static final String VERIFICATION_CANCL_MSG = QoSVerificationCancelledMessage.class.getName();

    private MessageGuard messageGuard;
    private VerifyOperationHandler fileOpHandler;

    public void messageArrived(QoSAdjustmentResponseMessage message) {
        QoSAdjustmentResponse response = message.getResponse();
        ACTIVITY_LOGGER.info("Received notice that qos adjustment for file {} has completed.",
              response.getPnfsId());
        if (messageGuard.getStatus(ADJUSTMENT_RESP_MSG, message)
              == Status.DISABLED) {
            return;
        }
        fileOpHandler.handleAdjustmentResponse(response);
    }

    public void messageArrived(QoSLocationExcludedMessage message) {
        ACTIVITY_LOGGER.info("Received notice of location exclusion = {} for pool {} from scanner.",
              message.isExcluded(), message.getLocation());
        if (messageGuard.getStatus(LOCATION_EXCL_MST, message)
              == Status.DISABLED) {
            return;
        }
        fileOpHandler.handleExcludedStatusChange(message.getLocation(), message.isExcluded());
    }

    public void messageArrived(QoSVerificationRequestMessage message) {
        QoSVerificationRequest request = message.getRequest();
        FileQoSUpdate update = request.getUpdate();
        ACTIVITY_LOGGER.info(
              "Received notice of request for verification for {}, pool {}, type {}.",
              update.getPnfsId(), update.getPool(), update.getMessageType());
        if (messageGuard.getStatus(VERIFICATION_REQ_MSG, message)
              == Status.DISABLED) {
            return;
        }
        fileOpHandler.handleVerificationRequest(request);
    }

    public void messageArrived(QoSScannerVerificationRequestMessage message) {
        QoSScannerVerificationRequest request = message.getRequest();
        ACTIVITY_LOGGER.info("Received notice of batched request for verification on "
                    + "{}, type {}, unit {}, forced {}.",
              request.getId(), request.getType(), request.getStorageUnit(), request.isForced());
        if (messageGuard.getStatus(SCAN_VERIFICATION_REQ_MSG, message)
              == Status.DISABLED) {
            return;
        }
        fileOpHandler.handleVerificationRequest(request);
    }

    public void messageArrived(QoSScannerVerificationCancelledMessage message) {
        ACTIVITY_LOGGER.info("Received notice to cancel batched request for verification on "
              + "{}.", message.getPool());
        if (messageGuard.getStatus(SCAN_VERIFICATION_CANCL_MSG, message)
              == Status.DISABLED) {
            return;
        }
        fileOpHandler.handleFileOperationsCancelledForPool(message.getPool());
    }

    public void messageArrived(QoSVerificationCancelledMessage message) {
        ACTIVITY_LOGGER.info("Received notice to cancel request for verification of "
              + "{}.", message.getPnfsId());
        if (messageGuard.getStatus(VERIFICATION_CANCL_MSG, message)
              == Status.DISABLED) {
            return;
        }
        fileOpHandler.handleFileOperationCancelled(message.getPnfsId());
    }

    public void setFileOpHandler(VerifyOperationHandler fileOpHandler) {
        this.fileOpHandler = fileOpHandler;
    }

    public void setMessageGuard(MessageGuard messageGuard) {
        this.messageGuard = messageGuard;
    }
}