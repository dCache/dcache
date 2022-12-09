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
package org.dcache.qos.remote.clients;

import com.google.common.util.concurrent.ListenableFuture;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.NoRouteToCellException;
import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import org.dcache.cells.CellStub;
import org.dcache.qos.QoSException;
import org.dcache.qos.data.FileQoSRequirements;
import org.dcache.qos.data.FileQoSUpdate;
import org.dcache.qos.listeners.QoSRequirementsListener;
import org.dcache.qos.util.CacheExceptionUtils;
import org.dcache.vehicles.qos.QoSCancelRequirementsModifiedMessage;
import org.dcache.vehicles.qos.QoSRequirementsModifiedMessage;
import org.dcache.vehicles.qos.QoSRequirementsRequestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Use this client when communicating with a remote requirements engine.
 */
public final class RemoteQoSRequirementsClient implements QoSRequirementsListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteQoSRequirementsClient.class);

    private CellStub requirementsService;

    @Override
    public FileQoSRequirements fileQoSRequirementsRequested(FileQoSUpdate update)
          throws QoSException {
        QoSRequirementsRequestMessage message = new QoSRequirementsRequestMessage(update);
        ListenableFuture<QoSRequirementsRequestMessage> future = requirementsService.send(message);

        try {
            message = future.get();
            LOGGER.trace("Received reply for requirements request {}.", message);
        } catch (InterruptedException | ExecutionException e) {
            throw new QoSException("Failed to request requirements for " + update, e);
        }

        Serializable error = message.getErrorObject();

        if (error != null) {
            throw new QoSException("Failed to request requirements for " + update,
                  CacheExceptionUtils.getCacheExceptionFrom(error));
        }

        return message.getRequirements();
    }

    @Override
    public void fileQoSRequirementsModified(FileQoSRequirements newRequirements)
          throws CacheException, NoRouteToCellException, InterruptedException {
        requirementsService.sendAndWait(new QoSRequirementsModifiedMessage(newRequirements));
    }

    @Override
    public void fileQoSRequirementsModifiedCancelled(PnfsId pnfsid) throws QoSException {
        /*
         *  Fire and forget. The sender will need to listen for a response.
         */
        requirementsService.send(new QoSCancelRequirementsModifiedMessage(pnfsid));
    }

    public void setRequirementsService(CellStub requirementsService) {
        this.requirementsService = requirementsService;
    }
}
