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
package org.dcache.restful.util.bulk;

import static javax.ws.rs.core.Response.Status.TOO_MANY_REQUESTS;

import com.google.common.base.Throwables;
import dmg.util.Exceptions;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import org.dcache.cells.CellStub;
import org.dcache.services.bulk.BulkPermissionDeniedException;
import org.dcache.services.bulk.BulkQuotaExceededException;
import org.dcache.services.bulk.BulkRequestNotFoundException;
import org.dcache.services.bulk.BulkServiceException;
import org.dcache.services.bulk.BulkServiceMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * For injection convenience.
 */
public class BulkServiceCommunicator {

    private static final Logger LOGGER
          = LoggerFactory.getLogger(BulkServiceCommunicator.class);

    protected CellStub bulkService;

    public <M extends BulkServiceMessage> M send(M message)
          throws ClientErrorException, InternalServerErrorException {
        Future<M> future = bulkService.send(message);

        try {
            message = future.get();
            LOGGER.trace("BulkServiceMessage reply: {}.", message);
        } catch (InterruptedException | ExecutionException e) {
            throw new BadRequestException(e.getMessage(), e);
        }

        checkError(message);

        return message;
    }

    @Required
    public void setBulkService(CellStub bulkService) {
        this.bulkService = bulkService;
    }

    private void checkError(BulkServiceMessage message) {
        Throwable error = (Throwable) message.getErrorObject();

        if (error == null) {
            return;
        }

        LOGGER.trace("check error, received {}.", error.toString());

        Throwables.throwIfUnchecked(error);

        if (error instanceof BulkPermissionDeniedException) {
            throw new ForbiddenException(error);
        } else if (error instanceof BulkQuotaExceededException) {
            throw new ClientErrorException(TOO_MANY_REQUESTS, error);
        } else if (error instanceof BulkRequestNotFoundException) {
            throw new NotFoundException(error);
        } else if (error instanceof BulkServiceException) {
            throw new BadRequestException(error);
        } else {
            LOGGER.warn(Exceptions.meaningfulMessage(error));
            throw new InternalServerErrorException(error);
        }
    }
}
