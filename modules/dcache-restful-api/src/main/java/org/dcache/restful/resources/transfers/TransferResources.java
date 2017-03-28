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
package org.dcache.restful.resources.transfers;

import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.UUID;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TransferInfo;
import org.dcache.restful.providers.transfers.TransferList;
import org.dcache.restful.services.transfers.TransferInfoService;
import org.dcache.restful.util.ServletContextHandlerAttributes;

/**
 * <p>RESTful API to the {@link TransferInfoService} service.</p>
 *
 * @version v1.0
 */
@Path("/transfers")
public final class TransferResources {

    /**
     * <p>Contains the reference to the {@link TransferInfoService} implementation.</p>
     */
    @Context
    ServletContext ctx;

    /**
     * <p>Transfer Metadata.</p>
     * <p>The Transfers endpoint, when given a subpath, returns the metadata
     *      for the user-initiated (not pool-to-pool) transfer operation.
     *      The most recent data is returned if avalailable.</p>
     *
     * @param door The door instance initiating the transfer.
     * @param seq The sequence number of the transfer (for the given door).
     * @return object representing requested Transfer information.
     * @throws CacheException if transfer entry not found.
     */
    @GET
    @Path("/{door : .*}-{seq : .*}")
    @Produces(MediaType.APPLICATION_JSON)
    public TransferInfo getTransferInfo(@PathParam("door") String door,
                                        @PathParam("seq") Long seq)
                    throws CacheException {
        TransferInfo transferInfo = new TransferInfo();
        transferInfo.setCellName(door);
        transferInfo.setSerialId(seq);

        TransferInfoService service
                        = ServletContextHandlerAttributes.getTransferInfoService(ctx);

        return service.populate(transferInfo);
    }

    /**
     * <p>Transfers.</p>
     * <p>The Transfers endpoint returns user-initiated
     * (not pool-to-pool) transfer operations that are currently
     * queued or running.</p>
     *
     * @param token Use the snapshot corresponding to this UUID.
     *              A <code>null</code> value indicates a request for
     *              refreshed (i.e., current) transfers.
     * @param offset Return transfers beginning at this index.
     * @param limit Return at most this number of items.
     * @param pnfsid Return only transfers for this file.
     * @return object containing list of transfers, along with token and
     *          offset information.
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public TransferList getTransfers(@QueryParam("token") UUID token,
                                     @QueryParam("offset") Integer offset,
                                     @QueryParam("limit") Integer limit,
                                     @QueryParam("pnfsid") PnfsId pnfsid) {
        TransferInfoService service
                        = ServletContextHandlerAttributes.getTransferInfoService(ctx);

        return service.get(token, offset, limit, pnfsid);
    }
}