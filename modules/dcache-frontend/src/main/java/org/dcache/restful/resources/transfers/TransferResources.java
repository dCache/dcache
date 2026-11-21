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

import diskCacheV111.util.CacheException;
import diskCacheV111.util.TransferInfo;
import dmg.util.Exceptions;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Inject;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.InternalServerErrorException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.UUID;
import org.dcache.restful.providers.SnapshotList;
import org.dcache.restful.services.transfers.TransferInfoService;
import org.dcache.restful.util.RequestUser;
import org.dcache.restful.util.Responses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * <p>RESTful API to the {@link TransferInfoService} service.</p>
 *
 * @version v1.0
 */
@Component
@Tag(name = "transfers")
@SecurityRequirement(name = "basicAuth")
@Path("/transfers")
public final class TransferResources {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransferResources.class);

    @Inject
    private TransferInfoService service;
    private boolean unlimitedOperationVisibility;

    @GET
    @Operation(summary = "Provide a list of all client-initiated transfers that are "
          + "either queued or currently running.  Internal (pool-to-pool) "
          + "transfers are excluded.")
    @ApiResponses({
          @ApiResponse(responseCode = "403",
                description = "User subject must contain uid to access transfers."),
          @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTransfers(@Parameter(description = "Use the snapshot "
          + "corresponding to this UUID.  The "
          + "contract with the service is that if the "
          + "parameter value is null, the current snapshot "
          + "will be used, regardless of whether offset and "
          + "limit are still valid.  Initial/refresh "
          + "calls should always be without a token.  "
          + "Subsequent calls should send back the "
          + "current token; in the case that it no "
          + "longer corresponds to the current list, "
          + "the service will return a null token "
          + "and an empty list, and the client will "
          + "need to recall the method without a "
          + "token (refresh).")
    @QueryParam("token") UUID token,
          @Parameter(description = "The number of items to skip.")
          @QueryParam("offset") Integer offset,
          @Parameter(description = "The maximum number items to return.")
          @QueryParam("limit") Integer limit,
          @Parameter(description = "Select transfers in this state (NOTFOUND, STAGING, QUEUED, "
                + "RUNNING, CANCELED, DONE)")
          @QueryParam("state") String state,
          @Parameter(description = "Select transfers initiated through this door.")
          @QueryParam("door") String door,
          @Parameter(description = "Select transfers initiated through a door in this domain.")
          @QueryParam("domain") String domain,
          @Parameter(description = "Select transfers using this protocol.")
          @QueryParam("prot") String protocol,
          @Parameter(description = "Select transfers initiated by this user.")
          @QueryParam("uid") String uid,
          @Parameter(description = "Select transfers initiated by a member of this group.")
          @QueryParam("gid") String gid,
          @Parameter(description = "Select transfers initiated by a member of this vomsgroup.")
          @QueryParam("vomsgroup") String vomsgroup,
          @Parameter(description = "Select transfers involving this path.")
          @QueryParam("path") String path,
          @Parameter(description = "Select transfers involving this pnfsid.")
          @QueryParam("pnfsid") String pnfsid,
          @Parameter(description = "Select transfers involving this pool.")
          @QueryParam("pool") String pool,
          @Parameter(description = "Select transfers involving this client.")
          @QueryParam("client") String client,
          @Parameter(description = "A comma-seperated list of fields to sort the responses.")
          @DefaultValue("door,waiting")
          @QueryParam("sort") String sort) {
        try {
            Long suid = RequestUser.getSubjectUidForFileOperations(unlimitedOperationVisibility);

            SnapshotList<TransferInfo> result = service.get(token,
                    offset,
                    limit,
                    suid == null ? null : String.valueOf(suid),
                    state,
                    door,
                    domain,
                    protocol,
                    uid,
                    gid,
                    vomsgroup,
                    path,
                    pnfsid,
                    pool,
                    client,
                    sort);
            return Responses.buildResponse(result, result.getTimeOfCreation());
        } catch (CacheException e) {
            LOGGER.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        }
    }

    public void setUnlimitedOperationVisibility(boolean visibility) {
        unlimitedOperationVisibility = visibility;
    }
}