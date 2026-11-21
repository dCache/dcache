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
package org.dcache.restful.resources.namespace;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import dmg.util.Exceptions;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.InternalServerErrorException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.List;
import org.dcache.cells.CellStub;
import org.dcache.http.PathMapper;
import org.dcache.qos.QoSPolicyStat;
import org.dcache.qos.data.FileQosPolicyInfo;
import org.dcache.restful.util.HandlerBuilders;
import org.dcache.restful.util.Responses;
import org.dcache.vehicles.qos.FileQoSPolicyInfoMessage;
import org.dcache.vehicles.qos.PnfsManagerGetQoSPolicyStatsMessage;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * <p>RESTful API to which allows one to get information about a file's QoS policy and state,
 *       as well as current statistics on the number of files in each policy state.</p>
 *
 * @version v1.0
 */
@Component
@Tag(name = "namespace")
@SecurityRequirement(name = "basicAuth")
@Path("/qos-policy")
public class QosPolicyResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(
                    QosPolicyResource.class);

    @Context
    private HttpServletRequest request;

    @Inject
    @Named("pnfs-stub")
    private CellStub pnfsmanager;

    @Inject
    private PathMapper pathMapper;

    @Inject
    @Named("qos-engine")
    private CellStub qosEngine;

    @GET
    @Operation(summary = "Retrieve the current count of files in the namespace by policy and "
          + "state.")
    @ApiResponses({
          @ApiResponse(responseCode = "400", description = "Bad Request"),
          @ApiResponse(responseCode = "401", description = "Unauthorized"),
          @ApiResponse(responseCode = "403", description = "Forbidden"),
          @ApiResponse(responseCode = "404", description = "Not Found"),
          @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Path("stats")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPolicyStats() {
        PnfsManagerGetQoSPolicyStatsMessage message = new PnfsManagerGetQoSPolicyStatsMessage();

        try {
            message = pnfsmanager.sendAndWait(message);
        } catch (CacheException e) {
            throw new BadRequestException(e);
        } catch (JSONException | IllegalArgumentException e) {
            throw new BadRequestException(e);
        } catch (Exception e) {
            LOGGER.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        }

        List<QoSPolicyStat> result = message.getPolicyStats();
        return Responses.buildResponse(result);
    }

    @GET
    @Operation(summary = "Retrieve the QoSPolicy name and status for this file pnfsid.")
    @ApiResponses({
          @ApiResponse(responseCode = "400", description = "Bad Request"),
          @ApiResponse(responseCode = "401", description = "Unauthorized"),
          @ApiResponse(responseCode = "403", description = "Forbidden"),
          @ApiResponse(responseCode = "404", description = "Not Found"),
          @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Path("id/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPolicyInfoForPnfsId(
          @Parameter(description = "The pnfsid of the file for which to retrieve policy info.")
          @PathParam("id")String id) {
        FileQoSPolicyInfoMessage message = new FileQoSPolicyInfoMessage(new PnfsId(id));

        try {
            message = qosEngine.sendAndWait(message);
        } catch (CacheException e) {
            throw new BadRequestException(e);
        } catch (JSONException | IllegalArgumentException e) {
            throw new BadRequestException(e);
        } catch (Exception e) {
            LOGGER.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        }

        FileQosPolicyInfo result = message.getQosPolicyInfo();
        return Responses.buildResponse(result);
    }

    @GET
    @Operation(summary = "Retrieve the QoSPolicy name and status for this file path.")
    @ApiResponses({
          @ApiResponse(responseCode = "400", description = "Bad Request"),
          @ApiResponse(responseCode = "401", description = "Unauthorized"),
          @ApiResponse(responseCode = "403", description = "Forbidden"),
          @ApiResponse(responseCode = "404", description = "Not Found"),
          @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Path("path/{path : .*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPolicyInfoForPath(
          @Parameter(description = "The path of the file for which to retrieve policy info.")
          @PathParam("path")String requestPath) {
        FileQoSPolicyInfoMessage message;
        PnfsHandler handler = HandlerBuilders.unrestrictedPnfsHandler(pnfsmanager);
        FsPath path;
        try {
            path = pathMapper.asDcachePath(request, requestPath, ForbiddenException::new, handler);
            message = new FileQoSPolicyInfoMessage(path);
            message = qosEngine.sendAndWait(message);
        } catch (CacheException e) {
            throw new BadRequestException(e);
        } catch (JSONException | IllegalArgumentException e) {
            throw new BadRequestException(e);
        } catch (Exception e) {
            LOGGER.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        }

        FileQosPolicyInfo stat = message.getQosPolicyInfo();
        stat.setPath(path);
        return Responses.buildResponse(stat);
    }

}
