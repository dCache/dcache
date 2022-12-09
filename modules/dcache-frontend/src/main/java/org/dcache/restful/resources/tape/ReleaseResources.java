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
package org.dcache.restful.resources.tape;

import static org.dcache.restful.util.RequestUser.getRestriction;
import static org.dcache.restful.util.RequestUser.getSubject;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.dcache.auth.attributes.Restriction;
import org.dcache.restful.util.bulk.BulkServiceCommunicator;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkRequest.Depth;
import org.dcache.services.bulk.BulkRequestMessage;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

/**
 * <p>RESTful API to release (Bulk).</p>
 *
 * @version v1.0
 */
@Component
@Api(value = "tape", authorizations = {@Authorization("basicAuth")})
@Path("tape/release")
public final class ReleaseResources {

    @Context
    private HttpServletRequest request;

    @Inject
    private BulkServiceCommunicator service;

    /**
     * Release files belonging to a bulk STAGE request.
     * <p>
     * NOTE:  users logged in with the admin role will be submitting the request as ROOT (0:0).
     *
     * @return response which includes a location HTTP response header with a value that is the
     * absolute URL for the resource associated with this bulk request.
     */
    @POST
    @ApiOperation(value = "RELEASE files associated with a STAGE request.")
    @ApiResponses({
          @ApiResponse(code = 201, message = "Created"),
          @ApiResponse(code = 400, message = "Bad request"),
          @ApiResponse(code = 401, message = "Unauthorized"),
          @ApiResponse(code = 403, message = "Forbidden"),
          @ApiResponse(code = 429, message = "Too many requests"),
          @ApiResponse(code = 500, message = "Internal Server Error")
    })
    @Path("/{id}")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response release(
          @PathParam("id") String id,
          @ApiParam(value = "List of file paths to release. If any path does not belong to the "
                + "stage request corresponding to the id, this request will fail.", required = true)
                String requestPayload) {

        JSONArray paths;
        List<String> targetPaths;

        try {
            JSONObject reqPayload = new JSONObject(requestPayload);
            paths = reqPayload.getJSONArray("paths");

            if (paths == null) {
                throw new BadRequestException("release request contains no paths.");
            }

            int len = paths.length();
            targetPaths = new ArrayList<>();
            for (int i = 0; i < len; ++i) {
                targetPaths.add(paths.getString(i));
            }
        } catch (JSONException e) {
            throw new BadRequestException(
                  String.format("badly formed json object (%s): %s.", requestPayload, e));
        }

        Subject subject = getSubject();
        Restriction restriction = getRestriction();

        /*
         *  For WLCG, this is a fire-and-forget request, so it does not need to
         *  stick around once it completes.
         */
        BulkRequest request = toEphemeralBulkRequest(id, "RELEASE", targetPaths);

        /*
         *  Frontend sets the URL.  The backend service provides the UUID.
         */
        request.setUrlPrefix(this.request.getRequestURL().toString());

        BulkRequestMessage message = new BulkRequestMessage(request, restriction);
        message.setSubject(subject);
        service.send(message);

        /*
         *  WLCG says response should always be OK to this request.
         */
        return Response.ok().build();
    }

    private static BulkRequest toEphemeralBulkRequest(String id, String activity,
          List<String> targetPaths) {
        BulkRequest request = new BulkRequest();
        request.setPrestore(false);
        request.setExpandDirectories(Depth.NONE);
        request.setCancelOnFailure(true);
        request.setClearOnFailure(true);
        request.setClearOnSuccess(true);
        request.setActivity(activity);
        request.setTarget(targetPaths);
        Map<String, String> arguments = new HashMap<>();
        arguments.put("id", id);
        request.setArguments(arguments);
        return request;
    }
}
