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

import static org.dcache.restful.resources.bulk.BulkResources.getRestriction;
import static org.dcache.restful.resources.bulk.BulkResources.getSubject;

import com.google.common.base.Strings;
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
import java.util.Optional;
import javax.inject.Inject;
import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.dcache.auth.attributes.Restriction;
import org.dcache.restful.providers.tape.StageRequestInfo;
import org.dcache.restful.util.bulk.BulkServiceCommunicator;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkRequest.Depth;
import org.dcache.services.bulk.BulkRequestCancelMessage;
import org.dcache.services.bulk.BulkRequestClearMessage;
import org.dcache.services.bulk.BulkRequestInfo;
import org.dcache.services.bulk.BulkRequestMessage;
import org.dcache.services.bulk.BulkRequestStatusMessage;
import org.dcache.services.bulk.BulkRequestTargetInfo;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.stereotype.Component;

/**
 * <p>RESTful API to stage (Bulk).</p>
 *
 * @version v1.0
 */
@Component
@Api(value = "tape", authorizations = {@Authorization("basicAuth")})
@Path("tape/stage")
public final class StageResources {
    private static final String STAGE = "STAGE";

    @Context
    private HttpServletRequest request;

    @Inject
    private BulkServiceCommunicator service;

    private String[] supportedSitenames;

    /**
     * Get status information for an individual request.
     * <p>
     * NOTE: users logged in with the admin role can obtain info on any request.
     *
     * @param id of the request.
     * @return Object which describes the status of the request.
     */
    @GET
    @ApiOperation("Get the status information for an individual stage request.")
    @ApiResponses({
          @ApiResponse(code = 400, message = "Bad request"),
          @ApiResponse(code = 401, message = "Unauthorized"),
          @ApiResponse(code = 403, message = "Forbidden"),
          @ApiResponse(code = 404, message = "Not found"),
          @ApiResponse(code = 500, message = "Internal Server Error")
    })
    @Path("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public StageRequestInfo getStageInfo(@ApiParam("The unique id of the request.")
    @PathParam("id") String id) {
        Subject subject = getSubject();
        Restriction restriction = getRestriction();

        BulkRequestInfo lastInfo = null;
        List<BulkRequestTargetInfo> targetInfos = new ArrayList<>();
        long offset = 0;

        /*
         *  Page in the full request info.
         */
        BulkRequestStatusMessage message = new BulkRequestStatusMessage(id, STAGE, restriction);
        message.setSubject(subject);
        while (offset != -1) {
            message.setInfo(null);
            message.clearReply();
            message.setOffset(offset);
            message = service.send(message);
            lastInfo = message.getInfo();
            targetInfos.addAll(lastInfo.getTargets());
            offset = lastInfo.getNextId();
        }

        lastInfo.setTargets(targetInfos);

        return new StageRequestInfo(lastInfo);
    }

    /**
     * Cancel files belonging to a bulk STAGE request.
     * <p>
     * NOTE:  users logged in with the admin role will be submitting the request as ROOT (0:0).
     *
     * @return response
     */
    @POST
    @ApiOperation(value = "Cancel a STAGE request.")
    @ApiResponses({
          @ApiResponse(code = 201, message = "Created"),
          @ApiResponse(code = 400, message = "Bad request"),
          @ApiResponse(code = 401, message = "Unauthorized"),
          @ApiResponse(code = 403, message = "Forbidden"),
          @ApiResponse(code = 429, message = "Too many requests"),
          @ApiResponse(code = 500, message = "Internal Server Error")
    })
    @Path("/{id}/cancel")
    @Consumes({MediaType.APPLICATION_JSON})
    public Response cancel(
          @PathParam("id") String id,
          @ApiParam(value = "List of file paths belonging to this request to cancel. If any path "
                + "does not belong to that stage request, this request will fail.", required = true)
                String requestPayload) {

        JSONObject reqPayload = new JSONObject(requestPayload);
        JSONArray paths = reqPayload.getJSONArray("paths");
        if (paths == null) {
            throw new BadRequestException("cancellation request contains no paths.");
        }

        List<String> targetPaths = new ArrayList<>();
        int len = paths.length();
        for (int i = 0; i < len; ++i) {
            targetPaths.add(paths.getString(i));
        }

        Subject subject = getSubject();
        Restriction restriction = getRestriction();
        BulkRequestCancelMessage message = new BulkRequestCancelMessage(id, STAGE, restriction);
        message.setSubject(subject);
        message.setTargetPaths(targetPaths);

        try {
            service.send(message);
        } catch (BadRequestException e) {
            return Response.status(Status.BAD_REQUEST)
                  .header("detail", e.getMessage())
                  .header("title", "File missing from stage request")
                  .build();
        }

        return Response.ok().build();
    }

    /**
     * Submit a bulk STAGE request.
     * <p>
     * NOTE:  users logged in with the admin role will be submitting the request as ROOT (0:0).
     *
     * @return response which includes a location HTTP response header with a value that is the *
     * absolute URL for the resource associated with this bulk request.
     */
    @POST
    @ApiOperation(value = "Submit a STAGE request.")
    @ApiResponses({
          @ApiResponse(code = 201, message = "Created"),
          @ApiResponse(code = 400, message = "Bad request"),
          @ApiResponse(code = 401, message = "Unauthorized"),
          @ApiResponse(code = 403, message = "Forbidden"),
          @ApiResponse(code = 429, message = "Too many requests"),
          @ApiResponse(code = 500, message = "Internal Server Error")
    })
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response submit(
          @ApiParam(value = "Description of the request, which consists of a list of file objects "
                + "containing path, optional diskLifetime, and targetedMetadata. The latter is keyed "
                + "to the sitename-from-well-known, and contains a map/object with site "
                + "and implementation-specific attributes.", required = true)
                String requestPayload) {
        Subject subject = getSubject();
        Restriction restriction = getRestriction();

        BulkRequest request = toBulkRequest(requestPayload);

        /*
         *  Frontend sets the URL.  The backend service provides the UUID.
         */
        request.setUrlPrefix(this.request.getRequestURL().toString());

        BulkRequestMessage message = new BulkRequestMessage(request, restriction);
        message.setSubject(subject);
        message = service.send(message);

        String requestUrl = message.getRequestUrl();
        String id = requestUrl.substring(requestUrl.lastIndexOf("/") + 1);
        Map idObject = new HashMap();
        idObject.put("requestId", id);

        return Response.status(Status.CREATED)
              .header("location", message.getRequestUrl())
              .type(MediaType.APPLICATION_JSON)
              .entity(idObject)
              .build();
    }

    /**
     * The request is first cancelled.  If the request is already in a terminal state, this is a
     * no-op.
     * <p>
     * The request is cleared.  No further activity will take place for this request.  The server
     * will respond to subsequent GET requests targeting this resource with a 404 (Not Found) status
     * code.
     * <p>
     * NOTE: users logged in with the admin role can clear any request.
     *
     * @param id of the request.
     * @return response
     */
    @DELETE
    @ApiOperation("Clear all resources pertaining to the given stage request id.")
    @ApiResponses({
          @ApiResponse(code = 204, message = "No content"),
          @ApiResponse(code = 400, message = "Bad request"),
          @ApiResponse(code = 401, message = "Unauthorized"),
          @ApiResponse(code = 403, message = "Forbidden"),
          @ApiResponse(code = 500, message = "Internal Server Error")
    })
    @Path("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response clearRequest(@ApiParam("The unique id of the request.")
    @PathParam("id") String id) {
        Subject subject = getSubject();
        Restriction restriction = getRestriction();
        BulkRequestClearMessage message = new BulkRequestClearMessage(id, STAGE, restriction);
        message.setSubject(subject);
        message.setCancelIfRunning(true);
        service.send(message);
        return Response.ok().build();
    }

    private BulkRequest toBulkRequest(String requestPayload) {
        if (Strings.emptyToNull(requestPayload) == null) {
            throw new BadRequestException("empty request payload.");
        }

        BulkRequest request = new BulkRequest();
        request.setPrestore(true);
        request.setExpandDirectories(Depth.NONE);
        request.setCancelOnFailure(false);
        request.setClearOnFailure(false);
        request.setClearOnSuccess(false);
        request.setActivity("STAGE");

        try {
            JSONObject reqPayload = new JSONObject(requestPayload);

            JSONArray fileset = reqPayload.getJSONArray("files");
            if (fileset == null || fileset.length() == 0) {
                throw new BadRequestException("request contains no files.");
            }
            reqPayload.remove("files");

            if (reqPayload.length() != 0) {
                throw new BadRequestException(
                      "unrecognized payload element(s): " + reqPayload.names());
            }

            List<String> paths = new ArrayList<>();
            JSONObject jsonLifetimes = new JSONObject();
            JSONObject jsonMetadata = new JSONObject();

            int len = fileset.length();
            for (int i = 0; i < len; ++i) {
                JSONObject file = fileset.getJSONObject(i);
                if (!file.has("path")) {
                    throw new BadRequestException("file object " + i + " has no path.");
                }
                String path = file.getString("path");
                paths.add(path);
                if (file.has("diskLifetime")) {
                    jsonLifetimes.put(path, file.getString("diskLifetime"));
                }
                if (file.has("targetedMetadata")) {
                    getTargetedMetadataForPath(file).ifPresent(mdata ->
                          jsonMetadata.put(path, mdata.toString()));
                }
            }

            request.setTarget(paths);
            Map<String, String> arguments = new HashMap<>();
            arguments.put("diskLifetime", jsonLifetimes.toString());
            arguments.put("targetedMetadata", jsonMetadata.toString());
            request.setArguments(arguments);
        } catch (JSONException e) {
            throw new BadRequestException(
                  String.format("badly formed json object (%s): %s.", requestPayload, e));
        }

        return request;
    }

    @Required
    public void setSupportedSitenames(String supportedSitenames) {
        if (Strings.emptyToNull(supportedSitenames) != null) {
            this.supportedSitenames = supportedSitenames.split("[,]");
        } else {
            this.supportedSitenames = new String[0];
        }
    }

    private Optional<JSONObject> getTargetedMetadataForPath(JSONObject file) {
        final JSONObject targetedMetadata = file.getJSONObject("targetedMetadata");
        final JSONObject metaDataForPath = new JSONObject();

        for (String sitename : supportedSitenames) {
            if (targetedMetadata.has(sitename)) {
                metaDataForPath.put(sitename, targetedMetadata.getJSONObject(sitename));
            }
        }

        if (metaDataForPath.length() == 0) {
            return Optional.empty();
        }

        return Optional.of(metaDataForPath);
    }
}
