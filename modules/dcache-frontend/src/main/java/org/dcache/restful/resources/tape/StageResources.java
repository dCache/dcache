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

import static org.dcache.http.AuthenticationFilter.getLoginAttributes;
import static org.dcache.restful.resources.bulk.BulkResources.getRestriction;
import static org.dcache.restful.resources.bulk.BulkResources.getSubject;
import static org.dcache.restful.util.HttpServletRequests.getUserRootAwareTargetPrefix;
import static org.dcache.restful.util.JSONUtils.newBadRequestException;

import com.google.common.base.Strings;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
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
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.security.auth.Subject;
import org.dcache.auth.attributes.LoginAttributes;
import org.dcache.auth.attributes.Restriction;
import org.dcache.cells.CellStub;
import org.dcache.http.PathMapper;
import org.dcache.restful.providers.tape.StageRequestInfo;
import org.dcache.restful.util.HandlerBuilders;
import org.dcache.restful.util.Responses;
import org.dcache.restful.util.bulk.BulkServiceCommunicator;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkRequest.Depth;
import org.dcache.services.bulk.BulkRequestCancelMessage;
import org.dcache.services.bulk.BulkRequestClearMessage;
import org.dcache.services.bulk.BulkRequestInfo;
import org.dcache.services.bulk.BulkRequestMessage;
import org.dcache.services.bulk.BulkRequestStatusMessage;
import org.dcache.services.bulk.BulkRequestTargetInfo;
import org.dcache.util.TimeUtils;
import org.dcache.util.spring.Required;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

/**
 * <p>RESTful API to stage (Bulk).</p>
 *
 * @version v1.0
 */
@Component
@Tag(name = "tape")
@SecurityRequirement(name = "basicAuth")
@Path("tape/stage")
public final class StageResources {

    private static final String STAGE = "STAGE";

    @Context
    private HttpServletRequest request;

    @Inject
    private BulkServiceCommunicator service;

    @Inject
    private PathMapper pathMapper;

    @Inject
    @Named("pnfs-stub")
    private CellStub pnfsmanager;

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
    @Operation(summary = "Get the status information for an individual stage request.")
    @ApiResponses({
          @ApiResponse(responseCode = "400", description = "Bad request"),
          @ApiResponse(responseCode = "401", description = "Unauthorized"),
          @ApiResponse(responseCode = "403", description = "Forbidden"),
          @ApiResponse(responseCode = "404", description = "Not found"),
          @ApiResponse(responseCode = "500", description = "Internal Server Error")
    })
    @Path("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getStageInfo(@Parameter(description = "The unique id of the request.")
    @PathParam("id") String id) {
        Subject subject = getSubject();
        Restriction restriction = getRestriction();
        FsPath userRoot = LoginAttributes.getUserRoot(getLoginAttributes(request));
        FsPath rootPath = pathMapper.effectiveRoot(userRoot, ForbiddenException::new);

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

        StageRequestInfo result = new StageRequestInfo(lastInfo);
        return Responses.buildResponse(result);
    }

    /**
     * Cancel files belonging to a bulk STAGE request.
     * <p>
     * NOTE:  users logged in with the admin role will be submitting the request as ROOT (0:0).
     *
     * @return response
     */
    @POST
    @Operation(summary = "Cancel a STAGE request.")
    @ApiResponses({
          @ApiResponse(responseCode = "201", description = "Created"),
          @ApiResponse(responseCode = "400", description = "Bad request"),
          @ApiResponse(responseCode = "401", description = "Unauthorized"),
          @ApiResponse(responseCode = "403", description = "Forbidden"),
          @ApiResponse(responseCode = "404", description = "Not Found"),
          @ApiResponse(responseCode = "413", description = "Content Too Large"),
          @ApiResponse(responseCode = "429", description = "Too many requests"),
          @ApiResponse(responseCode = "500", description = "Internal Server Error")
    })
    @Path("/{id}/cancel")
    @Consumes({MediaType.APPLICATION_JSON})
    public Response cancel(
          @PathParam("id") String id,
          @Parameter(description = "List of file paths belonging to this request to cancel. If "
                + "any path "
                + "does not belong to that stage request, this request will fail.", required = true)
                String requestPayload) {

	FsPath userRoot = LoginAttributes.getUserRoot(getLoginAttributes(request));
        FsPath rootPath = pathMapper.effectiveRoot(userRoot, ForbiddenException::new);
	JSONObject reqPayload;
        JSONArray paths;
        try {
            reqPayload = new JSONObject(requestPayload);
            paths = reqPayload.getJSONArray("paths");
            if (paths == null) {
                throw new BadRequestException("cancellation request contains no paths.");
            }
        } catch (JSONException e) {
            throw newBadRequestException(requestPayload, e);
        }

        List<String> targetPaths = new ArrayList<>();
        int len = paths.length();
        for (int i = 0; i < len; ++i) {
	    String path = paths.getString(i);
            targetPaths.add(path);
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
    @Operation(summary = "Submit a STAGE request.")
    @ApiResponses({
          @ApiResponse(responseCode = "201", description = "Created"),
          @ApiResponse(responseCode = "400", description = "Bad request"),
          @ApiResponse(responseCode = "401", description = "Unauthorized"),
          @ApiResponse(responseCode = "403", description = "Forbidden"),
          @ApiResponse(responseCode = "413", description = "Content Too Large"),
          @ApiResponse(responseCode = "429", description = "Too many requests"),
          @ApiResponse(responseCode = "500", description = "Internal Server Error")
    })
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response submit(
          @Parameter(description = "Request structure:\n\n"
                + "**files** - Array of File objects.  Required.\n"
                + "File object structure:\n"
                + "**path**:  - String.  Path of the file.  Duplicates will be sanitized.  Required.\n"
                + "**diskLifetime**: - String of ISO 8601 format.  Duration after which the replica is considered no longer needed."
                + "  Optional (defaults to system default for pin lifetime).\n"
                + "**targetdMetadata** - targeted metadata object. Keyed to the sitename-from-well-known, "
                + "and contains a map/object with site and implementation-specific attributes. "
                + "Currently ignored by dCache.\n"
                + "See further https://docs.google.com/document/d/1Zx_H5dRkQRfju3xIYZ2WgjKoOvmLtsafP2pKGpHqcfY/edit#heading=h.93yzfvog73oc.", required = true)
                String requestPayload) {
        Subject subject = getSubject();
        Restriction restriction = getRestriction();

        FsPath userRoot = LoginAttributes.getUserRoot(getLoginAttributes(request));
        FsPath rootPath = pathMapper.effectiveRoot(userRoot, ForbiddenException::new);

        BulkRequest request = toBulkRequest(requestPayload, rootPath);

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
    @Operation(summary = "Clear all resources pertaining to the given stage request id.")
    @ApiResponses({
          @ApiResponse(responseCode = "204", description = "No content"),
          @ApiResponse(responseCode = "400", description = "Bad request"),
          @ApiResponse(responseCode = "401", description = "Unauthorized"),
          @ApiResponse(responseCode = "403", description = "Forbidden"),
          @ApiResponse(responseCode = "404", description = "Not Found"),
          @ApiResponse(responseCode = "500", description = "Internal Server Error")
    })
    @Path("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response clearRequest(@Parameter(description = "The unique id of the request.")
    @PathParam("id") String id) {
        Subject subject = getSubject();
        Restriction restriction = getRestriction();
        BulkRequestClearMessage message = new BulkRequestClearMessage(id, STAGE, restriction);
        message.setSubject(subject);
        message.setCancelIfRunning(true);
        service.send(message);
        return Response.ok().build();
    }

    private BulkRequest toBulkRequest(String requestPayload, FsPath rootPath) {
        if (Strings.emptyToNull(requestPayload) == null) {
            throw new BadRequestException("empty request payload.");
        }

        BulkRequest request = new BulkRequest();
        request.setExpandDirectories(Depth.NONE);
        request.setCancelOnFailure(false);
        request.setClearOnFailure(false);
        request.setClearOnSuccess(false);
        request.setActivity("STAGE");

        PnfsHandler handler = HandlerBuilders.unrestrictedPnfsHandler(pnfsmanager);
        request.setTargetPrefix(getUserRootAwareTargetPrefix(this.request, rootPath.toString(), handler));

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
                    jsonLifetimes.put(path,
                          TimeUtils.validateDuration(file.getString("diskLifetime")));
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
        } catch (JSONException | IllegalArgumentException e) {
            throw newBadRequestException(requestPayload, e);
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
