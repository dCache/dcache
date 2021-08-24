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
package org.dcache.restful.resources.quota;

import com.google.common.base.Strings;
import diskCacheV111.util.CacheException;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Exceptions;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.dcache.auth.Subjects;
import org.dcache.cells.CellStub;
import org.dcache.quota.data.QuotaInfo;
import org.dcache.quota.data.QuotaRequest;
import org.dcache.quota.data.QuotaType;
import org.dcache.restful.util.RequestUser;
import org.dcache.vehicles.quota.PnfsManagerGetQuotaMessage;
import org.dcache.vehicles.quota.PnfsManagerQuotaMessage;
import org.dcache.vehicles.quota.PnfsManagerRemoveQuotaMessage;
import org.dcache.vehicles.quota.PnfsManagerSetQuotaMessage;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import static diskCacheV111.util.CacheException.*;
import static org.dcache.restful.util.RequestUser.checkAuthenticated;

/**
 * <p>RESTful API to the PnfsManager user/group quota management.</p>
 *
 * @version v1.0
 */
@Component
@Api(value = "quota", authorizations = {@Authorization("basicAuth")})
@Path("/quota")
public final class QuotaResources {
    private static final Logger LOGGER = LoggerFactory.getLogger(QuotaResources.class);

    @Inject
    @Named("pnfs-stub")
    private CellStub pnfsmanager;

    @GET
    @ApiOperation("Get information about all user quotas known to the system."
                    + " Results sorted lexicographically by user id.")
    @ApiResponses({
        @ApiResponse(code = 401, message = "Unauthorized"),
        @ApiResponse(code = 500, message = "Internal Server Error"),
        @ApiResponse(code = 503, message = "DCache not configured for quota management.")
    })
    @Path("/user")
    @Produces(MediaType.APPLICATION_JSON)
    public List<QuotaInfo> getUserQuotas(@ApiParam(value = "Return user quota associated with "
                                                                + "calling user only.")
                                         @DefaultValue("false")
                                         @QueryParam("user") boolean user) {
        checkAuthenticated();

        PnfsManagerGetQuotaMessage message;

        if (user) {
            message = new PnfsManagerGetQuotaMessage((int)Subjects.getUid(RequestUser.getSubject()),
                                                    QuotaType.USER);
        } else {
            message = new PnfsManagerGetQuotaMessage(QuotaType.USER);
        }

        return getQuotas(message);
    }

    @GET
    @ApiOperation("Get information about all group quotas known to the system."
        + " Results sorted lexicographically by group id.")
    @ApiResponses({
        @ApiResponse(code = 401, message = "Unauthorized"),
        @ApiResponse(code = 500, message = "Internal Server Error"),
        @ApiResponse(code = 503, message = "DCache not configured for quota management.")
    })
    @Path("/group")
    @Produces(MediaType.APPLICATION_JSON)
    public List<QuotaInfo> getGroupQuotas(@ApiParam(value = "Return group quota associated with "
                                                                + "calling user only.")
                                          @DefaultValue("false")
                                          @QueryParam("user") boolean user) {
        checkAuthenticated();

        PnfsManagerGetQuotaMessage message;

        /*
         * REVISIT --- we may eventually want another option to return all the user's gids.
         */
        if (user) {
            message = new PnfsManagerGetQuotaMessage((int)Subjects.getPrimaryGid(RequestUser.getSubject()),
                                                     QuotaType.GROUP);
        } else {
            message = new PnfsManagerGetQuotaMessage(QuotaType.GROUP);
        }

        return getQuotas(message);
    }

    @GET
    @ApiOperation("Get information about quota for given user. User must be authenticated.")
    @ApiResponses({
        @ApiResponse(code = 401, message = "Unauthorized"),
        @ApiResponse(code = 500, message = "Internal Server Error"),
        @ApiResponse(code = 503, message = "DCache not configured for quota management.")
    })
    @Path("/user/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<QuotaInfo> getUserQuota(@ApiParam(value = "The user id to which the quota corresponds.",
                                                    required = true)
                                        @PathParam("id") int id) {
        checkAuthenticated();

        return getQuotas(new PnfsManagerGetQuotaMessage(id, QuotaType.USER));
    }

    @GET
    @ApiOperation("Get information about quota for given group. User must be authenticated.")
    @ApiResponses({
        @ApiResponse(code = 401, message = "Unauthorized"),
        @ApiResponse(code = 500, message = "Internal Server Error"),
        @ApiResponse(code = 503, message = "DCache not configured for quota management.")
    })
    @Path("/group/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<QuotaInfo> getGroupQuota(@ApiParam(value = "The group id to which the quota corresponds.",
                                                    required = true)
                                         @PathParam("id") int id) {
        checkAuthenticated();

        return getQuotas(new PnfsManagerGetQuotaMessage(id, QuotaType.GROUP));
    }

    @POST
    @ApiOperation(value="Add a new quota for the given user. Requires admin privileges.")
    @Path("/user/{id}")
    @ApiResponses({
        @ApiResponse(code = 401, message = "Unauthorized"),
        @ApiResponse(code = 403, message = "Forbidden"),
        @ApiResponse(code = 503, message = "DCache not configured for quota management."),
        @ApiResponse(code = 509, message = "Conflict (quota exists)")
    })
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response createUserQuota(@ApiParam(value="The user id for which the quota is created.",
                                                required=true)
                                    @PathParam("id") int id,
                                    @ApiParam(value = "A JSON object that has at least "
                                                + "one of 'custodialLimit', 'outputLimit' or "
                                                + "'replicaLimit' defined. The object can "
                                                + "be null or empty, in which case the quota "
                                                + "will be created for the id with undefined "
                                                + "limit values. The value can be a number "
                                                + "interpreted as bytes or one followed by a byte "
                                                + "unit suffix using either SI or IEEE 1541, "
                                                + "such as KB, KiB, MB, MiB, etc. 'none' means no limit.")
                                    String requestPayload) {
        if (!RequestUser.isAdmin()) {
            throw new ForbiddenException("Requires admin privileges.");
        }

        return sendQuotaRequest(new PnfsManagerSetQuotaMessage(id,
                                                               QuotaType.USER,
                                                               toQuotaRequest(requestPayload),
                                                        true));
    }

    @POST
    @ApiOperation(value="Add a new quota for the given group. Requires admin privileges.")
    @Path("/group/{id}")
    @ApiResponses({
        @ApiResponse(code = 401, message = "Unauthorized"),
        @ApiResponse(code = 403, message = "Forbidden"),
        @ApiResponse(code = 503, message = "DCache not configured for quota management."),
        @ApiResponse(code = 509, message = "Conflict (quota exists)")
    })
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response createGroupQuota(@ApiParam(value="The group id for which the quota is created.",
                                                    required=true)
                                     @PathParam("id") int id,
                                     @ApiParam(value = "A JSON object that has at least "
                                                + "one of 'custodialLimit', 'outputLimit' or "
                                                + "'replicaLimit' defined. The object can "
                                                + "be null or empty, in which case the quota "
                                                + "will be created for the id with undefined "
                                                + "limit values. The value can be a number "
                                                + "interpreted as bytes or one followed by a byte "
                                                + "unit suffix using either SI or IEEE 1541, "
                                                + "such as KB, KiB, MB, MiB, etc. 'none' means no limit.")
                                     String requestPayload) {
        if (!RequestUser.isAdmin()) {
            throw new ForbiddenException("Requires admin privileges.");
        }

        return sendQuotaRequest(new PnfsManagerSetQuotaMessage(id,
                                                               QuotaType.GROUP,
                                                               toQuotaRequest(requestPayload),
                                                        true));
    }

    @PATCH
    @ApiOperation(value="Modify the existing quota for the given user. Requires admin privileges.")
    @Path("/user/{id}")
    @ApiResponses({
        @ApiResponse(code = 401, message = "Unauthorized"),
        @ApiResponse(code = 403, message = "Forbidden"),
        @ApiResponse(code = 404, message = "Not Found"),
        @ApiResponse(code = 503, message = "DCache not configured for quota management.")
    })
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response modifyUserQuota(@ApiParam(value="The user id for which the quota is to be modified.",
                                                    required=true)
                                      @PathParam("id") int id,
                                      @ApiParam(value = "A JSON object that has at least "
                                                 + "one of 'custodialLimit', 'outputLimit' or "
                                                 + "'replicaLimit' defined. The object cannot "
                                                 + "be null or empty. The value can be a number "
                                                 + "interpreted as bytes or one followed by a byte "
                                                 + "unit suffix using either SI or IEEE 1541, "
                                                 + "such as KB, KiB, MB, MiB, etc. 'none' means no limit.")
                                      String requestPayload) {
        if (!RequestUser.isAdmin()) {
            throw new ForbiddenException("Requires admin privileges.");
        }

        if (Strings.emptyToNull(requestPayload) == null) {
            throw new BadRequestException("At least one limit must be defined.");
        }

        return sendQuotaRequest(new PnfsManagerSetQuotaMessage(id,
                                                               QuotaType.USER,
                                                               toQuotaRequest(requestPayload),
                                                        false));
    }

    @PATCH
    @ApiOperation(value="Modify the existing quota for the given group. Requires admin privileges.")
    @Path("/group/{id}")
    @ApiResponses({
        @ApiResponse(code = 401, message = "Unauthorized"),
        @ApiResponse(code = 403, message = "Forbidden"),
        @ApiResponse(code = 404, message = "Not Found"),
        @ApiResponse(code = 503, message = "DCache not configured for quota management.")
    })
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response modifyGroupQuota(@ApiParam(value="The group id for which the quota is to be modified.",
                                                    required=true)
                                     @PathParam("id") int id,
                                     @ApiParam(value = "A JSON object that has at least "
                                                + "one of 'custodialLimit', 'outputLimit' or "
                                                + "'replicaLimit' defined. The object cannot "
                                                + "be null or empty. The value can be a number "
                                                + "interpreted as bytes or one followed by a byte "
                                                + "unit suffix using either SI or IEEE 1541, "
                                                + "such as KB, KiB, MB, MiB, etc. 'none' means no limit.")
                                     String requestPayload) {
        if (!RequestUser.isAdmin()) {
            throw new ForbiddenException("Requires admin privileges.");
        }

        if (Strings.emptyToNull(requestPayload) == null) {
            throw new BadRequestException("At least one limit must be defined.");
        }

        return sendQuotaRequest(new PnfsManagerSetQuotaMessage(id,
                                                               QuotaType.GROUP,
                                                               toQuotaRequest(requestPayload),
                                                        false));
    }

    @DELETE
    @ApiOperation(value="Remove the existing quota for the given user. Requires admin privileges.")
    @Path("/user/{id}")
    @ApiResponses({
        @ApiResponse(code = 401, message = "Unauthorized"),
        @ApiResponse(code = 404, message = "Not Found"),
        @ApiResponse(code = 503, message = "DCache not configured for quota management.")
    })
    public Response removeUserQuota(@ApiParam(value="The user id for which the quota is to be removed.",
                                                        required=true)
                                    @PathParam("id") int id) {
        if (!RequestUser.isAdmin()) {
            throw new ForbiddenException("Requires admin privileges.");
        }

        return sendQuotaRequest(new PnfsManagerRemoveQuotaMessage(id, QuotaType.USER));
    }

    @DELETE
    @ApiOperation(value="Remove the existing quota for the given group. Requires admin privileges.")
    @Path("/group/{id}")
    @ApiResponses({
        @ApiResponse(code = 401, message = "Unauthorized"),
        @ApiResponse(code = 404, message = "Not Found"),
        @ApiResponse(code = 503, message = "DCache not configured for quota management.")
    })
    public Response removeGroupQuota(@ApiParam(value="The group id for which the quota is to be removed..",
                                                        required=true)
                                     @PathParam("id") int id) {
        if (!RequestUser.isAdmin()) {
            throw new ForbiddenException("Requires admin privileges.");
        }

        return sendQuotaRequest(new PnfsManagerRemoveQuotaMessage(id, QuotaType.GROUP));
    }

    private List<QuotaInfo> getQuotas(PnfsManagerGetQuotaMessage message) {
        try {
            message = pnfsmanager.sendAndWait(message);

            /*
             *  REVISIT.  Output is currently not used.  This will be revisted with
             *            further redesign of the QoS definitions. For now, we null
             *            out those settings (just in case).
             */
            return message.getQuotaInfos().stream().map(quotaInfo -> {
                                                            quotaInfo.setOutput(null);
                                                            quotaInfo.setOutputLimit(null);
                                                            return quotaInfo;
                                                    }).collect(Collectors.toList());
        } catch (CacheException e) {
            switch(e.getRc()) {
                case SERVICE_UNAVAILABLE:
                    throw new WebApplicationException(Response.status(503,
                        "Service unavailable").build());
                case NO_ATTRIBUTE:
                    throw new NotFoundException("No such quota exists");
                default:
                    throw new BadRequestException(e);
            }
        } catch (InterruptedException | NoRouteToCellException e) {
            LOGGER.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        }
    }

    private Response sendQuotaRequest(PnfsManagerQuotaMessage message) {
        try {
            pnfsmanager.sendAndWait(message);
        } catch (CacheException e) {
            switch (e.getRc()) {
                case SERVICE_UNAVAILABLE:
                    throw new WebApplicationException(Response.status(503,
                        "Service unavailable").build());
                case ATTRIBUTE_EXISTS:
                    throw new WebApplicationException(Response.status(509,
                            "Quota already exists").build());
                case NO_ATTRIBUTE:
                    throw new NotFoundException("No such quota exists");
                default:
                    throw new BadRequestException(e);
            }
        } catch (InterruptedException | NoRouteToCellException e) {
            LOGGER.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        }

        return Response.ok().build();
    }

    private static QuotaRequest toQuotaRequest(String requestPayload) {
        if ( Strings.emptyToNull(requestPayload) == null) {
            return null;
        }

        QuotaRequest request = new QuotaRequest();
        JSONObject jsonObject = new JSONObject(requestPayload);

        if (jsonObject.length() == 0) {
            return null;
        }

        if (jsonObject.has("custodialLimit")) {
            request.setCustodialLimit(jsonObject.getString("custodialLimit"));
        }

        if (jsonObject.has("replicaLimit")) {
            request.setReplicaLimit(jsonObject.getString("replicaLimit"));
        }

        /*
         *  REVISIT.  Output is currently not used.  This will be revisted with
         *            further redesign of the QoS definitions.
         */
//        if (jsonObject.has("outputLimit")) {
//            request.setOutputLimit(jsonObject.getString("outputLimit"));
//        }

        return request;
    }
}
