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
package org.dcache.restful.resources.qos;

import static diskCacheV111.util.CacheException.RESOURCE;
import static org.dcache.auth.Subjects.ROOT;
import static org.dcache.restful.providers.SuccessfulResponse.successfulResponse;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import dmg.util.Exceptions;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Named;
import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.dcache.cells.CellStub;
import org.dcache.qos.DefaultQoSPolicyJsonDeserializer;
import org.dcache.qos.QoSPolicy;
import org.dcache.restful.util.RequestUser;
import org.dcache.vehicles.qos.PnfsManagerAddQoSPolicyMessage;
import org.dcache.vehicles.qos.PnfsManagerGetQoSPolicyMessage;
import org.dcache.vehicles.qos.PnfsManagerListQoSPoliciesMessage;
import org.dcache.vehicles.qos.PnfsManagerRmQoSPolicyMessage;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * RestFul API to manage dCache QoS Policy descriptions.
 */
@Api(value = "qos-policy", authorizations = {@Authorization("basicAuth")})
@Component
@Path("/qos-policy")
public class QoSPolicyResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(QoSPolicyResource.class);

    @Context
    private HttpServletRequest request;

    @Inject
    @Named("qos-engine")
    private CellStub qosEngine;

    @GET
    @ApiOperation(value = "List all the registered QoSPolicy names.")
    @ApiResponses({
          @ApiResponse(code = 400, message = "Bad Request"),
          @ApiResponse(code = 401, message = "Unauthorized"),
          @ApiResponse(code = 403, message = "Forbidden"),
          @ApiResponse(code = 500, message = "Internal Server Error"),
    })
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> listPolicies() {
        PnfsManagerListQoSPoliciesMessage message = new PnfsManagerListQoSPoliciesMessage();
        message.setSubject(getSubject());
        try {
            message = qosEngine.sendAndWait(message);
            return message.getPolicies();
        } catch (PermissionDeniedCacheException e) {
            if (RequestUser.isAnonymous()) {
                throw new NotAuthorizedException(e);
            } else {
                throw new ForbiddenException(e);
            }
        } catch (JSONException | IllegalArgumentException | CacheException e) {
            throw new BadRequestException(e);
        } catch (Exception e) {
            LOGGER.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        }
    }

    @GET
    @ApiOperation(value = "Retrieve the QoSPolicy by this name.")
    @ApiResponses({
          @ApiResponse(code = 400, message = "Bad Request"),
          @ApiResponse(code = 401, message = "Unauthorized"),
          @ApiResponse(code = 403, message = "Forbidden"),
          @ApiResponse(code = 404, message = "Not Found"),
          @ApiResponse(code = 500, message = "Internal Server Error"),
    })
    @Path("{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public QoSPolicy getPolicy(
          @ApiParam("The name of the policy (unique to this dCache instance).")
          @PathParam("name")String name) {
        PnfsManagerGetQoSPolicyMessage message = new PnfsManagerGetQoSPolicyMessage(name);
        message.setSubject(getSubject());

        try {
            message = qosEngine.sendAndWait(message);
            return message.getPolicy();
        } catch (PermissionDeniedCacheException e) {
            if (RequestUser.isAnonymous()) {
                throw new NotAuthorizedException(e);
            } else {
                throw new ForbiddenException(e);
            }
        } catch (CacheException e) {
            if (e.getRc() == RESOURCE) {
                throw new NotFoundException("qos policy " + name);
            }
            throw new BadRequestException(e);
        } catch (JSONException | IllegalArgumentException e) {
            throw new BadRequestException(e);
        } catch (Exception e) {
            LOGGER.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        }
    }

    @DELETE
    @ApiOperation(value = "Delete the QoSPolicy by this name.")
    @ApiResponses({
          @ApiResponse(code = 400, message = "Bad Request"),
          @ApiResponse(code = 401, message = "Unauthorized"),
          @ApiResponse(code = 403, message = "Forbidden"),
          @ApiResponse(code = 404, message = "Not Found"),
          @ApiResponse(code = 500, message = "Internal Server Error"),
    })
    @Path("{name}")
    public Response deletePolicy(
          @ApiParam("The name of the policy (unique to this dCache instance).")
          @PathParam("name")String name) {
        PnfsManagerRmQoSPolicyMessage message = new PnfsManagerRmQoSPolicyMessage(name);
        message.setSubject(getSubject());

        try {
            qosEngine.sendAndWait(message);
        } catch (PermissionDeniedCacheException e) {
            if (RequestUser.isAnonymous()) {
                throw new NotAuthorizedException(e);
            } else {
                throw new ForbiddenException(e);
            }
        } catch (CacheException e) {
            if (e.getRc() == RESOURCE) {
                throw new NotFoundException("qos policy " + name);
            }
            throw new BadRequestException(e);
        } catch (JSONException | IllegalArgumentException e) {
            throw new BadRequestException(e);
        } catch (Exception e) {
            LOGGER.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        }
        return successfulResponse(Response.Status.OK);
    }

    @POST
    @ApiOperation(value = "Add a QoSPolicy by this name; if a policy is currently "
          + "mapped to that name, an error is returned.")
    @ApiResponses({
          @ApiResponse(code = 400, message = "Bad Request"),
          @ApiResponse(code = 401, message = "Unauthorized"),
          @ApiResponse(code = 403, message = "Forbidden"),
          @ApiResponse(code = 404, message = "Not Found"),
          @ApiResponse(code = 500, message = "Internal Server Error"),
    })
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response submit(
          @ApiParam(value = "Description of the QoS policy, which defines the following:\n\n"
                + "**name** - String identifier.  Required.\n"
                + "**states** - Ordered list (array) of states. Required\n\n"
                + "Each state consists of:\n"
                + "**duration** - How long the state should last. Optional. No duration means "
                + "the same as INF/indefinite, except that it is telling the qos"
                + "system that it need not continue to verify the file's status after the first"
                + "check.\n"
                + "**media** -  Ordered list (array) of storage element descriptions. "
                + "At least one is required.\n\n"
                + "**Each storage element description consists of: \n"
                + "**storageElement** - one of (DISK, HSM). Required.\n"
                + "**numberOfCopies** - currently supported for DISK only.\n"
                + "**type** - String. Could describe a disk type or the hsm system name, "
                + "for example. Optional, currently "
                + "unused.\n"
                + "**instance** - String URI for the system instance (HSM only).\n"
                + "**partitionKeys** - list (array) of values used to distribute copies across "
                + "pools (DISK only).  Optional.",
                required = true)
                String requestPayload) {
        QoSPolicy policy;
        try {
            policy = DefaultQoSPolicyJsonDeserializer.fromJsonString(requestPayload);
        } catch (Exception e) {
            throw new BadRequestException(e);
        }

        PnfsManagerAddQoSPolicyMessage message = new PnfsManagerAddQoSPolicyMessage(policy);
        message.setSubject(getSubject());

        try {
            qosEngine.sendAndWait(message);
        } catch (PermissionDeniedCacheException e) {
            if (RequestUser.isAnonymous()) {
                throw new NotAuthorizedException(e);
            } else {
                throw new ForbiddenException(e);
            }
        } catch (CacheException e) {
            throw new BadRequestException(e);
        } catch (JSONException | IllegalArgumentException e) {
            throw new BadRequestException(e);
        } catch (Exception e) {
            LOGGER.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        }
        return successfulResponse(Response.Status.OK);
    }

    private static Subject getSubject() {
        if (RequestUser.isAdmin()) {
            return ROOT;
        } else {
            return RequestUser.getSubject();
        }
    }
}
