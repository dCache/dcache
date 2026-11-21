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
import jakarta.ws.rs.InternalServerErrorException;
import jakarta.ws.rs.NotAuthorizedException;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.List;
import javax.security.auth.Subject;
import org.dcache.cells.CellStub;
import org.dcache.qos.DefaultQoSPolicyJsonDeserializer;
import org.dcache.qos.QoSPolicy;
import org.dcache.restful.util.RequestUser;
import org.dcache.restful.util.Responses;
import org.dcache.vehicles.qos.PnfsManagerAddQoSPolicyMessage;
import org.dcache.vehicles.qos.PnfsManagerGetQoSPolicyMessage;
import org.dcache.vehicles.qos.PnfsManagerListQoSPoliciesMessage;
import org.dcache.vehicles.qos.PnfsManagerRmQoSPolicyMessage;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * RESTful API to manage dCache QoS Policies.
 * <p>
 * QoS Policies define how files should be stored and maintained across different storage
 * tiers (DISK, HSM). Each policy specifies a sequence of states that determine where files
 * should reside and for how long.</p>
 *
 */

@Component
@Tag(name = "qos-policy")
@SecurityRequirement(name = "basicAuth")
@Path("/qos-policy")
public class QoSPolicyResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(QoSPolicyResource.class);

    @Context
    private HttpServletRequest request;

    @Inject
    @Named("qos-engine")
    private CellStub qosEngine;

    /**
     * Get a list of all registered QoS policy names.
     *
     * @return List of policy names
     */
    @GET
    @Operation(summary = "List all the registered QoS policy names.")
    @ApiResponses({
          @ApiResponse(responseCode = "400", description = "Bad Request"),
          @ApiResponse(responseCode = "401", description = "Unauthorized"),
          @ApiResponse(responseCode = "403", description = "Forbidden"),
          @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Produces(MediaType.APPLICATION_JSON)
    public Response listPolicies() {
        PnfsManagerListQoSPoliciesMessage message = new PnfsManagerListQoSPoliciesMessage();
        message.setSubject(getCurrentSubject());
        try {
            message = qosEngine.sendAndWait(message);
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
        List<String> result = message.getPolicies();
        return Responses.buildResponse(result);
    }

    /**
     * Get detailed information about a specific QoS policy.
     *
     * @param name Policy name
     * @return Full QoS policy description
     */
    @GET
    @Path("{name}")
    @Operation(summary = "Retrieve a specific QoS policy by name.")
    @ApiResponses({
          @ApiResponse(responseCode = "400", description = "Bad Request"),
          @ApiResponse(responseCode = "401", description = "Unauthorized"),
          @ApiResponse(responseCode = "403", description = "Forbidden"),
          @ApiResponse(responseCode = "404", description = "Policy Not Found"),
          @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPolicy(
          @Parameter(description = "The name of the policy (unique to this dCache instance).")
          @PathParam("name")String name) {
        PnfsManagerGetQoSPolicyMessage message = new PnfsManagerGetQoSPolicyMessage(name);
        message.setSubject(getCurrentSubject());

        try {
            message = qosEngine.sendAndWait(message);
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
        QoSPolicy result = message.getPolicy();
        return Responses.buildResponse(result);
    }

    /**
     * Delete a QoS policy.
     *
     * <p><strong>Warning:</strong> This action is irreversible. The policy will be
     * completely removed from the system.</p>
     *
     * @param name Policy name to delete
     * @return HTTP 200 OK
     */
    @DELETE
    @Path("{name}")
    @Operation(summary = "Permanently delete a QoS policy by name.")
    @ApiResponses({
          @ApiResponse(responseCode = "400", description = "Bad Request"),
          @ApiResponse(responseCode = "401", description = "Unauthorized"),
          @ApiResponse(responseCode = "403", description = "Forbidden"),
          @ApiResponse(responseCode = "404", description = "Policy Not Found"),
          @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    public Response deletePolicy(
          @Parameter(description = "The name of the policy (unique to this dCache instance).")
          @PathParam("name")String name) {
        PnfsManagerRmQoSPolicyMessage message = new PnfsManagerRmQoSPolicyMessage(name);
        message.setSubject(getCurrentSubject());

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

    /**
     * Create a new QoS policy.
     *
     * @param requestPayload JSON policy definition
     * @return HTTP 200 OK
     */
    @POST
    @Operation(summary = "Add a QoS policy by this name; if a policy is currently "
          + "mapped to that name, an error is returned.")
    @ApiResponses({
          @ApiResponse(responseCode = "400", description = "Bad Request"),
          @ApiResponse(responseCode = "401", description = "Unauthorized"),
          @ApiResponse(responseCode = "403", description = "Forbidden"),
          @ApiResponse(responseCode = "404", description = "Not Found"),
          @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response submit(
          @Parameter(description = "Description of the QoS policy, which defines the following:\n\n"
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
        message.setSubject(getCurrentSubject());

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

    /**
     * Get the current user's subject for authorization.
     * Admin users are granted ROOT privileges, regular users get their standard subject.
     *
     * @return User's authentication subject
     */
    private static Subject getCurrentSubject() {
        return RequestUser.isAdmin() ? ROOT : RequestUser.getSubject();
    }
}
