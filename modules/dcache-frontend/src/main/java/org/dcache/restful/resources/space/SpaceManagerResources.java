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
package org.dcache.restful.resources.space;

import diskCacheV111.services.space.LinkGroup;
import diskCacheV111.services.space.Space;
import diskCacheV111.services.space.SpaceState;
import diskCacheV111.services.space.message.GetLinkGroupsMessage;
import diskCacheV111.services.space.message.GetSpaceTokensMessage;
import diskCacheV111.services.space.message.Reserve;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.VOInfo;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Exceptions;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.InternalServerErrorException;
import jakarta.ws.rs.NotAuthorizedException;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.dcache.cells.CellStub;
import org.dcache.restful.providers.space.LinkGroupInfo;
import org.dcache.restful.providers.space.SpaceToken;
import org.dcache.restful.util.RequestUser;
import org.dcache.restful.util.Responses;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * <p>RESTful API to the SpaceManager.</p>
 *
 * @version v1.0
 */
@Component
@Tag(name = "spacemanager")
@SecurityRequirement(name = "basicAuth")
@Path("/space")
public final class SpaceManagerResources {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpaceManagerResources.class);
    private final static String FORBIDDEN = "Spacemanager info only accessible to "
          + "admin users.";
    @Inject
    @Named("spacemanager-stub")
    private CellStub spacemanagerStub;

    private boolean spaceReservationEnabled;

    @GET
    @Operation(summary = "Get information about link groups."
          + " Results sorted lexicographically by link group name.")
    @ApiResponses({
          @ApiResponse(responseCode = "400", description = "Bad Request Error"),
          @ApiResponse(responseCode = "404",
                description = "DCache not configured for space management."),
          @ApiResponse(responseCode = "500", description = "Internal Server Error")
    })
    @Path("/linkgroups")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getLinkGroups(@Parameter(description = "The name of the link group.")
    @QueryParam("name") String name,
          @Parameter(description = "The id of the link group.")
          @QueryParam("id") Long id,
          @Parameter(description = "Whether the link group allows online access latency.")
          @QueryParam("onlineAllowed") Boolean onlineAllowed,
          @Parameter(description = "Whether the link group allows nearline access latency.")
          @QueryParam("nearlineAllowed") Boolean nearlineAllowed,
          @Parameter(description = "Whether the link group allows replica retention policy.")
          @QueryParam("replicaAllowed") Boolean replicaAllowed,
          @Parameter(description = "Whether the link group allows output retention policy.")
          @QueryParam("outputAllowed") Boolean outputAllowed,
          @Parameter(description = "Whether the link group allows custodial retention policy.")
          @QueryParam("custodialAllowed") Boolean custodialAllowed,
          @Parameter(description = "VO group associated with the link.")
          @QueryParam("voGroup") String voGroup,
          @Parameter(description = "VO role associated with the link.")
          @QueryParam("voRole") String voRole,
          @Parameter(description = "Minimum amount of space (in bytes) still available via the "
                + "link.")
          @QueryParam("minAvailableSpace") Long minAvailableSpace) {
        if (!spaceReservationEnabled) {
            throw new NotFoundException();
        }

        Predicate<LinkGroup> filter = getLinkGroupFilter(name,
              id,
              onlineAllowed,
              nearlineAllowed,
              replicaAllowed,
              outputAllowed,
              custodialAllowed,
              voGroup,
              voRole,
              minAvailableSpace);

        try {
            GetLinkGroupsMessage reply
                  = spacemanagerStub.sendAndWait(new GetLinkGroupsMessage());

            List<LinkGroupInfo> result = reply.getLinkGroups()
                  .stream()
                  .sorted(Comparator.comparing(LinkGroup::getName))
                  .filter(filter)
                  .map(LinkGroupInfo::new)
                  .collect(Collectors.toList());
            return Responses.buildResponse(result);
        } catch (CacheException | InterruptedException | NoRouteToCellException ex) {
            LOGGER.warn(Exceptions.meaningfulMessage(ex));
            throw new InternalServerErrorException(ex);
        }
    }

    @GET
    @Operation(summary = "Get information about space tokens.  "
          + "Results sorted by token id.")
    @ApiResponses({
          @ApiResponse(responseCode = "404",
                description = "DCache not configured for space management."),
          @ApiResponse(responseCode = "500", description = "Internal Server Error")
    })
    @Path("/tokens")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTokensForGroup(@Parameter(description = "The id of the space token.")
    @QueryParam("id") Long id,
          @Parameter(description = "VO group associated with the token.")
          @QueryParam("voGroup") String voGroup,
          @Parameter(description = "VO role associated with the token.")
          @QueryParam("voRole") String voRole,
          @Parameter(description = "Access Latency associated with the token.")
          @QueryParam("accessLatency") String accessLatency,
          @Parameter(description = "Retention Policy associated with the token.")
          @QueryParam("retentionPolicy") String retentionPolicy,
          @Parameter(description = "Id of link group to which token belongs.")
          @QueryParam("groupId") Long groupId,
          @Parameter(description = "State of the token.")
          @QueryParam("state") String state,
          @Parameter(description = "Minimum size (in bytes) of token.")
          @QueryParam("minSize") Long minSize,
          @Parameter(description = "Minimum amount of space (in bytes) still free for token.")
          @QueryParam("minFreeSpace") Long minFreeSpace) {
        if (!spaceReservationEnabled) {
            throw new NotFoundException();
        }

        Predicate<Space> filter = getSpaceFilter(id,
              voGroup,
              voRole,
              accessLatency,
              retentionPolicy,
              groupId,
              state,
              minSize,
              minFreeSpace);

        try {
            GetSpaceTokensMessage reply
                  = spacemanagerStub.sendAndWait(new GetSpaceTokensMessage());

            List<SpaceToken> result = reply.getSpaceTokenSet()
                  .stream()
                  .sorted(Comparator.comparing(Space::getId))
                  .filter(filter)
                  .map(SpaceToken::new)
                  .collect(Collectors.toList());
            return Responses.buildResponse(result);
        } catch (CacheException | InterruptedException | NoRouteToCellException ex) {
            LOGGER.warn(Exceptions.meaningfulMessage(ex));
            throw new InternalServerErrorException(ex);
        }
    }


    @POST
    @Operation(summary = "Create a new space reservation.")
    @ApiResponses({
          @ApiResponse(responseCode = "201", description = "Created"),
          @ApiResponse(responseCode = "400", description = "Bad Request Error"),
          @ApiResponse(responseCode = "403", description = "Forbidden"),
          @ApiResponse(responseCode = "404",
                description = "DCache not configured for space management."),
          @ApiResponse(responseCode = "500", description = "Internal Server Error")
    })
    @Path("/tokens")
    @Produces(MediaType.APPLICATION_JSON)
    public Response reserverSpace(
          @Parameter(description = "Access Latency associated with the token.",
                schema = @Schema(allowableValues = {"ONLINE", "NEARLINE"}))
          @QueryParam("accessLatency") String accessLatency,
          @Parameter(description = "Retention Policy associated with the token.",
                schema = @Schema(allowableValues = {"REPLICA", "OUTPUT", "CUSTODIAL"}))
          @QueryParam("retentionPolicy") String retentionPolicy,
          @Parameter(description = "Minimum size (in bytes) of token.")
          @QueryParam("minSize") Long minSize,
          @Parameter(description = "Reservation lifetime as ISO8601 Duration (for instance, "
                + "P2DT3H4M, for 2 days, 3 hours and 4 minutes). If not specified, the "
                + "reservation will be valid indefinitely.")
          @QueryParam("lifeTime") String lifeTime,
          @Parameter(description = "Description of the reservation.")
          @QueryParam("description") String description) {

        if (RequestUser.isAnonymous()) {
            throw new NotAuthorizedException("User cannot be anonymous.");
        }

        if (!spaceReservationEnabled) {
            throw new NotFoundException();
        }

        try {

            var subject = RequestUser.getSubject();

            Reserve newReservation = new Reserve(null,
                  minSize,
                  RetentionPolicy.valueOf(retentionPolicy.toUpperCase()),
                  AccessLatency.valueOf(accessLatency.toUpperCase()),
                  lifeTime == null ? -1L : Duration.parse(lifeTime).toMillis(),
                  description);

            newReservation.setSubject(subject);

            var response = spacemanagerStub.sendAndWait(newReservation);

            var jsonResponse = new JSONObject();
            jsonResponse.put("id", response.getSpaceToken());
            jsonResponse.put("description", response.getDescription());
            jsonResponse.put("sizeInBytes", response.getSizeInBytes());

            return Response.status(Response.Status.CREATED)
                  .type(MediaType.APPLICATION_JSON)
                  .entity(jsonResponse.toString())
                  .build();

        } catch (IllegalArgumentException ex) {
            LOGGER.warn(Exceptions.meaningfulMessage(ex));
            return Response.status(Status.BAD_REQUEST)
                  .build();
        } catch (NoRouteToCellException ex) {
            LOGGER.warn(Exceptions.meaningfulMessage(ex));
            return Response.status(Status.BAD_GATEWAY)
                  .build();
        } catch (InterruptedException ex) {
            LOGGER.warn(Exceptions.meaningfulMessage(ex));
            return Response.status(Status.GATEWAY_TIMEOUT)
                  .type(MediaType.APPLICATION_JSON)
                  .build();
        } catch (CacheException ex) {
            // TODO: distinguish between different types of CacheException and return more specific error codes.
            LOGGER.warn(Exceptions.meaningfulMessage(ex));
            return Response.status(Status.BAD_REQUEST)
                  .type(MediaType.APPLICATION_JSON)
                  .entity(ex.getMessage())
                  .build();
        }
    }

    private Predicate<LinkGroup> getLinkGroupFilter(String name,
          Long id,
          Boolean onlineAllowed,
          Boolean nearlineAllowed,
          Boolean replicaAllowed,
          Boolean outputAllowed,
          Boolean custodialAllowed,
          String voGroup,
          String voRole,
          Long minAvailableSpace) {
        Predicate<LinkGroup> predicate = group -> true;

        if (name != null) {
            predicate = predicate.and(group -> group.getName().equals(name));
        }

        if (id != null) {
            predicate = predicate.and(group -> group.getId() == id);
        }

        if (onlineAllowed != null) {
            predicate = predicate.and(group -> group.isOnlineAllowed() == onlineAllowed);
        }

        if (nearlineAllowed != null) {
            predicate = predicate.and(group -> group.isNearlineAllowed() == nearlineAllowed);
        }

        if (replicaAllowed != null) {
            predicate = predicate.and(group -> group.isReplicaAllowed() == replicaAllowed);
        }

        if (outputAllowed != null) {
            predicate = predicate.and(group -> group.isOutputAllowed() == outputAllowed);
        }

        if (custodialAllowed != null) {
            predicate = predicate.and(group -> group.isCustodialAllowed() == custodialAllowed);
        }

        if (voGroup != null) {
            predicate = predicate.and(group -> Stream.of(group.getVOs())
                  .map(VOInfo::getVoGroup)
                  .collect(Collectors.toSet())
                  .contains(voGroup));
        }

        if (voRole != null) {
            predicate = predicate.and(group -> Stream.of(group.getVOs())
                  .map(VOInfo::getVoRole)
                  .collect(Collectors.toSet())
                  .contains(voRole));
        }

        if (minAvailableSpace != null) {
            predicate = predicate.and(group -> group.getAvailableSpace() >= minAvailableSpace);
        }

        return predicate;
    }

    private Predicate<Space> getSpaceFilter(Long id,
          String voGroup,
          String voRole,
          String accessLatency,
          String retentionPolicy,
          Long groupId,
          String state,
          Long minSize,
          Long minFreeSpace) {
        Predicate<Space> predicate = space -> true;

        if (id != null) {
            predicate = predicate.and(space -> space.getId() == id);
        }

        if (voGroup != null) {
            predicate = predicate.and(space -> voGroup.equals(space.getVoGroup()));
        }

        if (voRole != null) {
            predicate = predicate.and(space -> voRole.equals(space.getVoRole()));
        }

        if (accessLatency != null) {
            predicate = predicate.and(space -> AccessLatency.valueOf(accessLatency.toUpperCase())
                  == space.getAccessLatency());
        }

        if (retentionPolicy != null) {
            predicate = predicate.and(
                  space -> RetentionPolicy.valueOf(retentionPolicy.toUpperCase())
                        == space.getRetentionPolicy());
        }

        if (groupId != null) {
            predicate = predicate.and(space -> space.getLinkGroupId() == groupId);
        }

        if (state != null) {
            predicate = predicate.and(space -> SpaceState.valueOf(state) == space.getState());
        }

        if (minSize != null) {
            predicate = predicate.and(space -> space.getSizeInBytes() >= minSize);
        }

        if (minFreeSpace != null) {
            predicate = predicate.and(space -> space.getAllocatedSpaceInBytes()
                  - space.getUsedSizeInBytes() >= minFreeSpace);
        }

        return predicate;
    }

    public void setSpaceReservationEnabled(boolean spaceReservationEnabled) {
        this.spaceReservationEnabled = spaceReservationEnabled;
    }
}
