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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Named;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import diskCacheV111.services.space.LinkGroup;
import diskCacheV111.services.space.Space;
import diskCacheV111.services.space.SpaceState;
import diskCacheV111.services.space.message.GetLinkGroupsMessage;
import diskCacheV111.services.space.message.GetSpaceTokensMessage;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.VOInfo;

import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.CellStub;
import org.dcache.restful.providers.space.LinkGroupInfo;
import org.dcache.restful.providers.space.SpaceToken;
import org.dcache.restful.util.HttpServletRequests;

/**
 * <p>RESTful API to the SpaceManager.</p>
 *
 * @version v1.0
 */
@Component
@Api(value = "spacemanager", authorizations = {@Authorization("basicAuth")})
@Path("/space")
public final class SpaceManagerResources {
    private final static String FORBIDDEN = "Spacemanager info only accessible to "
                                                + "admin users.";
    @Context
    private HttpServletRequest request;

    @Inject
    @Named("spacemanager-stub")
    private CellStub spacemanagerStub;

    private boolean spaceReservationEnabled;

    @GET
    @ApiOperation("Get information about link groups.  Requires admin role.")
    @ApiResponses({
        @ApiResponse(code = 400, message = "Bad Request Error"),
        @ApiResponse(code = 403, message = "Link group info only accessible to admin users."),
        @ApiResponse(code = 404, message = "DCache not configured for space management."),
        @ApiResponse(code = 500, message = "Internal Server Error")
    })
    @Path("/linkgroups")
    @Produces(MediaType.APPLICATION_JSON)
    public List<LinkGroupInfo> getLinkGroups(@ApiParam(value = "The name of the link group.")
                                      @QueryParam("name") String name,
                                      @ApiParam(value = "The id of the link group.")
                                      @QueryParam("id") Long id,
                                      @ApiParam(value = "Whether the link group allows online access latency.")
                                      @QueryParam("onlineAllowed") Boolean onlineAllowed,
                                      @ApiParam(value = "Whether the link group allows nearline access latency.")
                                      @QueryParam("nearlineAllowed") Boolean nearlineAllowed,
                                      @ApiParam(value = "Whether the link group allows replica retention policy.")
                                      @QueryParam("replicaAllowed") Boolean replicaAllowed,
                                      @ApiParam(value = "Whether the link group allows output retention policy.")
                                      @QueryParam("outputAllowed") Boolean outputAllowed,
                                      @ApiParam(value = "Whether the link group allows custodial retention policy.")
                                      @QueryParam("custodialAllowed") Boolean custodialAllowed,
                                      @ApiParam(value = "VO group associated with the link.")
                                      @QueryParam("voGroup") String voGroup,
                                      @ApiParam(value = "VO role associated with the link.")
                                      @QueryParam("voRole") String voRole,
                                      @ApiParam(value = "Minimum amount of space (in bytes) still available via the link.")
                                      @QueryParam("minAvailableSpace") Long minAvailableSpace) {
        if (!spaceReservationEnabled) {
            throw new NotFoundException();
        }

        if (!HttpServletRequests.isAdmin(request)) {
            throw new ForbiddenException(FORBIDDEN);
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

            return reply.getLinkGroups()
                        .stream()
                        .filter(filter)
                        .map(LinkGroupInfo::new)
                        .collect(Collectors.toList());
        } catch (CacheException | InterruptedException | NoRouteToCellException ex) {
            throw new InternalServerErrorException(ex);
        }
    }

    @GET
    @ApiOperation("Get information about space tokens.  "
                    + "Requires admin role.")
    @ApiResponses({
        @ApiResponse(code = 403, message = "Space token info only accessible to admin users."),
        @ApiResponse(code = 404, message = "DCache not configured for space management."),
        @ApiResponse(code = 500, message = "Internal Server Error")
    })
    @Path("/tokens")
    @Produces(MediaType.APPLICATION_JSON)
    public List<SpaceToken> getTokensForGroup(@ApiParam(value = "The id of the space token.")
                                              @QueryParam("id") Long id,
                                              @ApiParam(value = "VO group associated with the token.")
                                              @QueryParam("voGroup") String voGroup,
                                              @ApiParam(value = "VO role associated with the token.")
                                              @QueryParam("voRole") String voRole,
                                              @ApiParam(value = "Access Latency associated with the token.")
                                              @QueryParam("accessLatency") String accessLatency,
                                              @ApiParam(value = "Retention Policy associated with the token.")
                                              @QueryParam("retentionPolicy") String retentionPolicy,
                                              @ApiParam(value = "Id of link group to which token belongs.")
                                              @QueryParam("groupId") Long groupId,
                                              @ApiParam(value = "State of the token.")
                                              @QueryParam("state") String state,
                                              @ApiParam(value = "Minimum size (in bytes) of token.")
                                              @QueryParam("minSize") Long minSize,
                                              @ApiParam(value = "Minimum amount of space (in bytes) still free for token.")
                                              @QueryParam("minFreeSpace") Long minFreeSpace) {
        if (!spaceReservationEnabled) {
            throw new NotFoundException();
        }

        if (!HttpServletRequests.isAdmin(request)) {
            throw new ForbiddenException(FORBIDDEN);
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

            return reply.getSpaceTokenSet()
                        .stream()
                        .filter(filter)
                        .map(SpaceToken::new)
                        .collect(Collectors.toList());
        } catch (CacheException | InterruptedException | NoRouteToCellException ex) {
            throw new InternalServerErrorException(ex);
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
            predicate = predicate.and(space -> RetentionPolicy.valueOf(retentionPolicy.toUpperCase())
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
