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
package org.dcache.restful.resources.selection;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionLink;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionLinkGroup;
import diskCacheV111.poolManager.PoolSelectionUnit;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.restful.providers.selection.Link;
import org.dcache.restful.providers.selection.LinkGroup;
import org.dcache.restful.util.RequestUser;
import org.dcache.restful.util.Responses;
import org.dcache.restful.util.selection.PsuAdminCommands;
import org.dcache.restful.util.selection.SelectionNames;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

/**
 * <p>RESTful API to the {@link org.dcache.poolmanager.PoolMonitor}, in
 * order to deliver link and link group information as well as to add/remove/manage
 * links and link groups.</p>
 *
 * @version v1.0
 */
@Component
@Tag(name = "poolmanager")
@SecurityRequirement(name = "basicAuth")
@Path("/links")
public final class LinkResources {

    @Inject
    private PoolMonitor poolMonitor;

    @Inject
    @Named("psu-admin-commands")
    private PsuAdminCommands psuAdminCommands;

    @GET
    @Operation(summary = "Get information about all links."
          + " Results sorted lexicographically by link name.")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getLinks() {
        List<Link> result = poolMonitor.getPoolSelectionUnit().getLinks().values()
              .stream()
              .sorted(Comparator.comparing(SelectionLink::getName))
              .map(Link::new)
              .collect(Collectors.toList());
        return Responses.buildResponse(result);
    }

    @GET
    @Operation(summary = "Get information about all linkgroups."
          + " Results sorted lexicographically by link group name.")
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/groups")
    public Response getLinkGroups() {
        PoolSelectionUnit psu = poolMonitor.getPoolSelectionUnit();

        List<LinkGroup> result = psu.getLinkGroups().values()
              .stream()
              .sorted(Comparator.comparing(SelectionLinkGroup::getName))
              .map((g) -> new LinkGroup(g, psu))
              .collect(Collectors.toList());
        return Responses.buildResponse(result);
    }

    @POST
    @Operation(summary = "Add a link via 'psu create link'. Requires admin role. "
            + "Saves the PoolManager configuration.")
    @ApiResponses({
            @ApiResponse(responseCode = "201", description = "Created"),
            @ApiResponse(responseCode = "400", description = "Bad Request"),
            @ApiResponse(responseCode = "403", description = "Link command only accessible to admin users."),
            @ApiResponse(responseCode = "404", description = "Unit group not found"),
            @ApiResponse(responseCode = "409", description = "Link already exists"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response addLink(@Parameter(description = "JSON object describing the new link: "
            + "name (String, required), "
            + "unitGroups (array of String, required, at least one unit group).",
            required = true) String requestPayload) {

        RequestUser.checkAdmin("Link command only accessible to admin users.");

        String name;
        StringBuilder command = new StringBuilder("psu create link ");
        try {
            JSONObject payload = new JSONObject(requestPayload);
            name = payload.getString("name");
            SelectionNames.checkValidName("link", name);
            command.append(name);

            JSONArray unitGroups = payload.getJSONArray("unitGroups");
            if (unitGroups.length() == 0) {
                throw new BadRequestException("A link needs at least one unit group");
            }
            for (int i = 0; i < unitGroups.length(); i++) {
                String unitGroup = unitGroups.getString(i);
                SelectionNames.checkValidName("unit group", unitGroup);
                command.append(' ').append(unitGroup);
            }
        } catch (JSONException e) {
            throw new BadRequestException(e);
        }

        return psuAdminCommands.execute("Link: " + name + " was created",
                Response.Status.CREATED, command.toString());
    }

    @PATCH
    @Operation(summary = "Change a link's preferences and section via 'psu set link'. "
            + "Requires admin role. Saves the PoolManager configuration.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "successful operation"),
            @ApiResponse(responseCode = "400", description = "Bad Request"),
            @ApiResponse(responseCode = "403", description = "Link command only accessible to admin users."),
            @ApiResponse(responseCode = "404", description = "Not Found"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Path("/{link}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateLink(
            @Parameter(description = "The link to be changed.", required = true)
            @PathParam("link") String link,
            @Parameter(description = "JSON object describing the link's settings: "
                    + "readpref, writepref, cachepref, p2ppref (int), "
                    + "section (String, NONE removes the section). "
                    + "Only fields present are changed.",
                    required = true) String requestPayload) {

        RequestUser.checkAdmin("Link command only accessible to admin users.");
        SelectionNames.checkValidName("link", link);

        StringBuilder command = new StringBuilder("psu set link ").append(link);
        try {
            JSONObject payload = new JSONObject(requestPayload);
            for (String preference : List.of("readpref", "writepref", "cachepref", "p2ppref")) {
                if (payload.has(preference)) {
                    command.append(" -").append(preference).append('=')
                            .append(payload.getInt(preference));
                }
            }
            if (payload.has("section")) {
                String section = payload.getString("section");
                SelectionNames.checkValidName("section", section);
                command.append(" -section=").append(section);
            }
        } catch (JSONException e) {
            throw new BadRequestException(e);
        }

        return psuAdminCommands.execute("Link: " + link + " was changed", Response.Status.OK,
                command.toString());
    }

    @DELETE
    @Operation(summary = "Remove a link via 'psu remove link'. Requires admin role. "
            + "Saves the PoolManager configuration.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "successful operation"),
            @ApiResponse(responseCode = "400", description = "Bad Request"),
            @ApiResponse(responseCode = "403", description = "Link command only accessible to admin users."),
            @ApiResponse(responseCode = "404", description = "Not Found"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{link}")
    public Response removeLink(
            @Parameter(description = "The link to be removed.", required = true)
            @PathParam("link") String link) {

        RequestUser.checkAdmin("Link command only accessible to admin users.");
        SelectionNames.checkValidName("link", link);

        return psuAdminCommands.execute("Link: " + link + " was removed", Response.Status.OK,
                "psu remove link " + link);
    }

    @PUT
    @Operation(summary = "Make a link select a pool group via 'psu addto link'. Requires admin role. "
            + "Saves the PoolManager configuration.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "successful operation"),
            @ApiResponse(responseCode = "400", description = "Bad Request"),
            @ApiResponse(responseCode = "403", description = "Link command only accessible to admin users."),
            @ApiResponse(responseCode = "404", description = "Not Found"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{link}/poolgroups/{group}")
    public Response addPoolGroupToLink(
            @Parameter(description = "The link the pool group should be added to.", required = true)
            @PathParam("link") String link,
            @Parameter(description = "The pool group to be added.", required = true)
            @PathParam("group") String group) {

        RequestUser.checkAdmin("Link command only accessible to admin users.");
        SelectionNames.checkValidName("link", link);
        SelectionNames.checkValidName("pool group", group);
        checkNotAlsoPool(group);

        return psuAdminCommands.execute("Pool group: " + group + " was added to link: " + link,
                Response.Status.OK, "psu addto link " + link + " " + group);
    }

    @DELETE
    @Operation(summary = "Stop a link from selecting a pool group via 'psu removefrom link'. "
            + "Requires admin role. Saves the PoolManager configuration.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "successful operation"),
            @ApiResponse(responseCode = "400", description = "Bad Request"),
            @ApiResponse(responseCode = "403", description = "Link command only accessible to admin users."),
            @ApiResponse(responseCode = "404", description = "Not Found"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{link}/poolgroups/{group}")
    public Response removePoolGroupFromLink(
            @Parameter(description = "The link the pool group should be removed from.", required = true)
            @PathParam("link") String link,
            @Parameter(description = "The pool group to be removed.", required = true)
            @PathParam("group") String group) {

        RequestUser.checkAdmin("Link command only accessible to admin users.");
        SelectionNames.checkValidName("link", link);
        SelectionNames.checkValidName("pool group", group);
        checkNotAlsoPool(group);

        return psuAdminCommands.execute(
                "Pool group: " + group + " was removed from link: " + link,
                Response.Status.OK, "psu removefrom link " + link + " " + group);
    }

    @PUT
    @Operation(summary = "Make a link select a single pool via 'psu addto link'. Requires admin role. "
            + "Saves the PoolManager configuration.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "successful operation"),
            @ApiResponse(responseCode = "400", description = "Bad Request"),
            @ApiResponse(responseCode = "403", description = "Link command only accessible to admin users."),
            @ApiResponse(responseCode = "404", description = "Not Found"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{link}/pools/{pool}")
    public Response addPoolToLink(
            @Parameter(description = "The link the pool should be added to.", required = true)
            @PathParam("link") String link,
            @Parameter(description = "The pool to be added.", required = true)
            @PathParam("pool") String pool) {

        RequestUser.checkAdmin("Link command only accessible to admin users.");
        SelectionNames.checkValidName("link", link);
        SelectionNames.checkValidName("pool", pool);
        checkIsNotOnlyPoolGroup(pool);

        return psuAdminCommands.execute("Pool: " + pool + " was added to link: " + link,
                Response.Status.OK, "psu addto link " + link + " " + pool);
    }

    @DELETE
    @Operation(summary = "Stop a link from selecting a single pool via 'psu removefrom link'. "
            + "Requires admin role. Saves the PoolManager configuration.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "successful operation"),
            @ApiResponse(responseCode = "400", description = "Bad Request"),
            @ApiResponse(responseCode = "403", description = "Link command only accessible to admin users."),
            @ApiResponse(responseCode = "404", description = "Not Found"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{link}/pools/{pool}")
    public Response removePoolFromLink(
            @Parameter(description = "The link the pool should be removed from.", required = true)
            @PathParam("link") String link,
            @Parameter(description = "The pool to be removed.", required = true)
            @PathParam("pool") String pool) {

        RequestUser.checkAdmin("Link command only accessible to admin users.");
        SelectionNames.checkValidName("link", link);
        SelectionNames.checkValidName("pool", pool);
        checkIsNotOnlyPoolGroup(pool);

        return psuAdminCommands.execute("Pool: " + pool + " was removed from link: " + link,
                Response.Status.OK, "psu removefrom link " + link + " " + pool);
    }

    @POST
    @Operation(summary = "Add a link group via 'psu create linkGroup'. Requires admin role. "
            + "Saves the PoolManager configuration.")
    @ApiResponses({
            @ApiResponse(responseCode = "201", description = "Created"),
            @ApiResponse(responseCode = "400", description = "Bad Request"),
            @ApiResponse(responseCode = "403", description = "Link command only accessible to admin users."),
            @ApiResponse(responseCode = "409", description = "Link group already exists"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Path("/groups")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response addLinkGroup(@Parameter(description = "JSON object describing the new link group: "
            + "name (String, required).", required = true) String requestPayload) {

        RequestUser.checkAdmin("Link command only accessible to admin users.");

        String name;
        try {
            name = new JSONObject(requestPayload).getString("name");
        } catch (JSONException e) {
            throw new BadRequestException(e);
        }
        SelectionNames.checkValidName("link group", name);

        return psuAdminCommands.execute("Link group: " + name + " was created",
                Response.Status.CREATED, "psu create linkGroup " + name);
    }

    @PATCH
    @Operation(summary = "Change which retention policies and access latencies a link group allows "
            + "via 'psu set linkGroup'. Requires admin role. "
            + "Saves the PoolManager configuration.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "successful operation"),
            @ApiResponse(responseCode = "400", description = "Bad Request"),
            @ApiResponse(responseCode = "403", description = "Link command only accessible to admin users."),
            @ApiResponse(responseCode = "404", description = "Not Found"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Path("/groups/{group}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateLinkGroup(
            @Parameter(description = "The link group to be changed.", required = true)
            @PathParam("group") String group,
            @Parameter(description = "JSON object describing the link group's settings: "
                    + "custodialAllowed, replicaAllowed, outputAllowed, onlineAllowed, "
                    + "nearlineAllowed (boolean). Only fields present are changed.",
                    required = true) String requestPayload) {

        RequestUser.checkAdmin("Link command only accessible to admin users.");
        SelectionNames.checkValidName("link group", group);

        List<String> commands = new ArrayList<>();
        try {
            JSONObject payload = new JSONObject(requestPayload);
            for (String flag : List.of("custodialAllowed", "replicaAllowed", "outputAllowed",
                    "onlineAllowed", "nearlineAllowed")) {
                if (payload.has(flag)) {
                    commands.add("psu set linkGroup " + flag + " " + group + " "
                            + payload.getBoolean(flag));
                }
            }
        } catch (JSONException e) {
            throw new BadRequestException(e);
        }

        return psuAdminCommands.execute("Link group: " + group + " was changed",
                Response.Status.OK, commands.toArray(String[]::new));
    }

    @DELETE
    @Operation(summary = "Remove a link group via 'psu remove linkGroup'. Its links are kept. "
            + "Requires admin role. Saves the PoolManager configuration.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "successful operation"),
            @ApiResponse(responseCode = "400", description = "Bad Request"),
            @ApiResponse(responseCode = "403", description = "Link command only accessible to admin users."),
            @ApiResponse(responseCode = "404", description = "Not Found"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/groups/{group}")
    public Response removeLinkGroup(
            @Parameter(description = "The link group to be removed.", required = true)
            @PathParam("group") String group) {

        RequestUser.checkAdmin("Link command only accessible to admin users.");
        SelectionNames.checkValidName("link group", group);

        return psuAdminCommands.execute("Link group: " + group + " was removed",
                Response.Status.OK, "psu remove linkGroup " + group);
    }

    @PUT
    @Operation(summary = "Add a link to a link group via 'psu addto linkGroup'. A link can only be "
            + "in one link group. Requires admin role. Saves the PoolManager configuration.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "successful operation"),
            @ApiResponse(responseCode = "400", description = "Bad Request"),
            @ApiResponse(responseCode = "403", description = "Link command only accessible to admin users."),
            @ApiResponse(responseCode = "404", description = "Not Found"),
            @ApiResponse(responseCode = "409", description = "Link is already in a link group"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/groups/{group}/links/{link}")
    public Response addLinkToGroup(
            @Parameter(description = "The link group the link should be added to.", required = true)
            @PathParam("group") String group,
            @Parameter(description = "The link to be added.", required = true)
            @PathParam("link") String link) {

        RequestUser.checkAdmin("Link command only accessible to admin users.");
        SelectionNames.checkValidName("link group", group);
        SelectionNames.checkValidName("link", link);

        return psuAdminCommands.execute("Link: " + link + " was added to link group: " + group,
                Response.Status.OK, "psu addto linkGroup " + group + " " + link);
    }

    @DELETE
    @Operation(summary = "Remove a link from a link group via 'psu removefrom linkGroup'. "
            + "Requires admin role. Saves the PoolManager configuration.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "successful operation"),
            @ApiResponse(responseCode = "400", description = "Bad Request"),
            @ApiResponse(responseCode = "403", description = "Link command only accessible to admin users."),
            @ApiResponse(responseCode = "404", description = "Not Found"),
            @ApiResponse(responseCode = "409", description = "Link is not in the link group"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/groups/{group}/links/{link}")
    public Response removeLinkFromGroup(
            @Parameter(description = "The link group the link should be removed from.", required = true)
            @PathParam("group") String group,
            @Parameter(description = "The link to be removed.", required = true)
            @PathParam("link") String link) {

        RequestUser.checkAdmin("Link command only accessible to admin users.");
        SelectionNames.checkValidName("link group", group);
        SelectionNames.checkValidName("link", link);

        return psuAdminCommands.execute(
                "Link: " + link + " was removed from link group: " + group,
                Response.Status.OK, "psu removefrom linkGroup " + group + " " + link);
    }

    private void checkNotAlsoPool(String group) {
        if (poolMonitor.getPoolSelectionUnit().getPool(group) != null) {
            throw new ClientErrorException("A pool with the same name as pool group " + group
                    + " exists", Response.Status.CONFLICT);
        }
    }

    private void checkIsNotOnlyPoolGroup(String pool) {
        PoolSelectionUnit psu = poolMonitor.getPoolSelectionUnit();
        if (psu.getPool(pool) == null && psu.getPoolGroups().containsKey(pool)) {
            throw new NotFoundException("Pool not found: " + pool);
        }
    }
}
