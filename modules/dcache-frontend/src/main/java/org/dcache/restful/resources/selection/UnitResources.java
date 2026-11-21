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

import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnitGroup;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import diskCacheV111.poolManager.PoolSelectionUnit.UnitType;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.restful.providers.selection.Unit;
import org.dcache.restful.providers.selection.UnitGroup;
import org.dcache.restful.util.Responses;
import org.dcache.restful.util.RequestUser;
import org.dcache.restful.util.selection.PsuAdminCommands;
import org.dcache.restful.util.selection.SelectionNames;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

/**
 * <p>RESTful API to the {@link org.dcache.poolmanager.PoolMonitor}, in
 * order to deliver unit and unit group information as well as to add/remove/manage
 * unitgroups and units.</p>
 *
 * @version v1.0
 */
@Component
@Tag(name = "poolmanager")
@SecurityRequirement(name = "basicAuth")
@Path("/units")
public final class UnitResources {

    private static final Set<String> UNIT_TYPES = Set.of("store", "net", "protocol", "dcache");

    @Inject
    private PoolMonitor poolMonitor;

    @Inject
    @Named("psu-admin-commands")
    private PsuAdminCommands psuAdminCommands;

    @GET
    @Operation(summary = "List all units."
          + " Results sorted lexicographically by unit name.")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getUnits() {
        List<Unit> result = poolMonitor.getPoolSelectionUnit().getSelectionUnits().values()
              .stream()
              .sorted(Comparator.comparing(SelectionUnit::getName))
              .map(Unit::new)
              .collect(Collectors.toList());
        return Responses.buildResponse(result);
    }


    @GET
    @Operation(summary = "List all unitgroups."
          + " Results sorted lexicographically by unit group name.")
    @Path("/groups")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getUnitGroups() {
        PoolSelectionUnit psu = poolMonitor.getPoolSelectionUnit();

        List<UnitGroup> result = poolMonitor.getPoolSelectionUnit().getUnitGroups().values()
              .stream()
              .sorted(Comparator.comparing(SelectionUnitGroup::getName))
              .map((g) -> new UnitGroup(g, psu))
              .collect(Collectors.toList());
        return Responses.buildResponse(result);
    }

    @POST
    @Operation(summary = "Add a unit via 'psu create unit'. Requires admin role. "
            + "Saves the PoolManager configuration.")
    @ApiResponses({
            @ApiResponse(responseCode = "201", description = "Created"),
            @ApiResponse(responseCode = "400", description = "Bad Request"),
            @ApiResponse(responseCode = "403", description = "Unit command only accessible to admin users."),
            @ApiResponse(responseCode = "409", description = "Unit already exists"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response addUnit(@Parameter(description = "JSON object describing the new unit: "
            + "name (String, required, e.g. fuse:goettingen@osm, 131.169.0.0/16, Http/1), "
            + "type (String, required, one of store, net, protocol, dcache).",
            required = true) String requestPayload) {

        RequestUser.checkAdmin("Unit command only accessible to admin users.");

        String name;
        String type;
        try {
            JSONObject payload = new JSONObject(requestPayload);
            name = payload.getString("name");
            type = payload.getString("type");
        } catch (JSONException e) {
            throw new BadRequestException(e);
        }
        SelectionNames.checkValidUnitName(name);
        if (!UNIT_TYPES.contains(type)) {
            throw new BadRequestException("Invalid unit type: " + type);
        }

        return psuAdminCommands.execute("Unit: " + name + " was created",
                Response.Status.CREATED, "psu create unit -" + type + " " + name);
    }

    @DELETE
    @Operation(summary = "Remove a unit via 'psu remove unit'. Requires admin role. "
            + "Saves the PoolManager configuration.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "successful operation"),
            @ApiResponse(responseCode = "400", description = "Bad Request"),
            @ApiResponse(responseCode = "403", description = "Unit command only accessible to admin users."),
            @ApiResponse(responseCode = "404", description = "Not Found"),
            @ApiResponse(responseCode = "409", description = "Conflict"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{unit : .+}")
    public Response removeUnit(
            @Parameter(description = "The unit to be removed.", required = true)
            @PathParam("unit") String unit) {

        RequestUser.checkAdmin("Unit command only accessible to admin users.");
        SelectionNames.checkValidUnitName(unit);

        return psuAdminCommands.execute("Unit: " + unit + " was removed", Response.Status.OK,
                "psu remove unit " + unit + netOption(unit));
    }

    @POST
    @Operation(summary = "Add a unit group via 'psu create ugroup'. Requires admin role. "
            + "Saves the PoolManager configuration.")
    @ApiResponses({
            @ApiResponse(responseCode = "201", description = "Created"),
            @ApiResponse(responseCode = "400", description = "Bad Request"),
            @ApiResponse(responseCode = "403", description = "Unit command only accessible to admin users."),
            @ApiResponse(responseCode = "409", description = "Unit group already exists"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Path("/groups")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response addUnitGroup(@Parameter(description = "JSON object describing the new unit group: "
            + "name (String, required).", required = true) String requestPayload) {

        RequestUser.checkAdmin("Unit command only accessible to admin users.");

        String name;
        try {
            name = new JSONObject(requestPayload).getString("name");
        } catch (JSONException e) {
            throw new BadRequestException(e);
        }
        SelectionNames.checkValidName("unit group", name);

        return psuAdminCommands.execute("Unit group: " + name + " was created",
                Response.Status.CREATED, "psu create ugroup " + name);
    }

    @PUT
    @Operation(summary = "Add a unit to a unit group via 'psu addto ugroup'. Requires admin role. "
            + "Saves the PoolManager configuration.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "successful operation"),
            @ApiResponse(responseCode = "400", description = "Bad Request"),
            @ApiResponse(responseCode = "403", description = "Unit command only accessible to admin users."),
            @ApiResponse(responseCode = "404", description = "Not Found"),
            @ApiResponse(responseCode = "409", description = "Unit is already a member of the unit group"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/groups/{group}/units/{unit : .+}")
    public Response addUnitToGroup(
            @Parameter(description = "The unit group the unit should be added to.", required = true)
            @PathParam("group") String group,
            @Parameter(description = "The unit to be added.", required = true)
            @PathParam("unit") String unit) {

        RequestUser.checkAdmin("Unit command only accessible to admin users.");
        SelectionNames.checkValidName("unit group", group);
        SelectionNames.checkValidUnitName(unit);

        return psuAdminCommands.execute("Unit: " + unit + " was added to unit group: " + group,
                Response.Status.OK, "psu addto ugroup " + group + " " + unit + netOption(unit));
    }

    @DELETE
    @Operation(summary = "Remove a unit from a unit group via 'psu removefrom ugroup'. "
            + "Requires admin role. Saves the PoolManager configuration.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "successful operation"),
            @ApiResponse(responseCode = "400", description = "Bad Request"),
            @ApiResponse(responseCode = "403", description = "Unit command only accessible to admin users."),
            @ApiResponse(responseCode = "404", description = "Not Found"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/groups/{group}/units/{unit : .+}")
    public Response removeUnitFromGroup(
            @Parameter(description = "The unit group the unit should be removed from.", required = true)
            @PathParam("group") String group,
            @Parameter(description = "The unit to be removed.", required = true)
            @PathParam("unit") String unit) {

        RequestUser.checkAdmin("Unit command only accessible to admin users.");
        SelectionNames.checkValidName("unit group", group);
        SelectionNames.checkValidUnitName(unit);

        return psuAdminCommands.execute(
                "Unit: " + unit + " was removed from unit group: " + group,
                Response.Status.OK,
                "psu removefrom ugroup " + group + " " + unit + netOption(unit));
    }

    /**
     * psu commands on net units require the '-net' option, so that the unit is looked up as
     * an address/subnet instead of by its name.
     */
    private String netOption(String unit) {
        SelectionUnit selectionUnit = poolMonitor.getPoolSelectionUnit()
                .getSelectionUnits().get(unit);
        return selectionUnit != null && selectionUnit.getType() == UnitType.NET ? " -net" : "";
    }
}
