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

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Map;
import java.util.stream.Collectors;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.restful.providers.selection.Partition;
import org.dcache.restful.util.RequestUser;
import org.dcache.restful.util.Responses;
import org.dcache.restful.util.selection.PsuAdminCommands;
import org.dcache.restful.util.selection.SelectionNames;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

/**
 * <p>RESTful API to the {@link org.dcache.poolmanager.PoolMonitor}, in
 * order to deliver partition information as well as to change partition parameters.</p>
 *
 * @version v1.0
 */
@Component
@Tag(name = "poolmanager")
@SecurityRequirement(name = "basicAuth")
@Path("/partitions")
public final class PartitionResources {

    @Inject
    private PoolMonitor poolMonitor;

    @Inject
    @Named("psu-admin-commands")
    private PsuAdminCommands psuAdminCommands;


    @GET
    @Operation(summary = "Get information about all partitions."
          + " Results sorted lexicographically by partition name.")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPartitions() {
        List<Partition> result = poolMonitor.getPartitionManager().getPartitions().entrySet()
              .stream()
              .sorted(Comparator.comparing(Entry::getKey))
              .map(Partition::new)
              .collect(Collectors.toList());
        return Responses.buildResponse(result);
    }

    @PATCH
    @Operation(summary = "Change a partition's parameters via 'pm set'. Requires admin role. "
            + "Saves the PoolManager configuration.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "successful operation"),
            @ApiResponse(responseCode = "400", description = "Bad Request"),
            @ApiResponse(responseCode = "403", description = "Partition command only accessible to admin users."),
            @ApiResponse(responseCode = "404", description = "Not Found"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Path("/{partition}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updatePartition(
            @Parameter(description = "The partition to be changed.", required = true)
            @PathParam("partition") String partition,
            @Parameter(description = "JSON object mapping parameter names to their new values, e.g. "
                    + "{\"cpucostfactor\": \"0.5\", \"p2p-allowed\": \"no\"}. The parameters "
                    + "are those listed in the partition's properties. A value of null resets "
                    + "the parameter to its inherited or default value. Only parameters "
                    + "present are changed.",
                    required = true) String requestPayload) {

        RequestUser.checkAdmin("Partition command only accessible to admin users.");
        SelectionNames.checkValidName("partition", partition);

        org.dcache.poolmanager.Partition current =
                poolMonitor.getPartitionManager().getPartitions().get(partition);
        if (current == null) {
            throw new NotFoundException("Partition not found: " + partition);
        }

        Map<String, String> properties = new HashMap<>();
        try {
            JSONObject payload = new JSONObject(requestPayload);
            for (String key : payload.keySet()) {
                if (!current.getAllProperties().containsKey(key)) {
                    throw new BadRequestException("Unknown parameter for partition "
                            + partition + ": " + key);
                }
                String value = payload.isNull(key) ? null : payload.get(key).toString();
                if (value != null) {
                    SelectionNames.checkValidOptionValue(key, value);
                }
                properties.put(key, value);
            }
        } catch (JSONException e) {
            throw new BadRequestException(e);
        }

        StringBuilder command = new StringBuilder("pm set ").append(partition);
        properties.forEach((key, value) ->
                command.append(" -").append(key).append('=').append(value == null ? "off" : value));

        return psuAdminCommands.execute("Partition: " + partition + " was changed",
                Response.Status.OK, command.toString());
    }
}
