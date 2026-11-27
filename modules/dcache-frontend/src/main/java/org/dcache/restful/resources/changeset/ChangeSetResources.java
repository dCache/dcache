package org.dcache.restful.resources.changeset;

import static org.dcache.restful.providers.SuccessfulResponse.successfulResponse;

import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import dmg.cells.nucleus.CellPath;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.Consumes;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.dcache.auth.Subjects;
import org.dcache.poolmanager.Partition;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.restful.util.RequestUser;
import org.dcache.restful.util.Responses;
import org.dcache.restful.util.changeset.ChangeSetOperations;
import org.dcache.restful.util.changeset.ChangeSetStore;
import org.dcache.restful.util.selection.PsuAdminCommands;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

/**
 * <p>RESTful API to prepare, review and apply a set of changes to the pool selection
 * setup and partitions of the PoolManager as one unit.</p>
 */
@Component
@Api(value = "poolmanager", authorizations = {@Authorization("basicAuth")})
@Path("/changesets")
public final class ChangeSetResources {

    @Inject
    @Named("change-set-store")
    private ChangeSetStore changeSetStore;

    @Inject
    @Named("psu-admin-commands")
    private PsuAdminCommands psuAdminCommands;

    @Inject
    private PoolMonitor poolMonitor;

    @POST
    @ApiOperation("Create a draft change set. The operations are checked for valid names "
          + "and values, and partition parameters against the existing partitions; "
          + "everything else is only checked when the change set is applied. "
          + "Requires admin role.")
    @ApiResponses({
          @ApiResponse(code = 201, message = "Created"),
          @ApiResponse(code = 400, message = "Bad Request"),
          @ApiResponse(code = 403, message = "Change set command only accessible to admin users."),
          @ApiResponse(code = 404, message = "Partition not found"),
          @ApiResponse(code = 500, message = "Internal Server Error"),
    })
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createChangeSet(@ApiParam(value = "JSON object describing the change set: "
          + "description (String, optional), "
          + "operations (array of operations, required, at least one operation). "
          + "Each operation has an op field naming it (e.g. createPool) and the fields "
          + "of the corresponding endpoint.", required = true) String requestPayload) {

        RequestUser.checkAdmin("Change set command only accessible to admin users.");

        JSONObject changeSet = new JSONObject();
        try {
            JSONObject payload = new JSONObject(requestPayload);
            changeSet.put("status", "DRAFT");
            changeSet.put("description", payload.optString("description", null));
            changeSet.put("createdBy", Subjects.getUserName(RequestUser.getSubject()));
            changeSet.put("createdAt", System.currentTimeMillis());
            changeSet.put("operations", checkedOperations(payload));
        } catch (JSONException e) {
            throw new BadRequestException(e);
        }

        long id = changeSetStore.create(changeSet);
        return Response.status(Response.Status.CREATED)
              .entity(new JSONObject().put("id", id).toString())
              .build();
    }

    @PUT
    @ApiOperation("Replace the description and operations of a change set, which is a draft "
          + "again afterwards. Possible for drafts and for change sets that failed before "
          + "any command was sent. The operations are checked like on creation. "
          + "Requires admin role.")
    @ApiResponses({
          @ApiResponse(code = 200, message = "OK"),
          @ApiResponse(code = 400, message = "Bad Request"),
          @ApiResponse(code = 403, message = "Change set command only accessible to admin users."),
          @ApiResponse(code = 404, message = "Not Found"),
          @ApiResponse(code = 409, message = "Change set was already applied or is being applied"),
          @ApiResponse(code = 500, message = "Internal Server Error"),
    })
    @Path("/{id : [0-9]+}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateChangeSet(
          @ApiParam(value = "The change set to be replaced.", required = true)
          @PathParam("id") long id,
          @ApiParam(value = "JSON object describing the change set, like on creation.",
                required = true) String requestPayload) {

        RequestUser.checkAdmin("Change set command only accessible to admin users.");

        ChangeSetStore.Stored stored = changeSetStore.read(id)
              .orElseThrow(() -> new NotFoundException("Change set not found: " + id));
        JSONObject changeSet = stored.changeSet();
        String status = changeSet.getString("status");
        // a change set that was claimed by apply may have changed the setup already
        boolean editable = status.equals("DRAFT")
              || (status.equals("FAILED") && !changeSet.has("appliedBy"));
        if (!editable) {
            throw new ClientErrorException("Change set " + id + " is " + status,
                  Response.Status.CONFLICT);
        }

        try {
            JSONObject payload = new JSONObject(requestPayload);
            changeSet.put("status", "DRAFT");
            changeSet.remove("failure");
            changeSet.put("description", payload.optString("description", null));
            changeSet.put("operations", checkedOperations(payload));
            changeSet.put("updatedBy", Subjects.getUserName(RequestUser.getSubject()));
            changeSet.put("updatedAt", System.currentTimeMillis());
        } catch (JSONException e) {
            throw new BadRequestException(e);
        }

        changeSetStore.update(id, changeSet, stored.version());
        return successfulResponse(Response.Status.OK);
    }

    /**
     * Returns the operations of a POST or PUT body after checking each of them.
     */
    private JSONArray checkedOperations(JSONObject payload) {
        JSONArray operations = payload.getJSONArray("operations");
        if (operations.isEmpty()) {
            throw new BadRequestException("A change set needs at least one operation");
        }
        for (int i = 0; i < operations.length(); i++) {
            JSONObject operation = operations.getJSONObject(i);
            ChangeSetOperations.commandsFor(operation);
            if (operation.getString("op").equals("setPartition")) {
                checkPartitionParameters(operation);
            }
        }
        return operations;
    }

    // 'pm set' silently ignores unknown parameters, so check them like PATCH /partitions does
    private void checkPartitionParameters(JSONObject operation) {
        String partition = operation.getString("partition");
        Partition current = poolMonitor.getPartitionManager().getPartitions().get(partition);
        if (current == null) {
            throw new NotFoundException("Partition not found: " + partition);
        }
        for (String key : operation.getJSONObject("properties").keySet()) {
            if (!current.getAllProperties().containsKey(key)) {
                throw new BadRequestException("Unknown parameter for partition "
                      + partition + ": " + key);
            }
        }
    }

    @POST
    @ApiOperation("Apply a draft change set: send the commands of all operations in order to "
          + "one PoolManager and save its configuration. A change set that removes a running "
          + "pool is rejected before any command is sent. Otherwise stops at the first "
          + "operation that fails; operations before it stay applied. Requires admin role.")
    @ApiResponses({
          @ApiResponse(code = 200, message = "OK"),
          @ApiResponse(code = 400, message = "Bad Request"),
          @ApiResponse(code = 403, message = "Change set command only accessible to admin users."),
          @ApiResponse(code = 404, message = "Not Found"),
          @ApiResponse(code = 409, message = "Change set is not a draft or removes a running pool"),
          @ApiResponse(code = 500, message = "Internal Server Error"),
    })
    @Path("/{id : [0-9]+}/apply")
    @Produces(MediaType.APPLICATION_JSON)
    public Response applyChangeSet(
          @ApiParam(value = "The change set to be applied.", required = true)
          @PathParam("id") long id) {

        RequestUser.checkAdmin("Change set command only accessible to admin users.");

        ChangeSetStore.Stored stored = changeSetStore.read(id)
              .orElseThrow(() -> new NotFoundException("Change set not found: " + id));
        JSONObject changeSet = stored.changeSet();
        String status = changeSet.getString("status");
        if (!status.equals("DRAFT")) {
            throw new ClientErrorException("Change set " + id + " is " + status,
                  Response.Status.CONFLICT);
        }

        JSONArray operations = changeSet.getJSONArray("operations");
        List<List<String>> commands = new ArrayList<>();
        for (int i = 0; i < operations.length(); i++) {
            commands.add(ChangeSetOperations.commandsFor(operations.getJSONObject(i)));
        }

        // like DELETE /pools/{pool}, but for the whole change set and before anything is sent
        PoolSelectionUnit psu = poolMonitor.getPoolSelectionUnit();
        for (int i = 0; i < operations.length(); i++) {
            JSONObject operation = operations.getJSONObject(i);
            if (operation.getString("op").equals("removePool")) {
                String pool = operation.getString("pool");
                SelectionPool selectionPool = psu.getPool(pool);
                if (selectionPool != null && selectionPool.isActive()) {
                    String message = "Pool " + pool + " is running; stop it first";
                    changeSet.put("status", "FAILED");
                    changeSet.put("failure",
                          new JSONObject().put("message", message).put("operation", i));
                    changeSetStore.update(id, changeSet, stored.version());
                    throw new ClientErrorException(message, Response.Status.CONFLICT);
                }
            }
        }

        // claim the change set, so that it cannot be applied twice at the same time
        changeSet.put("status", "APPLYING");
        changeSet.put("appliedBy", Subjects.getUserName(RequestUser.getSubject()));
        changeSet.put("appliedAt", System.currentTimeMillis());
        int version = changeSetStore.update(id, changeSet, stored.version());

        int operation = -1;
        try {
            CellPath path = psuAdminCommands.adminBackend();
            for (operation = 0; operation < commands.size(); operation++) {
                for (String command : commands.get(operation)) {
                    String reply = psuAdminCommands.send(path, command);
                    // 'psu set pool' reports an unknown pool in its reply, not as an error
                    if (reply.startsWith("No pools")) {
                        throw new NotFoundException(reply);
                    }
                }
            }
            operation = -1;
            psuAdminCommands.saveAndRespond(path, "Change set " + id + " was applied",
                  Response.Status.OK);
        } catch (RuntimeException e) {
            changeSet.put("status", "FAILED");
            JSONObject failure = new JSONObject().put("message", e.getMessage());
            if (operation >= 0) {
                failure.put("operation", operation);
            }
            changeSet.put("failure", failure);
            changeSetStore.update(id, changeSet, version);
            throw e;
        }

        changeSet.put("status", "APPLIED");
        changeSetStore.update(id, changeSet, version);
        return successfulResponse(Response.Status.OK);
    }

}
