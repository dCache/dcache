package org.dcache.restful.resources.migration;

import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.util.CacheException;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.cells.CellStub;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.restful.util.RequestUser;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * <p>RESTful API for Migration.</p>
 *
 * @author Lukas Mansour
 */
@Component
@Api(value = "migrations", authorizations = {@Authorization("basicAuth")})
@Path("/migrations")
public final class MigrationResources {

    private static final Logger LOGGER = LoggerFactory.getLogger(MigrationResources.class);

    @Inject
    private PoolMonitor poolMonitor;

    @Inject
    @Named("pool-manager-stub")
    private CellStub poolmanager;

    /**
     * Submit a migration copy request. Request to migrate (copy) all data from a source pool to a
     * target pool.
     *
     * @return response which will confirm the execution of the command (no output).
     */
    @POST
    @ApiOperation(value = "Submit a migration copy request. (See Pool Operator Commands 'migration copy')")
    @ApiResponses({@ApiResponse(code = 201, message = "Created"),
          @ApiResponse(code = 400, message = "Bad request"),
          @ApiResponse(code = 401, message = "Unauthorized"),
          @ApiResponse(code = 403, message = "Forbidden"),
          @ApiResponse(code = 429, message = "Too many requests"),
          @ApiResponse(code = 500, message = "Internal Server Error")})
    @Path("/copy")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response submitMigrationCopy(@ApiParam(
          "Description of the request. Which contains the following: "
                + "sourcePool - Name of the pool to migrate from. (String)"
                + "targetPools - Array of Pools (Strings) - Possible target pools."
                + "concurrency - Integer - Amount of Concurrent Transfers to be performed."
                + "pins - String (MOVE | KEEP) - Controls how sticky flags owned by the PinManager are handled."
                + "smode - String (SAME | CACHED | PRECIOUS | REMOVABLE | DELETE)[+<owner>[(<lifetime>)] - "
                + "Update the local replica to the given mode after transfer. An optional list of sticky flags can be specified."
                +

                "tmode - String (SAME | CACHED | PRECIOUS )[+<owner>[(<lifetime>)] - "
                + "Sets the target replica to the given mode after transfer. An optional list of sticky flags can be specified."
                +

                "verify - Boolean - Force checksum computation when an existing target is updated."
                + "eager - Boolean - Copy replicas rather than retrying when pools with existing replicas fail to respond."
                +

                "exclude - Array of Pools (Strings) - Exclude Target Pools. Single character (?) and multi character (*) wildcards may be used."
                + "include - Array of Pools (Strings) - Only include the specified pools as target pools."
                + "refresh - Integer - Specifies the period in seconds of when target pool information is queried from the pool manager. The default is 300 seconds."
                + "select - String (PROPORTIONAL | BEST | RANDOM) - Determines how a pool is selected from the set of target pools."
                + "target - String (POOL | PGROUP | LINK) - Determines the interpretation of the target pools."
                + "fileAttributes - Description of the file attributes containing: "
                + " accessed - String (<n>|[<n>]..[<m>]) - Only copy replicas within a given time period."
                + " al - String (ONLINE | NEARLINE) - Only copy replicas with the given access latency."
                + " pnfsid - Array of String (PNFSIDs) - Only copy replicas with the given PNFSIDs, must contain 1 or more PNFSIDs."
                + " state - String (CACHED | PRECIOUS) - Only copy replicas with the given replica state."
                + " rp - String (CUSTODIAL | REPLICA | OUTPUT) - Only copy replicas with the given retention policy."
                + " size - String (<n>|[<n>]..[<m>]) - Only copy replicas with size <n>, or a size within the given, possibly open-ended, interval."
                + " sticky - Array of Owners (Strings) - Only copy replicas that are sticky, if the array is not empty, then it will be restricted to the specified owners."
                + " storage - String - Only copy replicas with a certain storage class.") String requestPayload) {
        // TODO: Add expressions (pause-when, include-when, exclude-when, stop-when) to the request.
        // TODO: Pass the migration request as a message and not via a CLI-Message.
        // This was implemented as a quick and dirty trick to fulfill some other projects' programmatic contracts.

        // Something to note here: We assume in getSubject() that you HAVE to be an Administrator!
        // Should this change, then the Restrictions must be reapplied accordingly.
        // If the user is an admin --> continue.
        // If the user is not an admin however he has no restrictions --> continue.
        // If the user is not an admin and does have restrictions --> Bad Request.
        if (!RequestUser.isAdmin() && RequestUser.getRestriction() != Restrictions.none()) {
            throw new BadRequestException("User not authorized.");
        }

        // First convert to JSON.
        JSONObject jsonPayload = new JSONObject(requestPayload);
        LOGGER.error("JSON Request: {}", jsonPayload);

        if (!jsonPayload.has("sourcePool")) {
            throw new BadRequestException("No 'sourcePool' was specified.");
        }
        String sourcePoolName = jsonPayload.getString("sourcePool");
        PoolSelectionUnit psu = poolMonitor.getPoolSelectionUnit();
        PoolSelectionUnit.SelectionPool sourcePool = psu.getPool(sourcePoolName);
        if (sourcePool == null) {
            throw new BadRequestException(
                  "No source pool with the name '" + sourcePoolName + "' is could be found.");
        }
        if (!jsonPayload.has("targetPools")) {
            throw new BadRequestException("No 'targetPools' were specified.");
        }
        JSONArray targetPools = jsonPayload.getJSONArray("targetPools");
        if (targetPools.length() == 0) {
            throw new BadRequestException("No 'targetPools' were specified (Empty).");
        }
        // The targetPools will be added to the command at the end of options.

        StringBuilder commandStrBuilder = new StringBuilder("migration copy");

        // Could also have been done with anonymous inner functions, would have cost a bit of performance, but would be less clutter.
        checkAppendInteger("concurrency", "concurrency", commandStrBuilder, jsonPayload);
        checkAppendString("pins", "pins", commandStrBuilder, jsonPayload);
        checkAppendString("smode", "smode", commandStrBuilder, jsonPayload);
        checkAppendString("tmode", "tmode", commandStrBuilder, jsonPayload);
        checkAppendBoolean("verify", "verify", commandStrBuilder, jsonPayload);
        checkAppendBoolean("eager", "eager", commandStrBuilder, jsonPayload);
        checkAppendInteger("refresh", "refresh", commandStrBuilder, jsonPayload);
        if (jsonPayload.has("include")) {
            // We can't run a check if the pool exists since wildcards can be sent here!
            JSONArray includePools = jsonPayload.getJSONArray("include");
            if (includePools.length() > 0) {
                commandStrBuilder.append(" -include=").append(includePools.getString(0));
            }
            // Then we can add all the rest with the delimiter
            for (int i = 1; i < includePools.length(); i++) {
                commandStrBuilder.append(",").append(includePools.getString(i));
            }
        }
        if (jsonPayload.has("exclude")) {
            // We can't run a check if the pool exists since wildcards can be sent here!
            JSONArray excludePools = jsonPayload.getJSONArray("exclude");
            if (excludePools.length() > 0) {
                commandStrBuilder.append(" -exclude=").append(excludePools.getString(0));
            }
            // Then we can add all the rest with the delimiter
            for (int i = 1; i < excludePools.length(); i++) {
                commandStrBuilder.append(",").append(excludePools.getString(i));
            }

        }
        checkAppendString("select", "select", commandStrBuilder, jsonPayload);
        checkAppendString("target", "target", commandStrBuilder, jsonPayload);

        // fileAttributes are optional if they are left out, then it will just migrate the whole source pool.
        if (jsonPayload.has("fileAttributes")) {
            JSONObject fileAttributes = jsonPayload.getJSONObject("fileAttributes");

            checkAppendString("accessed", "accessed", commandStrBuilder, fileAttributes);
            checkAppendString("al", "al", commandStrBuilder, fileAttributes);
            if (fileAttributes.has("pnfsid")) {
                JSONArray pnfsids = fileAttributes.getJSONArray("pnfsid");
                if (pnfsids.length() > 0) {
                    commandStrBuilder.append(" -pnfsid=").append(pnfsids.getString(0));
                }
                // Then we can add all the rest with the delimiter
                for (int i = 1; i < pnfsids.length(); i++) {
                    commandStrBuilder.append(",").append(pnfsids.getString(i));
                }

            }
            checkAppendString("state", "state", commandStrBuilder, fileAttributes);
            checkAppendString("rp", "rp", commandStrBuilder, fileAttributes);
            checkAppendString("size", "size", commandStrBuilder, fileAttributes);
            if (fileAttributes.has("sticky")) {
                // If the array exists we add the sticky flag, then if it has elements it is restricted to certain owners.
                commandStrBuilder.append("-sticky");
                JSONArray sticky = fileAttributes.getJSONArray("sticky");
                if (sticky.length() > 0) {
                    commandStrBuilder.append("=").append(sticky.getString(0));
                }
                // Then we can add all the rest with the delimiter
                for (int i = 1; i < sticky.length(); i++) {
                    commandStrBuilder.append(",").append(sticky.getString(i));
                }
            }
            checkAppendString("storage", "storage", commandStrBuilder, fileAttributes);
        }
        // Add the target pools
        // We know that the length != 0 from earlier!
        for (int i = 0; i < targetPools.length(); i++) {
            String poolName = targetPools.getString(i);
            PoolSelectionUnit.SelectionPool sp = psu.getPool(poolName);
            if (sp == null) {
                throw new BadRequestException(
                      "One of the specified pools '" + poolName + "' could not be found.");
            }
            commandStrBuilder.append(" ").append(poolName);
        }

        LOGGER.info("Command parsed from json payload is: '{}'.", commandStrBuilder);
        LOGGER.info("Sending parsed command to pool '{}'.", sourcePool);
        try {
            String migrationResponseMsg = poolmanager.sendAndWait(
                  new CellPath(sourcePool.getAddress()), commandStrBuilder.toString(),
                  String.class);
            // At the moment the command will return the migration job id with the following format:
            // [MIG. JOB ID] STATE COMMAND
            // Let's extract the Mig Job Id and return it.
            LOGGER.info("Received '{}' back from source pool.", migrationResponseMsg);
            // Get the first part of the message [MIG. JOB ID] and then remove the first and last char.
            String digitsPart = migrationResponseMsg.split(" ")[0];
            String digits = digitsPart.substring(1, digitsPart.length() - 1);
            long migrationJobId = Long.parseLong(digits);

            return Response.status(Response.Status.CREATED)
                  .header("migration-job-id", migrationJobId).type(MediaType.APPLICATION_JSON)
                  .build();
        } catch (InterruptedException | CacheException | NoRouteToCellException e) {
            throw new InternalServerErrorException(e);
        }

    }

    private static void checkAppendString(String cmdParam, String jsonKey, StringBuilder sb,
          JSONObject jsonPayload) {
        if (jsonPayload.has(jsonKey)) {
            sb.append(" -").append(cmdParam).append("=").append(jsonPayload.getString(jsonKey));
        }
    }

    private static void checkAppendInteger(String cmdParam, String jsonKey, StringBuilder sb,
          JSONObject jsonPayload) {
        if (jsonPayload.has(jsonKey)) {
            sb.append(" -").append(cmdParam).append("=").append(jsonPayload.getInt(jsonKey));
        }
    }

    private static void checkAppendBoolean(String cmdParam, String jsonKey, StringBuilder sb,
          JSONObject jsonPayload) {
        if (jsonPayload.has(jsonKey) && jsonPayload.getBoolean(jsonKey)) {
            sb.append(" -").append(cmdParam);
        }
    }
}