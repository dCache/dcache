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
package org.dcache.restful.resources.pool;

import static org.dcache.restful.providers.ErrorResponseProvider.NOT_IMPLEMENTED;
import static org.dcache.restful.providers.PagedList.TOTAL_COUNT_HEADER;
import static org.dcache.restful.providers.SuccessfulResponse.successfulResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PoolModifyModeMessage;
import diskCacheV111.vehicles.PoolMoverKillMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Exceptions;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.headers.Header;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.InternalServerErrorException;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import org.dcache.cells.CellStub;
import org.dcache.pool.nearline.json.NearlineData;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.restful.providers.PagedList;
import org.dcache.restful.providers.pool.MoverData;
import org.dcache.restful.providers.pool.PoolInfo;
import org.dcache.restful.providers.pool.PoolModeUpdate;
import org.dcache.restful.providers.selection.Pool;
import org.dcache.restful.services.pool.PoolInfoService;
import org.dcache.restful.services.transfers.TransferInfoService;
import org.dcache.restful.util.RequestUser;
import org.dcache.restful.util.selection.PsuAdminCommands;
import org.dcache.restful.util.selection.SelectionNames;
import org.dcache.util.spring.Required;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * <p>RESTful API to the {@link PoolInfoService} service.</p>
 *
 * @version v1.0
 */
@Component
@Tag(name = "pools")
@SecurityRequirement(name = "basicAuth")
@Path("/pools")
public final class PoolInfoResources {

    private static final Logger LOGGER = LoggerFactory.getLogger(PoolInfoResources.class);

    private static final String TYPE_ERROR =
          "type specification %s not supported; please indicate all "
                + "door-initiated movers by an undefined "
                + "type parameter, or p2p movers using "
                + "'p2p-client,p2p-server'";

    @Context
    private HttpServletRequest request;

    @Context
    private HttpServletResponse response;

    @Inject
    private PoolInfoService service;

    @Inject
    private TransferInfoService transferInfoService;

    @Inject
    private PoolMonitor poolMonitor;

    @Inject
    @Named("pool-stub")
    private CellStub poolStub;

    @Inject
    @Named("psu-admin-commands")
    private PsuAdminCommands psuAdminCommands;

    private boolean unlimitedOperationVisibility;

    @GET
    @Operation(summary = "Get information about all pools (name, group membership, links).  "
          + "Results sorted lexicographically by pool name.")
    @Produces(MediaType.APPLICATION_JSON)
    public List<Pool> getPools() throws CacheException {
        PoolSelectionUnit psu = poolMonitor.getPoolSelectionUnit();

        return psu.getPools().values()
              .stream()
              .sorted(Comparator.comparing(SelectionPool::getName))
              .map((p) -> new Pool(p.getName(), psu))
              .collect(Collectors.toList());
    }


    @GET
    @Operation(summary = "Get information about a specific pool (name, group membership, links).")
    @ApiResponses({
          @ApiResponse(responseCode = "200", description = "successful operation"),
          @ApiResponse(responseCode = "404", description = "Not Found"),
          @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Path("/{pool}")
    @Produces(MediaType.APPLICATION_JSON)
    public Pool getPool(@Parameter(description = "The pool to be described.",
          required = true)
    @PathParam("pool") String pool) {
        try {
            return new Pool(pool, poolMonitor.getPoolSelectionUnit());
        } catch (NoSuchElementException e) {
            throw new NotFoundException(e);
        }
    }

    private void setlastModfied() {
        long lastModified = service.getLastUpdated();
        if(lastModified != 0L) {
            response.addDateHeader("Last-Modified", lastModified);
        }
    }

    @GET
    @Operation(summary = "Get information about a specific pool (configuration, state, usage).")
    @Path("/{pool}/usage")
    @Produces(MediaType.APPLICATION_JSON)
    public PoolInfo getPoolUsage(@Parameter(description = "The pool to be described.",
          required = true)
    @PathParam("pool") String pool) {
        PoolInfo info = new PoolInfo();

        service.getDiagnosticInfo(pool, info);
        setlastModfied();
        return info;
    }



    @GET
    @Operation(summary = "Get information about a specific PNFS-ID usage within a "
          + "specific pool.")
    @Path("/{pool}/{pnfsid}")
    @Produces(MediaType.APPLICATION_JSON)
    public PoolInfo getRepositoryInfoForFile(@Parameter(description = "The pool to be described.",
          required = true)
    @PathParam("pool") String pool,
          @Parameter(description = "The PNFS-ID of the file to be described.",
                required = true)
          @PathParam("pnfsid") PnfsId pnfsid) {
        PoolInfo info = new PoolInfo();

        service.getCacheInfo(pool, pnfsid, info);
        setlastModfied();
        return info;
    }


    @GET
    @Operation(summary = "Get histogram data concerning activity on a specific pool (48-hour "
          + "window).")
    @Path("/{pool}/histograms/queues")
    @Produces(MediaType.APPLICATION_JSON)
    public PoolInfo getQueueHistograms(@Parameter(description = "The pool to be described.",
          required = true)
    @PathParam("pool") String group) {
        PoolInfo info = new PoolInfo();

        service.getQueueStat(group, info);
        setlastModfied();
        return info;
    }


    @GET
    @Operation(summary = "Get histogram data concerning file lifetime on a specific pool (60-day "
          + "window).")
    @Path("/{pool}/histograms/files")
    @Produces(MediaType.APPLICATION_JSON)
    public PoolInfo getFilesHistograms(@Parameter(description = "The pool to be described.",
          required = true)
    @PathParam("pool") String group) {
        PoolInfo info = new PoolInfo();

        service.getFileStat(group, info);
        setlastModfied();
        return info;
    }


    @GET
    @Operation(summary = "Get mover information for a specific pool.")
    @ApiResponses({
          @ApiResponse(responseCode = "200", description = "successful operation", headers = {
                @Header(name = "X-Total-Count", description = "Total "
                      + "number of potential responses.  This may be greater "
                      + "than the number of response if offset or limit are "
                      + "specified")
          }),
          @ApiResponse(responseCode = "403",
                description = "Pool command only accessible to admin users."),
          @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Path("/{pool}/movers")
    @Produces(MediaType.APPLICATION_JSON)
    public List<MoverData> getMovers(@Parameter(description = "The pool to be described.")
    @PathParam("pool") String pool,
          @Parameter(description = "A comma-seperated list of mover types. "
                + "Currently, either 'p2p-client,p2p-server' "
                + "or none (meaning all) is supported.")
          @QueryParam("type") String typeList,
          @Parameter(description = "The number of items to skip.")
          @DefaultValue("0")
          @QueryParam("offset") int offset,
          @Parameter(description = "The maximum number of items to return.")
          @QueryParam("limit") Integer limit,
          @Parameter(description = "Select movers operating on a specific PNFS-ID.")
          @QueryParam("pnfsid") String pnfsid,
          @Parameter(description = "Select movers with a specific queue.")
          @QueryParam("queue") String queue,
          @Parameter(description = "Select movers in a particular state.")
          @QueryParam("state") String state,
          @Parameter(description = "Select movers with a specific mode.")
          @QueryParam("mode") String mode,
          @Parameter(description = "Select movers initiated by a specific door.")
          @QueryParam("door") String door,
          @Parameter(description = "Select movers with a specific storage class.")
          @QueryParam("storageClass") String storageClass,
          @Parameter(description = "How returned items should be sorted.")
          @DefaultValue("door,startTime")
          @QueryParam("sort") String sort) {
        if (!RequestUser.canViewFileOperations(unlimitedOperationVisibility)) {
            throw new ForbiddenException("Pool command only accessible to admin users.");
        }

        limit = limit == null ? Integer.MAX_VALUE : limit;

        String[] type = typeList == null ? new String[0] :
              typeList.split(",");
        PagedList<MoverData> pagedList;

        try {
            if (type.length == 0) {
                pagedList = service.getMovers(pool,
                      offset,
                      limit,
                      pnfsid,
                      queue,
                      state,
                      mode,
                      door,
                      storageClass,
                      sort);
                response.addIntHeader(TOTAL_COUNT_HEADER, pagedList.total);
                setlastModfied();
                return pagedList.contents;
            } else if (type.length == 2) {
                if ((type[0].equals("p2p-client")
                      && type[1].equals("p2p-server"))
                      || (type[1].equals("p2p-client")
                      && type[0].equals("p2p-server"))) {
                    pagedList = service.getP2p(pool,
                          offset,
                          limit,
                          pnfsid,
                          queue,
                          state,
                          storageClass,
                          sort);
                    response.addIntHeader(TOTAL_COUNT_HEADER, pagedList.total);
                    return pagedList.contents;
                }
            }
        } catch (InterruptedException | NoRouteToCellException | CacheException e) {
            LOGGER.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        }

        String error = String.format(TYPE_ERROR, typeList);

        throw new InternalServerErrorException(error, NOT_IMPLEMENTED);
    }


    @GET
    @Operation(summary = "Get nearline activity information for a specific pool.")
    @ApiResponses({
          @ApiResponse(responseCode = "200", description = "successful operation"),
          @ApiResponse(responseCode = "400", description = "unrecognized queue type"),
          @ApiResponse(responseCode = "403",
                description = "Pool command only accessible to admin users."),
          @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Path("/{pool}/nearline/queues")
    @Produces(MediaType.APPLICATION_JSON)
    public List<NearlineData> getNearlineQueues(
          @Parameter(description = "The pool to be described.")
          @PathParam("pool") String pool,
          @Parameter(description = "Select transfers of a specific type "
                + "(flush, stage, remove).")
          @DefaultValue("flush,stage,remove")
          @QueryParam("type") String typeList,
          @Parameter(description = "The number of items to skip.")
          @DefaultValue("0")
          @QueryParam("offset") int offset,
          @Parameter(description = "The maximum number of items to return.")
          @QueryParam("limit") Integer limit,
          @Parameter(description = "Select only operations affecting this PNFS-ID.")
          @QueryParam("pnfsid") String pnfsid,
          @Parameter(description = "Select only operations in this state.")
          @QueryParam("state") String state,
          @Parameter(description = "Select only operations of this storage class.")
          @QueryParam("storageClass") String storageClass,
          @Parameter(description = "How the returned values should be sorted.")
          @DefaultValue("class,created")
          @QueryParam("sort") String sort) {
        if (!RequestUser.canViewFileOperations(unlimitedOperationVisibility)) {
            throw new ForbiddenException("Pool command only accessible to admin users.");
        }

        limit = limit == null ? Integer.MAX_VALUE : limit;

        List<NearlineData> list = new ArrayList<>();

        PagedList<NearlineData> pagedList;
        int count = 0;

        try {
            String[] types = typeList.split(",");
            for (String type : types) {
                switch (type) {
                    case "flush":
                        pagedList = service.getFlush(pool,
                              offset,
                              limit,
                              pnfsid,
                              state,
                              storageClass,
                              sort);
                        list.addAll(pagedList.contents);
                        count += pagedList.total;
                        break;
                    case "stage":
                        pagedList = service.getStage(pool,
                              offset,
                              limit,
                              pnfsid,
                              state,
                              storageClass,
                              sort);
                        list.addAll(pagedList.contents);
                        count += pagedList.total;
                        break;
                    case "remove":
                        pagedList = service.getRemove(pool,
                              offset,
                              limit,
                              pnfsid,
                              state,
                              storageClass,
                              sort);
                        list.addAll(pagedList.contents);
                        count += pagedList.total;
                        break;
                    default:
                        throw new BadRequestException("unrecognized queue type: "
                              + type);
                }
            }

            response.addIntHeader(TOTAL_COUNT_HEADER, count);
            setlastModfied();
            return list;
        } catch (InterruptedException | NoRouteToCellException | CacheException e) {
            LOGGER.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        }
    }

    @DELETE
    @Operation(summary = "Kill a mover.  Requires admin role.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "successful operation"),
          @ApiResponse(responseCode = "400", description = "Bad Request"),
          @ApiResponse(responseCode = "403",
                description = "Pool command only accessible to admin users."),
          @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Path("/{pool}/movers/{id : [0-9]+}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response killMovers(@Parameter(description = "The pool with the mover to be killed.",
          required = true)
    @PathParam("pool") String pool,
          @Parameter(description = "The id of the mover to be killed.",
                required = true)
          @PathParam("id") int id) {
        RequestUser.checkAdmin("Pool command only accessible to admin users.");

        try {
            poolStub.sendAndWait(new CellPath(pool),
                  new PoolMoverKillMessage(pool, id,
                        "Killed by user."));
            transferInfoService.setCancelled(pool, id);
        } catch (IllegalArgumentException e) {
            throw new BadRequestException(e);
        } catch (CacheException e) {
            if (e.getRc() == CacheException.MOVER_NOT_FOUND) {
                transferInfoService.setCancelled(pool, id);
            } else {
                LOGGER.warn(Exceptions.meaningfulMessage(e));
                throw new InternalServerErrorException(e);
            }
        } catch (InterruptedException | NoRouteToCellException e) {
            LOGGER.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        }

        return successfulResponse(Response.Status.OK);
    }

    @Required
    public void setUnlimitedOperationVisibility(boolean visibility) {
        unlimitedOperationVisibility = visibility;
    }

    @PATCH
    @Operation(summary = "Modify a pool's mode.  Requires admin role. This change is not persistent after restart")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "successful operation"),
          @ApiResponse(responseCode = "400", description = "Bad Request"),
          @ApiResponse(responseCode = "403",
                description = "Pool command only accessible to admin users."),
          @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Path("/{pool}/usage/mode")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateMode(@Parameter(description = "The pool affected by the mode change.",
          required = true)
    @PathParam("pool") String pool,
          @Parameter(description = "JSON object describing how the "
                + "pool should be modified. "
                + "(Corresponds to PoolModeUpdate.)",
                required = true)
                String requestPayload) {
        RequestUser.checkAdmin("Pool command only accessible to admin users.");

        try {
            PoolModeUpdate update
                  = new ObjectMapper().readValue(requestPayload,
                  PoolModeUpdate.class);
            PoolV2Mode mode = new PoolV2Mode(update.mode());
            mode.setResilienceEnabled(update.isResilience());
            Message message = new PoolModifyModeMessage(pool, mode);
            poolStub.sendAndWait(new CellPath(pool), message);
        } catch (JSONException | IllegalArgumentException | IOException e) {
            throw new BadRequestException(e);
        } catch (InterruptedException | NoRouteToCellException | CacheException e) {
            LOGGER.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        }

        return successfulResponse(Response.Status.OK);
    }

    @POST
    @Operation(summary = "Add a pool via 'psu create pool'. Requires admin role. "
            + "Saves the PoolManager configuration.")
    @ApiResponses({
            @ApiResponse(responseCode = "201", description = "Created"),
            @ApiResponse(responseCode = "400", description = "Bad Request"),
            @ApiResponse(responseCode = "403", description = "Pool command only accessible to admin users."),
            @ApiResponse(responseCode = "409", description = "Pool already exists"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Consumes(MediaType.APPLICATION_JSON)
    public Response addPool(@Parameter(description = "JSON object describing the new pool: "
            + "name (String, required), "
            + "noping, disabled, rdonly (boolean, default false).",
            required = true) String requestPayload) {

        RequestUser.checkAdmin("Pool command only accessible to admin users.");

        String name;
        String adminCommand;
        try {
            JSONObject payload = new JSONObject(requestPayload);
            name = payload.getString("name");
            SelectionNames.checkValidName("pool", name);
            String noping = payload.optBoolean("noping") ? " -noping" : "";
            String disabled = payload.optBoolean("disabled") ? " -disabled" : "";
            String rdonly = payload.optBoolean("rdonly") ? " -rdonly" : "";

            adminCommand = "psu create pool " + name + disabled + noping + rdonly;
        } catch (JSONException | IllegalArgumentException e) {
            throw new BadRequestException(e);
        }

        return psuAdminCommands.execute("Pool " + name + " was added", Response.Status.CREATED,
                adminCommand);
    }

    @PATCH
    @Operation(summary = "Change a pool's selection settings via 'psu set pool'. Requires admin role. "
            + "Saves the PoolManager configuration.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "successful operation"),
            @ApiResponse(responseCode = "400", description = "Bad Request"),
            @ApiResponse(responseCode = "403", description = "Pool command only accessible to admin users."),
            @ApiResponse(responseCode = "404", description = "Not Found"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Path("/{pool}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response updatePoolMode(@Parameter(description = "The pool affected by the mode change.",
                    required = true)
            @PathParam("pool") String pool,
            @Parameter(description = "JSON object describing the pool's selection settings: "
                    + "disabled, noping, rdonly (boolean). Only fields present are changed.",
                    required = true) String requestPayload) {

        RequestUser.checkAdmin("Pool command only accessible to admin users.");

        SelectionNames.checkValidName("pool", pool);

        List<String> modes = new ArrayList<>();
        try {
            JSONObject payload = new JSONObject(requestPayload);
            if (payload.has("disabled")) {
                modes.add(payload.getBoolean("disabled") ? "disabled" : "enabled");
            }
            if (payload.has("noping")) {
                modes.add(payload.getBoolean("noping") ? "noping" : "ping");
            }
            if (payload.has("rdonly")) {
                modes.add(payload.getBoolean("rdonly") ? "rdonly" : "notrdonly");
            }
        } catch (JSONException e) {
            throw new BadRequestException(e);
        }

        CellPath path = psuAdminCommands.adminBackend();
        for (String mode : modes) {
            String reply = psuAdminCommands.send(path, "psu set pool " + pool + " " + mode);
            if (reply.startsWith("No pools")) {
                throw new NotFoundException("Pool not found: " + pool);
            }
        }

        return psuAdminCommands.saveAndRespond(path, "Pool " + pool + " mode changed",
                Response.Status.OK);
    }

    @DELETE
    @Operation(summary = "Remove a stopped pool via 'psu remove pool'. Requires admin role. "
            + "Saves the PoolManager configuration.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "successful operation"),
            @ApiResponse(responseCode = "400", description = "Bad Request"),
            @ApiResponse(responseCode = "403", description = "Pool command only accessible to admin users."),
            @ApiResponse(responseCode = "404", description = "Not Found"),
            @ApiResponse(responseCode = "409", description = "Pool is running"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Path("/{pool}")
    public Response deletePool(@Parameter(description = "The pool to be deleted.",
                    required = true)
            @PathParam("pool") String pool) {

        RequestUser.checkAdmin("Pool command only accessible to admin users.");

        SelectionNames.checkValidName("pool", pool);

        SelectionPool selectionPool = poolMonitor.getPoolSelectionUnit().getPool(pool);
        if (selectionPool != null && selectionPool.isActive()) {
            throw new ClientErrorException("Pool " + pool + " is running; stop it first",
                    Response.Status.CONFLICT);
        }

        return psuAdminCommands.execute("Pool " + pool + " was deleted", Response.Status.OK,
                "psu remove pool " + pool);
    }
}
