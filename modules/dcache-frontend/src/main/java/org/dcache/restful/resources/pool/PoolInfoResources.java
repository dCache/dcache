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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONException;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Named;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.PATCH;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PoolModifyModeMessage;
import diskCacheV111.vehicles.PoolMoverKillMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import org.dcache.cells.CellStub;
import org.dcache.pool.movers.json.MoverData;
import org.dcache.pool.nearline.json.NearlineData;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.restful.providers.PagedList;
import org.dcache.restful.providers.pool.PoolInfo;
import org.dcache.restful.providers.pool.PoolModeUpdate;
import org.dcache.restful.providers.selection.Pool;
import org.dcache.restful.services.pool.PoolInfoService;
import org.dcache.restful.util.HttpServletRequests;

import static org.dcache.restful.providers.ErrorResponseProvider.NOT_IMPLEMENTED;
import static org.dcache.restful.providers.PagedList.TOTAL_COUNT_HEADER;
import static org.dcache.restful.providers.SuccessfulResponse.successfulResponse;

/**
 * <p>RESTful API to the {@link PoolInfoService} service.</p>
 *
 * @version v1.0
 */
@Component
@Path("/pools")
public final class PoolInfoResources {
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
    private PoolMonitor poolMonitor;

    @Inject
    @Named("pool-stub")
    private CellStub poolStub;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<Pool> getPools() throws CacheException {
        if (!HttpServletRequests.isAdmin(request)) {
            throw new ForbiddenException(
                            "Pool info only accessible to admin users.");
        }

        PoolSelectionUnit psu = poolMonitor.getPoolSelectionUnit();

        return psu.getPools().values()
                  .stream()
                  .map((p) -> new Pool(p.getName(), psu))
                  .collect(Collectors.toList());
    }

    @GET
    @Path("/{pool}")
    @Produces(MediaType.APPLICATION_JSON)
    public Pool getPool(@PathParam("pool") String pool) {
        if (!HttpServletRequests.isAdmin(request)) {
            throw new ForbiddenException(
                            "Pool info only accessible to admin users.");
        }

        return new Pool(pool, poolMonitor.getPoolSelectionUnit());
    }

    @GET
    @Path("/{pool}/usage")
    @Produces(MediaType.APPLICATION_JSON)
    public PoolInfo getPoolUsage(@PathParam("pool") String pool) {
        if (!HttpServletRequests.isAdmin(request)) {
            throw new ForbiddenException(
                            "Pool info only accessible to admin users.");
        }

        PoolInfo info = new PoolInfo();

        service.getDiagnosticInfo(pool, info);

        return info;
    }

    @GET
    @Path("/{pool}/{pnfsid}")
    @Produces(MediaType.APPLICATION_JSON)
    public PoolInfo getRepositoryInfoForFile(@PathParam("pool") String pool,
                                             @PathParam("pnfsid") PnfsId pnfsid) {
        if (!HttpServletRequests.isAdmin(request)) {
            throw new ForbiddenException(
                            "Repository info for file only accessible to admin users.");
        }

        PoolInfo info = new PoolInfo();

        service.getCacheInfo(pool, pnfsid, info);

        return info;
    }

    @GET
    @Path("/{pool}/histograms/queues")
    @Produces(MediaType.APPLICATION_JSON)
    public PoolInfo getQueueHistograms(@PathParam("pool") String group) {
        PoolInfo info = new PoolInfo();

        service.getQueueStat(group, info);

        return info;
    }

    @GET
    @Path("/{pool}/histograms/files")
    @Produces(MediaType.APPLICATION_JSON)
    public PoolInfo getFilesHistograms(@PathParam("pool") String group) {
        PoolInfo info = new PoolInfo();

        service.getFileStat(group, info);

        return info;
    }

    @GET
    @Path("/{pool}/movers")
    @Produces(MediaType.APPLICATION_JSON)
    public List<MoverData> getMovers(@PathParam("pool") String pool,
                                     @QueryParam("type") String typeList,
                                     @DefaultValue("0")
                                     @QueryParam("offset") int  offset,
                                     @QueryParam("limit") Integer limit,
                                     @QueryParam("pnfsid") String pnfsid,
                                     @QueryParam("queue") String queue,
                                     @QueryParam("state") String state,
                                     @QueryParam("mode") String mode,
                                     @QueryParam("door") String door,
                                     @QueryParam("storageClass") String storageClass,
                                     @QueryParam("sort") String sort) {
        if (!HttpServletRequests.isAdmin(request)) {
            throw new ForbiddenException(
                            "Mover info only accessible to admin users.");
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
            throw new InternalServerErrorException(e);
        }

        String error = String.format(TYPE_ERROR, typeList);

        throw new InternalServerErrorException(error, NOT_IMPLEMENTED);
    }

    @GET
    @Path("/{pool}/nearline/queues")
    @Produces(MediaType.APPLICATION_JSON)
    public List<NearlineData> getNearlineQueues(@PathParam("pool") String pool,
                                                @QueryParam("type") String typeList,
                                                @DefaultValue("0")
                                                @QueryParam("offset") int  offset,
                                                @QueryParam("limit") Integer limit,
                                                @QueryParam("pnfsid") String pnfsid,
                                                @QueryParam("state") String state,
                                                @QueryParam("storageClass") String storageClass,
                                                @QueryParam("sort") String sort) {
        if (!HttpServletRequests.isAdmin(request)) {
            throw new ForbiddenException(
                            "Flush info only accessible to admin users.");
        }

        limit = limit == null ? Integer.MAX_VALUE : limit;

        List<NearlineData> list = new ArrayList<>();

        PagedList<NearlineData> pagedList;
        int count = 0;

        try {
            String[] types = typeList == null ? new String[0] :
                             typeList.split(",");
            for (String type: types) {
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
            return list;
        } catch (InterruptedException | NoRouteToCellException | CacheException e) {
            throw new InternalServerErrorException(e);
        }
    }

    @DELETE
    @Path("/{pool}/movers/{id : [0-9]+}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response killMovers(@PathParam("pool") String pool,
                               @PathParam("id") int id ) {
        if (!HttpServletRequests.isAdmin(request)) {
            throw new ForbiddenException(
                            "Pool command only accessible to admin users.");
        }

        try {
            poolStub.sendAndWait(new CellPath(pool),
                                 new PoolMoverKillMessage(pool, id,
                                    "Killed by user."));
        } catch (IllegalArgumentException e) {
            throw new BadRequestException(e);
        } catch (InterruptedException | NoRouteToCellException | CacheException e) {
            throw new InternalServerErrorException(e);
        }

        return successfulResponse(Response.Status.OK);
    }

    @PATCH
    @Path("/{pool}/usage/mode")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateMode(@PathParam("pool") String pool,
                               String requestPayload) {
        if (!HttpServletRequests.isAdmin(request)) {
            throw new ForbiddenException(
                            "Pool command only accessible to admin users.");
        }

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
            throw new InternalServerErrorException(e);
        }

        return successfulResponse(Response.Status.OK);
    }
}
