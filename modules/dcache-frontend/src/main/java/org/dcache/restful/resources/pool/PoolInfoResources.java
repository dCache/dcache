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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONException;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Named;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.IOException;

import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PoolModifyModeMessage;
import diskCacheV111.vehicles.PoolMoverKillMessage;

import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.CellStub;
import org.dcache.restful.providers.pool.PoolGroupInfo;
import org.dcache.restful.providers.pool.PoolInfo;
import org.dcache.restful.providers.pool.PoolKillMover;
import org.dcache.restful.providers.pool.PoolModeUpdate;
import org.dcache.restful.services.pool.PoolInfoService;
import org.dcache.restful.util.HttpServletRequests;

import static org.dcache.restful.providers.SuccessfulResponse.successfulResponse;

/**
 * <p>RESTful API to the {@link PoolInfoService} service.</p>
 *
 * @version v1.0
 */
@Component
@Path("/pools")
public final class PoolInfoResources {
    @Context
    private HttpServletRequest request;

    @Inject
    private PoolInfoService service;

    @Inject
    @Named("pool-stub")
    private CellStub poolStub;

    /**
     * @return a list of pool group names
     * @throws CacheException
     */
    @GET
    @Path("/groups")
    @Produces(MediaType.APPLICATION_JSON)
    public String[] getGroups() throws CacheException {
        /*
         *  Allow pools and pool groups to be listed without privileges.
         */
        return service.listPools();
    }

    /**
     * <p>Request for data aggregated by pool group.</p>
     *
     * @param groupName   of pool group; 'ALL'/'all' is reserved to mean all pools
     * @param isFilestat  if true, include file lifetime histograms
     * @param isQueuestat if true, include queue histograms
     * @return the object populated with lists of the requested histograms
     * @throws CacheException
     */
    @GET
    @Path("/groups/{name : .*}")
    @Produces(MediaType.APPLICATION_JSON)
    public PoolGroupInfo getPoolGroupInfo(@PathParam("name") String groupName,
                                          @DefaultValue("false")
                                          @QueryParam("cellinfo") boolean isCellInfo,
                                          @DefaultValue("false")
                                          @QueryParam("queueinfo") boolean isQueueInfo,
                                          @DefaultValue("false")
                                          @QueryParam("spaceinfo") boolean isSpaceInfo,
                                          @DefaultValue("false")
                                          @QueryParam("filestat") boolean isFilestat,
                                          @DefaultValue("false")
                                          @QueryParam("queuestat") boolean isQueuestat)
                    throws CacheException {
        /*
         *  Allow access to aggregated data to be seen without privileges
         */
        PoolGroupInfo info = new PoolGroupInfo();

        /*
         *  Allow histograms to be seen without privileges
         */
        if (isFilestat) {
            service.getFileStat(groupName, info);
        }

        if (isQueuestat) {
            service.getQueueStat(groupName, info);
        }

        boolean isAdmin = HttpServletRequests.isAdmin(request);

        if (isCellInfo) {
            if (!isAdmin) {
                throw new ForbiddenException(
                                "Cell Info accessible to admin users.");
            }
            service.getGroupCellInfos(groupName, info);
        }

        if (isQueueInfo) {
            if (!isAdmin) {
                throw new ForbiddenException(
                                "Cell Info accessible to admin users.");
            }
            service.getGroupQueueInfos(groupName, info);
        }

        if (isSpaceInfo) {
            if (!isAdmin) {
                throw new ForbiddenException(
                                "Cell Info accessible to admin users.");
            }
            service.getGroupSpaceInfos(groupName, info);
        }

        return info;
    }

    /**
     * <p>Main request for pool info.  The returned object is populated
     * according to the parameter options.</p>
     *
     * @param poolName    the pool in question
     * @param pnfsid      if not <code>null</code>, include repository info for
     *                    this file
     * @param isInfo      if true, include extended (diagnostic) cell info
     * @param isFilestat  if true, include file lifetime histograms
     * @param isQueuestat if true, include queue histograms
     * @param isMovers    if true, include list of movers (excluding p2ps)
     * @param isP2ps      if true, include list of server and client p2ps
     * @param isFlush     if true, include list of store/flush requests
     * @param isStage     if true, include list of restore/stage requests
     * @param isRemove    if true, include list of remove requests
     * @return the info object populated with the requested data
     * @throws CacheException
     */
    @GET
    @Path("{name : .*}")
    @Produces(MediaType.APPLICATION_JSON)
    public PoolInfo getPoolInfo(
                    @PathParam("name") String poolName,
                    @QueryParam("pnfsid") PnfsId pnfsid,
                    @DefaultValue("false")
                    @QueryParam("info") boolean isInfo,
                    @DefaultValue("false")
                    @QueryParam("filestat") boolean isFilestat,
                    @DefaultValue("false")
                    @QueryParam("queuestat") boolean isQueuestat,
                    @DefaultValue("false")
                    @QueryParam("movers") boolean isMovers,
                    @DefaultValue("false")
                    @QueryParam("p2ps") boolean isP2ps,
                    @DefaultValue("false")
                    @QueryParam("flush") boolean isFlush,
                    @DefaultValue("false")
                    @QueryParam("stage") boolean isStage,
                    @DefaultValue("false")
                    @QueryParam("remove") boolean isRemove)
                    throws CacheException {
        PoolInfo info = new PoolInfo();

        /*
         *  Allow histograms to be seen without privileges
         */
        if (isFilestat) {
            service.getFileStat(poolName, info);
        }

        if (isQueuestat) {
            service.getQueueStat(poolName, info);
        }

        boolean isAdmin = HttpServletRequests.isAdmin(request);

        if (isInfo) {
            if (!isAdmin) {
                throw new ForbiddenException(
                                "Pool info only accessible to admin users.");
            }
            service.getDiagnosticInfo(poolName, info);
        }

        if (pnfsid != null) {
            if (!isAdmin) {
                throw new ForbiddenException(
                                "Repository info only accessible to admin users.");
            }
            service.getCacheInfo(poolName, pnfsid, info);
        }

        if (isMovers) {
            if (!isAdmin) {
                throw new ForbiddenException(
                                "Mover info only accessible to admin users.");
            }
            service.getMovers(poolName, info);
        }

        if (isP2ps) {
            if (!isAdmin) {
                throw new ForbiddenException(
                                "P2P info only accessible to admin users.");
            }
            service.getP2p(poolName, info);
        }

        if (isFlush) {
            if (!isAdmin) {
                throw new ForbiddenException(
                                "Flush info only accessible to admin users.");
            }
            service.getFlush(poolName, info);
        }

        if (isStage) {
            if (!isAdmin) {
                throw new ForbiddenException(
                                "Stage info only accessible to admin users.");
            }
            service.getStage(poolName, info);
        }

        if (isRemove) {
            if (!isAdmin) {
                throw new ForbiddenException(
                                "Remove info only accessible to admin users.");
            }
            service.getRemove(poolName, info);
        }

        return info;
    }

    /**
     * <p>Get a list of current pools.</p>
     *
     * @param group limit the list to this pool group
     * @return names of pools in system, or in the specified group, if specified
     * @throws CacheException
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String[] getPools(@QueryParam("group") String group)
                    throws CacheException {
        /*
         *  Allow pools and pool groups to be listed without privileges.
         */
        if (group == null) {
            return service.listPools();
        }

        return service.listPools(group);
    }

    /**
     * <p>Request to update the indicated pool to either enabled or disabled.</p>
     *
     * @param requestPayload containing the object with updated fields.
     */
    @POST
    @Path("movers")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response killMovers(String requestPayload) throws
                    CacheException {
        if (!HttpServletRequests.isAdmin(request)) {
            throw new ForbiddenException(
                            "Pool command only accessible to admin users.");
        }

        try {
            PoolKillMover update = new ObjectMapper().readValue(requestPayload,
                                                                PoolKillMover.class);
            String pool = update.getPool();
            Message message;
            Integer moverId = update.getMoverId();
            if (moverId != null) {
                message = new PoolMoverKillMessage(pool, moverId,
                                                   update.getReason());
                poolStub.sendAndWait(new CellPath(pool), message);
            }
        } catch (JSONException | IllegalArgumentException | JsonParseException
                        | JsonMappingException e) {
            throw new BadRequestException(e);
        } catch (IOException | InterruptedException | NoRouteToCellException e) {
            throw new InternalServerErrorException(e);
        }

        return successfulResponse(Response.Status.OK);
    }

    /**
     * <p>Request to update the indicated pool to either enabled or disabled.</p>
     *
     * @param requestPayload containing the object with updated fields.
     */
    @POST
    @Path("mode")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateMode(String requestPayload) throws
                    CacheException {
        if (!HttpServletRequests.isAdmin(request)) {
            throw new ForbiddenException(
                            "Pool command only accessible to admin users.");
        }

        try {
            PoolModeUpdate update
                            = new ObjectMapper().readValue(requestPayload,
                                                           PoolModeUpdate.class);
            String pool = update.getPool();
            PoolV2Mode mode = new PoolV2Mode(update.mode());
            mode.setResilienceEnabled(update.isResilience());
            Message message = new PoolModifyModeMessage(pool, mode);
            poolStub.sendAndWait(new CellPath(pool), message);
        } catch (JSONException | IllegalArgumentException | JsonParseException
                        | JsonMappingException e) {
            throw new BadRequestException(e);
        } catch (IOException | InterruptedException | NoRouteToCellException e) {
            throw new InternalServerErrorException(e);
        }

        return successfulResponse(Response.Status.OK);
    }
}
