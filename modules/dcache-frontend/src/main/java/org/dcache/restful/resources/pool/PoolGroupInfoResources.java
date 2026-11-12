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

import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.util.CacheException;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.CommandThrowableException;
import dmg.util.Exceptions;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.dcache.cells.CellStub;
import org.dcache.pool.statistics.StorageUnitSpaceStatistics;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.restful.providers.pool.PoolGroupInfo;
import org.dcache.restful.providers.selection.PoolGroup;
import org.dcache.restful.services.pool.PoolInfoService;
import org.dcache.restful.util.RequestUser;
import org.dcache.restful.util.Responses;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import static org.dcache.restful.providers.SuccessfulResponse.successfulResponse;

/**
 * <p>RESTful API to the {@link PoolInfoService} service.</p>
 *
 * @version v1.0
 */
@Component
@Api(value = "poolmanager", authorizations = {@Authorization("basicAuth")})
@Path("/poolgroups")
public final class PoolGroupInfoResources {

    private static final Logger LOGGER = LoggerFactory.getLogger(PoolGroupInfoResources.class);

    private static final Pattern VALID_TAG = Pattern.compile("([A-Za-z0-9_.-]+)=[A-Za-z0-9_.-]+");

    @Inject
    private PoolInfoService service;

    @Inject
    private PoolMonitor poolMonitor;

    @Inject
    @Named("pool-manager-stub")
    private CellStub poolManagerStub;

    @GET
    @ApiOperation("Get a list of poolgroups."
          + " Results sorted lexicographically by group name.")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPoolGroups() {
        PoolSelectionUnit psu = poolMonitor.getPoolSelectionUnit();

        List<PoolGroup> result = psu.getPoolGroups().values()
              .stream()
              .sorted(Comparator.comparing(SelectionPoolGroup::getName))
              .map((g) -> new PoolGroup(g.getName(), psu))
              .collect(Collectors.toList());
        return Responses.buildResponse(result, service.getLastUpdated());
    }

    @POST
    @ApiOperation("add pool group.")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response addPoolGroup(@ApiParam("JSON Object describing the pool group: " +
            "name (required) " +
            "primary, dynamic (boolean, default false) " +
            "tags (list of strings) ") String requestPayload) {

        if (!RequestUser.isAdmin()) {
            throw new ForbiddenException("Pool command only accessible to admin users.");
        }

        String name;
        try {
            JSONObject payload = new JSONObject(requestPayload);
            name = payload.getString("name");
            String primary = payload.optBoolean("primary") ? " -primary" : "";
            String dynamic = payload.optBoolean("dynamic") ? " -dynamic" : "";
            JSONArray tagArray = payload.optJSONArray("tags");
            validateTags(tagArray);
            String tags = (tagArray == null) ? ""
                    : IntStream.range(0, tagArray.length())
                    .mapToObj(tagArray::getString)
                    .collect(Collectors.joining(","));
            String tagsCommand = (tags.isEmpty()) ? tags : " -tags=" + tags;
            String adminCommand = "psu create pgroup " + name + primary + dynamic
                    + tagsCommand;
            poolManagerStub.sendAndWait(adminCommand, String.class);
        } catch (JSONException | IllegalArgumentException e) {
            throw new BadRequestException(e);
        } catch (CacheException e) {
            if (e.getCause() instanceof CommandThrowableException
                    && ((CommandThrowableException) e.getCause()).getTargetException()
                    instanceof IllegalArgumentException) {
                throw new ClientErrorException(Response.Status.CONFLICT);
            }
            LOGGER.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        } catch (InterruptedException | NoRouteToCellException e) {
            LOGGER.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        }
        return saveAndRespond("Pool group: " + name + " was created", Response.Status.CREATED);
    }

    @DELETE
    @ApiOperation("remove pool group.")
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{group}")
    public Response removePoolGroup(
            @ApiParam(value = "name of pool group to be removed")
            @PathParam("group") String group) {

        if (!RequestUser.isAdmin()) {
            throw new ForbiddenException("Pool command only accessible to admin users.");
        }

        try{
            poolManagerStub.sendAndWait("psu remove pgroup " + group, String.class);
        } catch (JSONException | IllegalArgumentException e) {
            throw new BadRequestException(e);
        } catch (CacheException e) {
            throwIfClientError(e);
            LOGGER.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        } catch (InterruptedException | NoRouteToCellException e) {
            LOGGER.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        }
        return saveAndRespond("Pool group: " + group + " was removed.",
                Response.Status.OK);
    }

    @PUT
    @ApiOperation("add pool to group.")
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{group}/pools/{pool}")
    public Response addPoolToGroup(
            @ApiParam(value = "The pool group the pool should be added to.", required = true)
            @PathParam("group") String group,
            @ApiParam(value= "The pool to be added.", required = true)
            @PathParam("pool") String pool){

        if (!RequestUser.isAdmin()) {
            throw new ForbiddenException("Pool command only accessible to admin users.");
        }

        try{
            poolManagerStub.sendAndWait("psu addto pgroup " + group + " " + pool, String.class);
        } catch (JSONException | IllegalArgumentException e) {
            throw new BadRequestException(e);
        } catch (CacheException e) {
            throwIfClientError(e);
            LOGGER.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        } catch (InterruptedException | NoRouteToCellException e) {
            LOGGER.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        }
        return saveAndRespond("Pool: " + pool + " was added to pool group: " + group,
                Response.Status.OK);
    }

    @DELETE
    @ApiOperation("remove pool from pool group.")
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{group}/pools/{pool}")
    public Response removePoolFromGroup(
            @ApiParam(value = "The pool group the pool should be removed from.", required = true)
            @PathParam("group") String group,
            @ApiParam(value= "The pool to be removed.", required = true)
            @PathParam("pool") String pool){

        if (!RequestUser.isAdmin()) {
            throw new ForbiddenException("Pool command only accessible to admin users.");
        }

        try{
            poolManagerStub.sendAndWait("psu removefrom pgroup " + group + " " + pool, String.class);
        } catch (JSONException | IllegalArgumentException e) {
            throw new BadRequestException(e);
        } catch (CacheException e) {
            throwIfClientError(e);
            LOGGER.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        } catch (InterruptedException | NoRouteToCellException e) {
            LOGGER.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        }
        return saveAndRespond("Pool: " + pool + " was removed from pool group: " + group,
                Response.Status.OK);
    }

    private static void validateTags(JSONArray tagArray) {
        if (tagArray == null) {
            return;
        }
        Set<String> keys = new HashSet<>();
        for (int i = 0; i < tagArray.length(); i++) {
            String tag = tagArray.getString(i);
            Matcher matcher = VALID_TAG.matcher(tag);
            if (!matcher.matches()) {
                throw new BadRequestException("Invalid tag (expected key=value): " + tag);
            }
            if (!keys.add(matcher.group(1))) {
                throw new BadRequestException("Duplicate tag key: " + matcher.group(1));
            }
        }
    }

    private static void throwIfClientError(CacheException e) {
        if (!(e.getCause() instanceof CommandThrowableException)) {
            return;
        }
        Throwable target = ((CommandThrowableException) e.getCause()).getTargetException();
        if (!(target instanceof IllegalArgumentException)) {
            return;
        }
        String message = String.valueOf(target.getMessage());
        if (message.contains("Not found") || message.contains("not found")
                || message.contains("not member of") || message.contains("not a subgroup of")) {
            throw new NotFoundException(message);
        }
        throw new ClientErrorException(Response.Status.CONFLICT);
    }

    private Response saveAndRespond(String action, Response.Status status) {
        try {
            poolManagerStub.sendAndWait("save", String.class);
        } catch (CacheException | NoRouteToCellException | InterruptedException e) {
            LOGGER.warn("{} but PoolManager configuration could not be saved: {}",
                    action, Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(
                    action + " but configuration could not be saved");
        }
        return successfulResponse(status);
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation("Get information about a poolgroup.")
    @Path("/{group}")
    public Response getPoolGroup(@ApiParam("The poolgroup to be described.")
    @PathParam("group") String group) {
        PoolGroup result = new PoolGroup(group, poolMonitor.getPoolSelectionUnit());
        return Responses.buildResponse(result, service.getLastUpdated());
    }

    @GET
    @Path("/{group}/pools")
    @ApiOperation("Get a list of pools that are a member of a poolgroup.  If no "
          + "poolgroup is specified then all pools are listed. "
          + "Results sorted lexicographically by pool name.")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPoolsOfGroup(@ApiParam("The poolgroup to be described.")
    @PathParam("group") String group) {
        String[] result = (group == null) ? service.listPools() : service.listPools(group);
        return Responses.buildResponse(result, service.getLastUpdated());
    }


    @GET
    @ApiOperation("Get usage metadata about a specific poolgroup.")
    @Path("/{group}/usage")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getGroupUsage(@ApiParam("The poolgroup to be described.")
    @PathParam("group") String group) {
        PoolGroupInfo info = new PoolGroupInfo();

        service.getGroupCellInfos(group, info);

        return Responses.buildResponse(info, service.getLastUpdated());
    }


    @GET
    @ApiOperation("Get pool activity information about pools of a specific poolgroup.")
    @Path("/{group}/queues")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getQueueInfo(@ApiParam("The poolgroup to be described.")
    @PathParam("group") String group) {
        PoolGroupInfo info = new PoolGroupInfo();

        service.getGroupQueueInfos(group, info);

        return Responses.buildResponse(info, service.getLastUpdated());
    }


    @GET
    @ApiOperation("Get space information about pools of a specific poolgroup.")
    @Path("/{group}/space")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSpaceInfo(@ApiParam("The poolgroup to be described.")
    @PathParam("group") String group) {
        PoolGroupInfo info = new PoolGroupInfo();

        service.getGroupSpaceInfos(group, info);

        return Responses.buildResponse(info, service.getLastUpdated());
    }

    @ApiResponses({@ApiResponse(code = 404, message = "Not Found")})
    @GET
    @ApiOperation("Get the storage units linked to pools of a specific poolgroup.")
    @Path("/{group}/storageunits")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getStorageGroups(@ApiParam("The poolgroup to be described.")
    @PathParam("group") String group,
          @ApiParam("Return full storage unit string; "
                + "otherwise, just the storage class")
          @DefaultValue("false")
          @QueryParam("useUnits") boolean useUnits) {
        Map<String, StorageUnitSpaceStatistics> map = getSpaceByStorageUnits(group, useUnits);
        if (map == null) {
            throw new NotFoundException("no such group: " + group);
        }

        List<String> result = map.keySet().stream().collect(Collectors.toList());
        return Responses.buildResponse(result, service.getLastUpdated());
    }

    @ApiResponses({@ApiResponse(code = 404, message = "Not Found")})
    @GET
    @ApiOperation("Get space information about the storage units linked to pools of a specific poolgroup.")
    @Path("/{group}/storageunits/space")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getStorageGroupSpaceInfo(
          @ApiParam("The poolgroup to be described.")
          @PathParam("group") String group,
          @ApiParam("Return full storage unit string as keys; "
                + "otherwise, just the storage class")
          @DefaultValue("false")
          @QueryParam("useUnits") boolean useUnits) {
        Map<String, StorageUnitSpaceStatistics> result = getSpaceByStorageUnits(group, useUnits);
        return Responses.buildResponse(result, service.getLastUpdated());
    }

    @ApiResponses({@ApiResponse(code = 404, message = "Not Found")})
    @GET
    @ApiOperation("Get space information about a given storage unit linked to pools of a specific poolgroup.")
    @Path("/{group}/storageunits/{key}/space")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getStorageGroupSpaceInfo(
          @ApiParam("The poolgroup to be described.")
          @PathParam("group") String group,
          @ApiParam("The storage unit to be described.")
          @PathParam("key") String key,
          @ApiParam("Key expresses full storage unit string; "
                + "otherwise, just the storage class")
          @DefaultValue("false")
          @QueryParam("useUnits") boolean useUnits) {
        StorageUnitSpaceStatistics statistics = getSpaceByStorageUnits(group, useUnits).get(key);
        if (statistics == null) {
            throw new NotFoundException("no such storage key: " + key);
        }
        return Responses.buildResponse(statistics, service.getLastUpdated());
    }

    @GET
    @ApiOperation("Get aggregated pool activity histogram information from pools in a specific poolgroup.")
    @Path("/{group}/histograms/queues")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getQueueHistograms(@ApiParam("The poolgroup to be described.")
    @PathParam("group") String group) {
        PoolGroupInfo info = new PoolGroupInfo();

        service.getQueueStat(group, info);

        return Responses.buildResponse(info, service.getLastUpdated());
    }


    @GET
    @ApiOperation("Get aggregated file statistics histogram information from pools in a specific poolgroup.")
    @Path("/{group}/histograms/files")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getFilesHistograms(@ApiParam("The poolgroup to be described.")
    @PathParam("group") String group) {
        PoolGroupInfo info = new PoolGroupInfo();

        service.getFileStat(group, info);

        return Responses.buildResponse(info, service.getLastUpdated());
    }

    private Map<String, StorageUnitSpaceStatistics> getSpaceByStorageUnits(String group,
          boolean units) {
        PoolGroupInfo info = new PoolGroupInfo();
        service.getStorageGroupSpaceInfosOfPoolGroup(group, info);
        Map<String, StorageUnitSpaceStatistics> map = info.getSpaceDataByStorageUnit();
        if (map == null) {
            throw new NotFoundException("no such group: " + group);
        }
        if (!units) {
            map = service.mapToStorageClass(map);
        }
        return map;
    }
}
