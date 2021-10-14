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
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.dcache.pool.statistics.StorageUnitSpaceStatistics;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.restful.providers.pool.PoolGroupInfo;
import org.dcache.restful.providers.selection.PoolGroup;
import org.dcache.restful.services.pool.PoolInfoService;
import org.springframework.stereotype.Component;

/**
 * <p>RESTful API to the {@link PoolInfoService} service.</p>
 *
 * @version v1.0
 */
@Component
@Api(value = "poolmanager", authorizations = {@Authorization("basicAuth")})
@Path("/poolgroups")
public final class PoolGroupInfoResources {

    @Inject
    private PoolInfoService service;

    @Inject
    private PoolMonitor poolMonitor;

    @GET
    @ApiOperation("Get a list of poolgroups."
          + " Results sorted lexicographically by group name.")
    @Produces(MediaType.APPLICATION_JSON)
    public List<PoolGroup> getPoolGroups() {
        PoolSelectionUnit psu = poolMonitor.getPoolSelectionUnit();

        return psu.getPoolGroups().values()
              .stream()
              .sorted(Comparator.comparing(SelectionPoolGroup::getName))
              .map((g) -> new PoolGroup(g.getName(), psu))
              .collect(Collectors.toList());
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @ApiOperation("Get information about a poolgroup.")
    @Path("/{group}")
    public PoolGroup getPoolGroup(@ApiParam("The poolgroup to be described.")
    @PathParam("group") String group) {
        return new PoolGroup(group, poolMonitor.getPoolSelectionUnit());
    }

    @GET
    @Path("/{group}/pools")
    @ApiOperation("Get a list of pools that are a member of a poolgroup.  If no "
          + "poolgroup is specified then all pools are listed. "
          + "Results sorted lexicographically by pool name.")
    @Produces(MediaType.APPLICATION_JSON)
    public String[] getPoolsOfGroup(@ApiParam("The poolgroup to be described.")
    @PathParam("group") String group) {
        if (group == null) {
            return service.listPools();
        }

        return service.listPools(group);
    }


    @GET
    @ApiOperation("Get usage metadata about a specific poolgroup.")
    @Path("/{group}/usage")
    @Produces(MediaType.APPLICATION_JSON)
    public PoolGroupInfo getGroupUsage(@ApiParam("The poolgroup to be described.")
    @PathParam("group") String group) {
        PoolGroupInfo info = new PoolGroupInfo();

        service.getGroupCellInfos(group, info);

        return info;
    }


    @GET
    @ApiOperation("Get pool activity information about pools of a specific poolgroup.")
    @Path("/{group}/queues")
    @Produces(MediaType.APPLICATION_JSON)
    public PoolGroupInfo getQueueInfo(@ApiParam("The poolgroup to be described.")
    @PathParam("group") String group) {
        PoolGroupInfo info = new PoolGroupInfo();

        service.getGroupQueueInfos(group, info);

        return info;
    }


    @GET
    @ApiOperation("Get space information about pools of a specific poolgroup.")
    @Path("/{group}/space")
    @Produces(MediaType.APPLICATION_JSON)
    public PoolGroupInfo getSpaceInfo(@ApiParam("The poolgroup to be described.")
    @PathParam("group") String group) {
        PoolGroupInfo info = new PoolGroupInfo();

        service.getGroupSpaceInfos(group, info);

        return info;
    }

    @ApiResponses({@ApiResponse(code = 404, message = "Not Found")})
    @GET
    @ApiOperation("Get the storage units linked to pools of a specific poolgroup.")
    @Path("/{group}/storageunits")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> getStorageGroups(@ApiParam("The poolgroup to be described.")
    @PathParam("group") String group,
          @ApiParam("Return full storage unit string; "
                + "otherwise, just the storage class")
          @DefaultValue("false")
          @QueryParam("useUnits") boolean useUnits) {
        Map<String, StorageUnitSpaceStatistics> map = getSpaceByStorageUnits(group, useUnits);
        if (map == null) {
            throw new NotFoundException("no such group: " + group);
        }

        return map.keySet().stream().collect(Collectors.toList());
    }

    @ApiResponses({@ApiResponse(code = 404, message = "Not Found")})
    @GET
    @ApiOperation("Get space information about the storage units linked to pools of a specific poolgroup.")
    @Path("/{group}/storageunits/space")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, StorageUnitSpaceStatistics> getStorageGroupSpaceInfo(
          @ApiParam("The poolgroup to be described.")
          @PathParam("group") String group,
          @ApiParam("Return full storage unit string as keys; "
                + "otherwise, just the storage class")
          @DefaultValue("false")
          @QueryParam("useUnits") boolean useUnits) {
        return getSpaceByStorageUnits(group, useUnits);
    }

    @ApiResponses({@ApiResponse(code = 404, message = "Not Found")})
    @GET
    @ApiOperation("Get space information about a given storage unit linked to pools of a specific poolgroup.")
    @Path("/{group}/storageunits/{key}/space")
    @Produces(MediaType.APPLICATION_JSON)
    public StorageUnitSpaceStatistics getStorageGroupSpaceInfo(
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
        return statistics;
    }

    @GET
    @ApiOperation("Get aggregated pool activity histogram information from pools in a specific poolgroup.")
    @Path("/{group}/histograms/queues")
    @Produces(MediaType.APPLICATION_JSON)
    public PoolGroupInfo getQueueHistograms(@ApiParam("The poolgroup to be described.")
    @PathParam("group") String group) {
        PoolGroupInfo info = new PoolGroupInfo();

        service.getQueueStat(group, info);

        return info;
    }


    @GET
    @ApiOperation("Get aggregated file statistics histogram information from pools in a specific poolgroup.")
    @Path("/{group}/histograms/files")
    @Produces(MediaType.APPLICATION_JSON)
    public PoolGroupInfo getFilesHistograms(@ApiParam("The poolgroup to be described.")
    @PathParam("group") String group) {
        PoolGroupInfo info = new PoolGroupInfo();

        service.getFileStat(group, info);

        return info;
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
