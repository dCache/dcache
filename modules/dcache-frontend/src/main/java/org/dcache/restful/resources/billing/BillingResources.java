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
package org.dcache.restful.resources.billing;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.PnfsId;

import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.restful.providers.PagedList;
import org.dcache.restful.providers.billing.BillingDataGrid;
import org.dcache.restful.providers.billing.BillingDataGridEntry;
import org.dcache.restful.providers.billing.DoorTransferRecord;
import org.dcache.restful.providers.billing.HSMTransferRecord;
import org.dcache.restful.providers.billing.P2PTransferRecord;
import org.dcache.restful.services.billing.BillingInfoService;
import org.dcache.util.histograms.Histogram;

import static org.dcache.restful.providers.PagedList.TOTAL_COUNT_HEADER;

/**
 * <p>RestFul API for providing billing records and histograms.</p>
 */
@Component
@Api(value = "billing", authorizations = {@Authorization("basicAuth")})
@Path("/billing")
public class BillingResources {
    @Inject
    private BillingInfoService service;

    @Context
    private HttpServletResponse response;


    @GET
    @ApiOperation("Provides a list of read transfers for a specific PNFS-ID.")
    @ApiResponses({
                @ApiResponse(code = 400, message = "Bad request"),
                @ApiResponse(code = 403, message = "Billing records are only available to admin users."),
                @ApiResponse(code = 404, message = "Not Found"),
                @ApiResponse(code = 500, message = "Internal Server Error"),
            })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("reads/{pnfsid}")
    public List<DoorTransferRecord> getReads(@ApiParam("The file to list.")
                                             @PathParam("pnfsid") PnfsId pnfsid,
                                             @ApiParam("Return no reads after this datestamp.")
                                             @QueryParam("before") String before,
                                             @ApiParam("Return no reads before this datestamp.")
                                             @QueryParam("after") String after,
                                             @ApiParam("Maximum number of reads to return.")
                                             @QueryParam("limit") Integer limit,
                                             @ApiParam("Number of reads to skip.")
                                             @DefaultValue("0")
                                             @QueryParam("offset") Integer offset,
                                             @ApiParam("Only select reads from the specified pool.")
                                             @QueryParam("pool") String pool,
                                             @ApiParam("Only select reads initiated by the specified door.")
                                             @QueryParam("door") String door,
                                             @ApiParam("Only select reads requested by the client.")
                                             @QueryParam("client") String client,
                                             @ApiParam("How to sort responses.")
                                             @DefaultValue("date")
                                             @QueryParam("sort") String sort) {
        try {
            limit = limit == null ? Integer.MAX_VALUE: limit;

            PagedList<DoorTransferRecord> result = service.getReads(pnfsid,
                                                                    before,
                                                                    after,
                                                                    limit,
                                                                    offset,
                                                                    pool,
                                                                    door,
                                                                    client,
                                                                    sort);
            response.addIntHeader(TOTAL_COUNT_HEADER, result.total);
            return result.contents;
        } catch (FileNotFoundCacheException e) {
            throw new NotFoundException(e);
        } catch (NoRouteToCellException | InterruptedException | CacheException e) {
            throw new InternalServerErrorException(e);
        } catch (IllegalArgumentException | ParseException e) {
            throw new BadRequestException(e.getMessage(), e);
        }
    }


    @GET
    @ApiOperation("Provides a list of write transfers for a specific PNFS-ID.")
    @ApiResponses({
                @ApiResponse(code = 400, message = "Bad request"),
                @ApiResponse(code = 403, message = "Billing records are only available to admin users."),
                @ApiResponse(code = 404, message = "Not Found"),
                @ApiResponse(code = 500, message = "Internal Server Error"),
            })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("writes/{pnfsid}")
    public List<DoorTransferRecord> getWrites(@ApiParam("The file to list.")
                                              @PathParam("pnfsid") PnfsId pnfsid,
                                              @ApiParam("Return no writes after this datestamp.")
                                              @QueryParam("before") String before,
                                              @ApiParam("Return no writes before this datestamp.")
                                              @QueryParam("after") String after,
                                              @ApiParam("Maximum number of writes to return.")
                                              @QueryParam("limit") Integer limit,
                                              @ApiParam("Number of writes to skip.")
                                              @DefaultValue("0")
                                              @QueryParam("offset") int offset,
                                              @ApiParam("Only select writes from the specified pool.")
                                              @QueryParam("pool") String pool,
                                              @ApiParam("Only select writes initiated by the specified door.")
                                              @QueryParam("door") String door,
                                              @ApiParam("Only select writes requested by the client.")
                                              @QueryParam("client") String client,
                                              @ApiParam("How to sort responses.")
                                              @DefaultValue("date")
                                              @QueryParam("sort") String sort) {
        try {
            limit = limit == null ? Integer.MAX_VALUE: limit;

            PagedList<DoorTransferRecord> result = service.getWrites(pnfsid,
                                                                     before,
                                                                     after,
                                                                     limit,
                                                                     offset,
                                                                     pool,
                                                                     door,
                                                                     client,
                                                                     sort);
            response.addIntHeader(TOTAL_COUNT_HEADER, result.total);
            return result.contents;
        } catch (FileNotFoundCacheException e) {
            throw new NotFoundException(e);
        } catch (NoRouteToCellException | InterruptedException | CacheException e) {
            throw new InternalServerErrorException(e);
        } catch (IllegalArgumentException | ParseException e) {
            throw new BadRequestException(e.getMessage(), e);
        }
    }


    @GET
    @ApiOperation("Provides a list of pool-to-pool transfers for a specific "
            + "PNFS-ID.")
    @ApiResponses({
                @ApiResponse(code = 400, message = "Bad request"),
                @ApiResponse(code = 403, message = "Billing records are only available to admin users."),
                @ApiResponse(code = 404, message = "Not Found"),
                @ApiResponse(code = 500, message = "Internal Server Error"),
            })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("p2ps/{pnfsid}")
    public List<P2PTransferRecord> getP2ps(@ApiParam("The file to list.")
                                           @PathParam("pnfsid") PnfsId pnfsid,
                                           @ApiParam("Return no transfers after this datestamp.")
                                           @QueryParam("before") String before,
                                           @ApiParam("Return no transfers before this datestamp.")
                                           @QueryParam("after") String after,
                                           @ApiParam("Maximum number of transfers to return.")
                                           @QueryParam("limit") Integer limit,
                                           @ApiParam("Number of transfers to skip.")
                                           @DefaultValue("0")
                                           @QueryParam("offset") int offset,
                                           @ApiParam("Only select transfers from the specified pool.")
                                           @QueryParam("serverPool") String serverPool,
                                           @ApiParam("Only select transfers to the specified pool.")
                                           @QueryParam("clientPool") String clientPool,
                                           @ApiParam("Only select transfers triggered by the specified client.")
                                           @QueryParam("client") String client,
                                           @ApiParam("How to sort responses.")
                                           @DefaultValue("date")
                                           @QueryParam("sort") String sort) {
        try {
            limit = limit == null ? Integer.MAX_VALUE: limit;

            PagedList<P2PTransferRecord> result = service.getP2ps(pnfsid,
                                                                  before,
                                                                  after,
                                                                  limit,
                                                                  offset,
                                                                  serverPool,
                                                                  clientPool,
                                                                  client,
                                                                  sort);
            response.addIntHeader(TOTAL_COUNT_HEADER, result.total);
            return result.contents;
        } catch (FileNotFoundCacheException e) {
            throw new NotFoundException(e);
        } catch (NoRouteToCellException | InterruptedException | CacheException e) {
            throw new InternalServerErrorException(e);
        } catch (IllegalArgumentException | ParseException e ) {
            throw new BadRequestException(e.getMessage(), e);
        }
    }


    @GET
    @ApiOperation("Provides a list of tape writes for a specific PNFS-ID.")
    @ApiResponses({
                @ApiResponse(code = 400, message = "Bad request"),
                @ApiResponse(code = 403, message = "Billing records are only available to admin users."),
                @ApiResponse(code = 404, message = "Not Found"),
                @ApiResponse(code = 500, message = "Internal Server Error"),
            })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("stores/{pnfsid}")
    public List<HSMTransferRecord> getStores(@ApiParam("The file to list.")
                                             @PathParam("pnfsid") PnfsId pnfsid,
                                             @ApiParam("Return no tape writes after this datestamp.")
                                             @QueryParam("before") String before,
                                             @ApiParam("Return no tape writes before this datestamp.")
                                             @QueryParam("after") String after,
                                             @ApiParam("Maximum number of tape writes to return.")
                                             @QueryParam("limit") Integer limit,
                                             @ApiParam("Number of tape writes to skip.")
                                             @DefaultValue("0")
                                             @QueryParam("offset") int offset,
                                             @ApiParam("Only select tape writes involving the specified pool.")
                                             @QueryParam("pool") String pool,
                                             @ApiParam("How to sort responses.")
                                             @DefaultValue("date")
                                             @QueryParam("sort") String sort) {
        try {
            limit = limit == null ? Integer.MAX_VALUE: limit;

            PagedList<HSMTransferRecord> result = service.getStores(pnfsid,
                                                                    before,
                                                                    after,
                                                                    limit,
                                                                    offset,
                                                                    pool,
                                                                    sort);
            response.addIntHeader(TOTAL_COUNT_HEADER, result.total);
            return result.contents;
        } catch (FileNotFoundCacheException e) {
            throw new NotFoundException(e);
        } catch (NoRouteToCellException | InterruptedException | CacheException e) {
            throw new InternalServerErrorException(e);
        } catch (IllegalArgumentException | ParseException e) {
            throw new BadRequestException(e.getMessage(), e);
        }
    }


    @GET
    @ApiOperation("Provide a list of tape reads for a specific PNFS-ID.")
    @ApiResponses({
                @ApiResponse(code = 400, message = "Bad request"),
                @ApiResponse(code = 403, message = "Billing records are only available to admin users."),
                @ApiResponse(code = 404, message = "Not Found"),
                @ApiResponse(code = 500, message = "Internal Server Error"),
            })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("restores/{pnfsid}")
    public List<HSMTransferRecord> getRestores(@ApiParam("The file to list.")
                                               @PathParam("pnfsid") PnfsId pnfsid,
                                               @ApiParam("Return no tape reads after this datestamp.")
                                               @QueryParam("before") String before,
                                               @ApiParam("Return no tape reads before this datestamp.")
                                               @QueryParam("after") String after,
                                               @ApiParam("Maximum number of tape reads to return.")
                                               @QueryParam("limit") Integer limit,
                                               @ApiParam("Number of tape reads to skip.")
                                               @DefaultValue("0")
                                               @QueryParam("offset") int offset,
                                               @ApiParam("Only select tape reads involving the specified pool.")
                                               @QueryParam("pool") String pool,
                                               @ApiParam("How to sort responses.")
                                               @DefaultValue("date")
                                               @QueryParam("sort") String sort) {
        try {
            limit = limit == null ? Integer.MAX_VALUE: limit;

            PagedList<HSMTransferRecord> result = service.getRestores(pnfsid,
                                                                      before,
                                                                      after,
                                                                      limit,
                                                                      offset,
                                                                      pool,
                                                                      sort);
            response.addIntHeader(TOTAL_COUNT_HEADER, result.total);
            return result.contents;
        } catch (FileNotFoundCacheException e) {
            throw new NotFoundException(e);
        } catch (NoRouteToCellException | InterruptedException | CacheException e) {
            throw new InternalServerErrorException(e);
        } catch (IllegalArgumentException | ParseException e) {
            throw new BadRequestException(e.getMessage(), e);
        }
    }


    @GET
    @ApiOperation("Provide the full \"grid\" of time series data in one pass.")
    @ApiResponses({
                @ApiResponse(code = 500, message = "Internal Server Error"),
            })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("histograms")
    public List<Histogram> getGridData() {
        List<Histogram> gridData = new ArrayList<>();

        try {
            service.getGrid()
                   .getDataGrid()
                   .keySet()
                   .stream()
                   .sorted()
                   .forEach((key) -> {
                       try {
                           gridData.add(service.getHistogram(key));
                       } catch (CacheException e1) {
                           throw new InternalServerErrorException(e1);
                       }
                   });
        } catch (CacheException e) {
            throw new InternalServerErrorException(e);
        }

        return gridData;
    }

    /**
     * <p>Request the time series data for a particular specification.</p>
     *
     * <p>The available types of time series can be obtained by calling
     *    {@link #getGrid()}.</p>
     *
     * <p>The range upper bound is to be determined by the service implementation,
     *      but will generally coincide with the most recent information.</p>
     *
     * @param key string specifying the type of series.  This is the string
     *            value of a {@link BillingDataGridEntry}.
     * @return the data (array of doubles).
     */
    @GET
    @ApiOperation("Request the time series data for a particular specification. "
                    + "The available specifications can be obtained via GET on "
                    + "histograms/grid/description.")
    @ApiResponses({
                @ApiResponse(code = 400, message = "Bad request"),
                @ApiResponse(code = 500, message = "Internal Server Error"),
            })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("histograms/{key}")
    public Histogram getData(@ApiParam("The specification identifier for which to fetch data.")
                             @PathParam("key") String key) {
        /*
         *  No admin privileges necessary for billing histogram data.
         */
        try {
            return service.getHistogram(key);
        } catch (CacheException e) {
            throw new InternalServerErrorException(e);
        } catch (IllegalArgumentException e) {
            throw new BadRequestException(e);
        }
    }


    @GET
    @ApiOperation("Provides the list of available histograms with their "
            + "corresponding identifer.")
    @ApiResponses({
                @ApiResponse(code = 500, message = "Internal Server Error"),
            })
    @Produces(MediaType.APPLICATION_JSON)
    @Path("histograms/grid/description")
    public BillingDataGrid getGrid() {
        /*
         *  No admin privileges necessary for billing histogram data.
         */
        try {
            return service.getGrid();
        } catch (CacheException e) {
            throw new InternalServerErrorException(e);
        }
    }
}
