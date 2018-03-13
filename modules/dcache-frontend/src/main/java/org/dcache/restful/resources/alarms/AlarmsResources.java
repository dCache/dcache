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
package org.dcache.restful.resources.alarms;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.Example;
import io.swagger.annotations.ExampleProperty;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.PATCH;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import diskCacheV111.util.CacheException;

import org.dcache.alarms.LogEntry;
import org.dcache.restful.services.alarms.AlarmsInfoService;
import org.dcache.restful.util.HttpServletRequests;

import static org.dcache.restful.providers.ErrorResponseProvider.NOT_IMPLEMENTED;
import static org.dcache.restful.providers.SuccessfulResponse.successfulResponse;

/**
 * <p>RESTful API to the {@link AlarmsInfoService} service.</p>
 *
 * @version v1.0
 */
@Component
@Api(value = "alarms", authorizations = {@Authorization("basicAuth")})
@Path("/alarms")
public final class AlarmsResources {
    @Context
    private HttpServletRequest request;

    @Inject
    private AlarmsInfoService service;


    @ApiOperation(value = "General information about alarms service.",
            hidden = true)
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String info() {
        throw new InternalServerErrorException("Method not yet implemented.",
                                               NOT_IMPLEMENTED);
    }


    @GET
    @ApiOperation("Provides a filtered list of log entries.")
    @ApiResponses({
                @ApiResponse(code = 400, message = "Bad request"),
                @ApiResponse(code = 403, message = "Alarm service only accessible to admin users."),
                @ApiResponse(code = 500, message = "Internal Server Error"),
            })
    @Path("logentries") // collection of all LogEntry.
    @Produces(MediaType.APPLICATION_JSON)
    public List<LogEntry> getAlarms(@ApiParam("Number of entries to skip in directory listing.")
                                    @QueryParam("offset") Long offset,
                                    @ApiParam("Limit number of replies in directory listing.")
                                    @QueryParam("limit") Long limit,
                                    @ApiParam("Return no alarms before this datestamp, in unix-time.")
                                    @QueryParam("after") Long after,
                                    @ApiParam("Return no alarms after this datestamp, in unix-time.")
                                    @QueryParam("before") Long before,
                                    @ApiParam("Whether to include closed alarms.")
                                    @QueryParam("includeClosed") Boolean includeClosed,
                                    @ApiParam("Select log entries with at least this severity.")
                                    @QueryParam("severity") String severity,
                                    @ApiParam("Select only log entries of this alarm type.")
                                    @QueryParam("type") String type,
                                    @ApiParam("Select only log entries from this host.")
                                    @QueryParam("host") String host,
                                    @ApiParam("Select only log entries from this domain.")
                                    @QueryParam("domain") String domain,
                                    @ApiParam("Select only log entries from this service.")
                                    @QueryParam("service") String service,
                                    @ApiParam("Select only log entries that match the info.")
                                    @QueryParam("info") String info,
                                    @ApiParam("A comma-seperated list of fields to sort log entries.")
                                    @QueryParam("sort") String sort) {
        if (!HttpServletRequests.isAdmin(request)) {
            throw new ForbiddenException(
                            "Alarm service only accessible to admin users.");
        }

        try {
            return this.service.get(offset,
                                    limit,
                                    after,
                                    before,
                                    includeClosed,
                                    severity,
                                    type,
                                    host,
                                    domain,
                                    service,
                                    info,
                                    sort);
        } catch (IllegalArgumentException e) {
            throw new BadRequestException(e);
        } catch (CacheException | InterruptedException e) {
            throw new InternalServerErrorException(e);
        }
    }


    @PATCH
    @Path("logentries") // collection of all LogEntry.
    @ApiOperation("Batch request to update or delete the indicated alarms.")
    @ApiResponses({
                @ApiResponse(code = 400, message = "Bad request"),
                @ApiResponse(code = 403, message = "Alarm service only accessible to admin users."),
                @ApiResponse(code = 500, message = "Internal Server Error"),
            })
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response bulkUpdateOrDelete(@ApiParam(value = "A JSON object "
                                        + "describing the changes.  The \"action\" "
                                        + "item is a string with either \"update\" "
                                        + "or \"delete\" as a value.  The "
                                        + "\"items\" item is a JSON Array. For the "
                                        + "\"delete\" action, this array contains "
                                        + "strings, each the key of a log entry "
                                        + "to delete.  For the \"update\" action, "
                                        + "the array contains JSON objects with a "
                                        + "\"key\" item and a \"closed\" item.  "
                                        + "The closed value is a boolean and "
                                        + "the key value is a String.",
                                               examples = @Example({
                                                   @ExampleProperty("{\n"
                                                           + "    \"action\" : \"update\",\n"
                                                           + "    \"items\" : [ \n"
                                                           + "            { \"key\" : \"key-1\", \"closed\" : true },\n"
                                                           + "            { \"key\" : \"key-2\", \"closed\" : false }\n"
                                                           + "        ]\n"
                                                           + "}")
                                             }))
                                       String requestPayload) {
        if (!HttpServletRequests.isAdmin(request)) {
            throw new ForbiddenException(
                            "Alarm service only accessible to admin users.");
        }

        JSONObject reqPayload = new JSONObject(requestPayload);
        String action = reqPayload.getString("action");
        JSONArray items  = reqPayload.getJSONArray("items");
        int numberOfItems = items.length();
        List<LogEntry> list = new ArrayList<>();

        try {
            switch (action) {
                case "delete":
                    for (int i = 0; i < numberOfItems; ++i) {
                        LogEntry entry = new LogEntry();
                        entry.setKey(items.getString(i));
                        list.add(entry);
                    }
                    service.delete(list);
                    break;
                case "update":
                    for (int i = 0; i < numberOfItems; ++i) {
                        JSONObject object = items.getJSONObject(i);
                        LogEntry entry = new LogEntry();
                        entry.setKey(object.getString("key"));
                        entry.setClosed(object.getBoolean("closed"));
                        list.add(entry);
                    }
                    service.update(list);
                    break;
                default:
                    String message = "Bulk action '" + action + "' not understood;"
                                    + " must be either 'delete' or 'update'.";
                    throw new BadRequestException(message);
            }
        } catch (JSONException | IllegalArgumentException e) {
            throw new BadRequestException(e);
        } catch (CacheException | InterruptedException e) {
            throw new InternalServerErrorException(e);
        }

        return successfulResponse(Response.Status.OK);
    }


    @GET
    @ApiOperation(value = "Request for a single log entry.", hidden = true)
    @Path("/logentries/{key}")
    @Produces(MediaType.APPLICATION_JSON)
    public LogEntry getLogEntry(@ApiParam("The log entry to provide.")
                                @PathParam("key") String key) {
        throw new InternalServerErrorException("Method not yet implemented.",
                                               NOT_IMPLEMENTED);
    }


    @PATCH
    @ApiOperation("Request to open or close the indicated log entry.")
    @ApiResponses({
                @ApiResponse(code = 400, message = "Bad request"),
                @ApiResponse(code = 403, message = "Alarm service only accessible to admin users."),
                @ApiResponse(code = 500, message = "Internal Server Error"),
            })
    @Path("/logentries/{key}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateAlarmEntry(@ApiParam("The identifier for the specific log entry.")
                                     @PathParam("key") String key,
                                     @ApiParam(value = "A JSON Object with a 'closed' "
                                             + "item containing a JSON Boolean value.",
                                             examples = @Example({
                                                     @ExampleProperty("{\"closed\" : true}")
                                             }))
                                     String requestPayload) {
        if (!HttpServletRequests.isAdmin(request)) {
            throw new ForbiddenException(
                            "Alarm service only accessible to admin users.");
        }

        JSONObject reqPayload = new JSONObject(requestPayload);

        try {
            LogEntry entry = new LogEntry();
            entry.setKey(key);
            entry.setClosed(reqPayload.getBoolean("closed"));
            service.update(Collections.singletonList(entry));
        } catch (JSONException | IllegalArgumentException e) {
            throw new BadRequestException(e);
        } catch (CacheException | InterruptedException e) {
            throw new InternalServerErrorException(e);
        }

        return successfulResponse(Response.Status.OK);
    }


    @DELETE
    @ApiOperation("Delete a specific log entry.")
    @ApiResponses({
                @ApiResponse(code = 400, message = "Bad request"),
                @ApiResponse(code = 403, message = "Alarm service only accessible to admin users."),
                @ApiResponse(code = 500, message = "Internal Server Error"),
            })
    @Path("/logentries/{key}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteAlarmEntry(@ApiParam("The identifier for the specific log entry.")
                                     @PathParam("key") String key) {
        if (!HttpServletRequests.isAdmin(request)) {
            throw new ForbiddenException(
                            "Alarm service only accessible to admin users.");
        }

        try {
            LogEntry entry = new LogEntry();
            entry.setKey(key);
            service.delete(Collections.singletonList(entry));
        } catch (JSONException | IllegalArgumentException e) {
            throw new BadRequestException(e);
        } catch (CacheException | InterruptedException e) {
            throw new InternalServerErrorException(e);
        }

        return successfulResponse(Response.Status.OK);
    }


    @GET
    @ApiOperation("Request the current mapping of all alarm types to priorities.")
    @ApiResponses({
                @ApiResponse(code = 403, message = "Alarm service only accessible to admin users.")
            })
    @Path("/priorities")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, String> getPriorities() {
        if (!HttpServletRequests.isAdmin(request)) {
            throw new ForbiddenException(
                            "Alarm service only accessible to admin users.");
        }

        return service.getMap();
    }


    @GET
    @ApiOperation("Request the current mapping of an alarm type to its priority.")
    @ApiResponses({
                @ApiResponse(code = 403, message = "Alarm service only accessible to admin users.")
            })
    @Path("/priorities/{type}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getPriority(@ApiParam("The alarm type.")
                              @PathParam("type") String type) {
        if (!HttpServletRequests.isAdmin(request)) {
            throw new ForbiddenException(
                            "Alarm service only accessible to admin users.");
        }

        return service.getMap().get(type);
    }


    @PUT
    @ApiOperation(value="Change the priority of the given alarm type.",
            hidden=true)
    @Path("/priorities/{type}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response updatePriority(@ApiParam("The alarm type.")
                                   @PathParam("type") String type,
                                   @ApiParam("The desired priority.")
                                   @QueryParam("priority") String priority) {
        return NOT_IMPLEMENTED;
    }


    @DELETE
    @ApiOperation(value="Reset the priority of the given alarm to the default.",
            hidden=true)
    @Path("/priorities/{type}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response resetDefaultPriority(@ApiParam("The alarm type.")
                                         @PathParam("type") String type) {
        return NOT_IMPLEMENTED;
    }
}
