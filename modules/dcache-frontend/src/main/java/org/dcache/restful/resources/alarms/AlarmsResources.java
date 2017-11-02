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
@Path("/alarms")
public final class AlarmsResources {
    @Context
    private HttpServletRequest request;

    @Inject
    private AlarmsInfoService service;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String info() {
        throw new InternalServerErrorException("Method not yet implemented.",
                                               NOT_IMPLEMENTED);
    }

    /**
     * <p>Alarms.</p>
     * <p>
     * <p>The Alarms endpoint returns a (filtered) list of alarms.</p>
     *
     * @param offset        specifying the index at which to begin.
     * @param limit         maximum number of alarms to include.
     * @param after         Return no alarms before this datestamp.
     * @param before        Return no alarms after this datestamp.
     * @param includeClosed If false, no alarms which are closed
     * @param severity      Filter on severity
     * @param type          Filter on type
     * @param host          Filter on host
     * @param domain        Filter on domain
     * @param service       Filter on service
     * @param info          Filter on info
     * @param sort          List of fields on which to sort
     * @return              List of LogEntry objects.
     */
    @GET
    @Path("logentries") // collection of all LogEntry.
    @Produces(MediaType.APPLICATION_JSON)

    public List<LogEntry> getAlarms(@QueryParam("offset") Long offset,
                                    @QueryParam("limit") Long limit,
                                    @QueryParam("after") Long after,
                                    @QueryParam("before") Long before,
                                    @QueryParam("includeClosed") Boolean includeClosed,
                                    @QueryParam("severity") String severity,
                                    @QueryParam("type") String type,
                                    @QueryParam("host") String host,
                                    @QueryParam("domain") String domain,
                                    @QueryParam("service") String service,
                                    @QueryParam("info") String info,
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

    /**
     * <p>Batch request to update or delete the indicated alarms.</p>
     *
     * @param requestPayload containing the list of partial log entry objects;
     *                       for delete, all that is necessary is the key;
     *                       for close, the key and closed value.
     */
    @PATCH
    @Path("logentries") // collection of all LogEntry.
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response bulkUpdateOrDelete(String requestPayload) {
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

    /**
     * <p>Request for a single alarm.</p>
     */
    @GET
    @Path("/logentries/{key : .*}") // the specific LogEntry with the given key
    @Produces(MediaType.APPLICATION_JSON)
    public LogEntry getLogEntry(@PathParam("key") String key) {
        throw new InternalServerErrorException("Method not yet implemented.",
                                               NOT_IMPLEMENTED);
    }

    /**
     * <p>Request to close or open the indicated alarm.</p>
     *
     * @param key of the alarm to delete.
     */
    @PATCH
    @Path("/logentries/{key : .*}") // the specific LogEntry with the given key
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateAlarmEntry(@PathParam("key") String key,
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

    /**
     * <p>Request to delete the indicated alarm from the service's store.</p>
     *
     * @param key of the alarm to delete.
     */
    @DELETE
    @Path("/logentries/{key : .*}")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteAlarmEntry(@PathParam("key") String key) {
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

    /**
     * <p>Request for current mapping of alarm types to priorities.</p>
     *
     * @return requested priority map
     */
    @GET
    @Path("/priorities")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, String> getPriorities() {
        if (!HttpServletRequests.isAdmin(request)) {
            throw new ForbiddenException(
                            "Alarm service only accessible to admin users.");
        }

        return service.getMap();
    }

    /**
     * <p>Request for current mapping of an alarm type to its priority.</p>
     *
     * @return requested priority map
     */
    @GET
    @Path("/priorities/{type: .*}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getPriority(@PathParam("type") String type) {
        if (!HttpServletRequests.isAdmin(request)) {
            throw new ForbiddenException(
                            "Alarm service only accessible to admin users.");
        }

        return service.getMap().get(type);
    }

    /**
     * <p>Change the priority of the given alarm type.</p>
     *
     * @param priority new priority to assign.
     */
    @PUT
    @Path("/priorities/{type : .*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response updatePriority(@PathParam("type") String type,
                                   @QueryParam("priority") String priority) {
        return NOT_IMPLEMENTED;
    }

    /**
     * <p>Reset the priority of the given alarm to the default.</p>
     */
    @DELETE
    @Path("/priorities/{type : .*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response resetDefaultPriority(@PathParam("type") String type) {
        return NOT_IMPLEMENTED;
    }
}
