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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONException;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import diskCacheV111.util.CacheException;

import org.dcache.alarms.LogEntry;
import org.dcache.restful.services.alarms.AlarmsInfoService;
import org.dcache.restful.util.HttpServletRequests;

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

    /**
     * <p>Alarms.</p>
     *
     * <p>The Alarms endpoint returns a (filtered) list of alarms.</p>
     *
     * @param offset specifying the index at which to begin.
     * @param limit  maximum number of alarms to include.
     * @param after  Return no alarms before this datestamp.
     * @param before Return no alarms after this datestamp.
     * @param type   Return only alarms of this type.
     *
     * @return object containing list of transfers, along with token and
     *         offset information.
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<LogEntry> getAlarms(@QueryParam("offset") Long offset,
                                    @QueryParam("limit") Long limit,
                                    @QueryParam("after") Long after,
                                    @QueryParam("before") Long before,
                                    @QueryParam("type") String type) {
        if (!HttpServletRequests.isAdmin(request)) {
            throw new ForbiddenException(
                            "Alarm service only accessible to admin users.");
        }

        try {
            return service.get(offset, limit, after, before, type);
        } catch (IllegalArgumentException | CacheException e) {
            throw new BadRequestException(e);
        } catch (Exception e) {
            throw new InternalServerErrorException(e);
        }
    }

    /**
     * <p>Request for current mapping of alarm types to priorities.</p>
     *
     * @return requested priority map
     */
    @GET
    @Path("/map")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, String> getAlarmsMap() {
        if (!HttpServletRequests.isAdmin(request)) {
            throw new ForbiddenException(
                            "Alarm service only accessible to admin users.");
        }

        return service.getMap();
    }

    /**
     * <p>Request to delete the indicated alarm from the service's store.</p>
     *
     * @param requestPayload containing the object to delete.
     */
    @DELETE
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteAlarmEntry(String requestPayload) throws
                    CacheException {
        return updateOrDelete(requestPayload, true);
    }

    /**
     * <p>Request to update the indicated alarm.</p>
     *
     * @param requestPayload containing the object with updated fields.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateAlarmEntry(String requestPayload) throws
                    CacheException {
        return updateOrDelete(requestPayload, false);
    }

    private Response updateOrDelete(String requestPayload, boolean delete)
                throws CacheException {
        if (!HttpServletRequests.isAdmin(request)) {
            throw new ForbiddenException(
                            "Alarm service only accessible to admin users.");
        }

        try {
            LogEntry[] entries = new ObjectMapper().readValue(requestPayload,
                                                              LogEntry[].class);
            List<LogEntry> list = Arrays.stream(entries)
                                        .collect(Collectors.toList());

            if (delete) {
                service.delete(list);
            } else {
                service.update(list);
            }
        } catch (JSONException | IllegalArgumentException e) {
            throw new BadRequestException(e);
        } catch (Exception e) {
            throw new InternalServerErrorException(e);
        }

        return successfulResponse(Response.Status.OK);
    }

}
