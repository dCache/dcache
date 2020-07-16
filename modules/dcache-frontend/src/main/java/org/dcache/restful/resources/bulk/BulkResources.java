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
package org.dcache.restful.resources.bulk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.Example;
import io.swagger.annotations.ExampleProperty;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.inject.Inject;
import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.restful.util.RequestUser;
import org.dcache.restful.util.bulk.BulkServiceCommunicator;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkRequestCancelMessage;
import org.dcache.services.bulk.BulkRequestClearMessage;
import org.dcache.services.bulk.BulkRequestListMessage;
import org.dcache.services.bulk.BulkRequestMessage;
import org.dcache.services.bulk.BulkRequestStatus;
import org.dcache.services.bulk.BulkRequestStatus.Status;
import org.dcache.services.bulk.BulkRequestStatusMessage;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

/**
 * <p>RESTful API to the BulkService.</p>
 *
 * @version v1.0
 */
@Component
@Api(value = "bulk-requests", authorizations = {@Authorization("basicAuth")})
@Path("/bulk-requests")
public final class BulkResources {

  @Context
  private HttpServletRequest request;

  @Inject
  private BulkServiceCommunicator service;

  /**
   * @return List of absolute URLs of bulk requests made by this user and that have not been
   * cleared. If the client includes no query string then the response contains all bulk requests
   * made by this user that have not been cleared.  If the user has made no bulk requests or all
   * bulk requests have been cleared then the response is an empty array. If the client specified a
   * query string then the response contains  all bulk requests that match the query string
   * arguments and have not been cleared.  If the user has no bulk requests that match the query
   * string and have not been cleared then the response is an empty array.
   * <p>
   * NOTE: users logged in with the admin role will see all users' requests.
   * @status A comma-separated list of non-repeating elements, each of which is one of: queued,
   * started, completed, cancelled.
   */
  @GET
  @ApiOperation("Get the status of bulk operations submitted by the user.")
  @ApiResponses({
      @ApiResponse(code = 400, message = "Bad request"),
      @ApiResponse(code = 401, message = "Not authorized"),
      @ApiResponse(code = 500, message = "Internal Server Error")
  })
  @Produces(MediaType.APPLICATION_JSON)
  public List<String> getRequests(@ApiParam("A comma-separated list of non-repeating elements, "
                                            + "each of which is one of: queued, started, completed, "
                                            + "cancelled.")
  @QueryParam("status") String status) {
    Subject subject = getSubject();
    Restriction restriction = getRestriction();

    Set<Status> filter;
    if (Strings.emptyToNull(status) != null) {
        filter = new HashSet<>();
        Splitter.on(",").split(status).forEach(o -> filter.add(Status.valueOf(o)));
    } else {
      filter = null;
    }

    BulkRequestListMessage message = new BulkRequestListMessage(filter, restriction);
    message.setSubject(subject);
    message = service.send(message);

    return message.getRequests();
  }

  /**
   * Submit a bulk request.  See {@link BulkRequest}.
   * <p>
   * NOTE:  users logged in with the admin role will be submitting the request as ROOT (0:0).
   *
   * @return response which includes a location HTTP response header with a value that is the
   * absolute URL for the resource associated with this bulk request.
   */
  @POST
  @ApiOperation(value = "Submit a bulk request.")
  @ApiResponses({
      @ApiResponse(code = 201, message = "Created"),
      @ApiResponse(code = 400, message = "Bad request"),
      @ApiResponse(code = 401, message = "Unauthorized"),
      @ApiResponse(code = 403, message = "Forbidden"),
      @ApiResponse(code = 429, message = "Too many requests"),
      @ApiResponse(code = 500, message = "Internal Server Error")
  })
  @Consumes({MediaType.APPLICATION_JSON})
  @Produces(MediaType.APPLICATION_JSON)
  public Response submit(@ApiParam(value = "Description of the request, which defines the following: "
                                    + "target, targetPrefix, activity, cancelOnFailure, "
                                    + "clearOnSuccess, clearOnFailure, delayClear, expandDirectories "
                                    + "(NONE, TARGETS, ALL), and arguments (map of name:value "
                                    + "pairs) if required.", required = true)
                                   String requestPayload) {
    Subject subject = getSubject();
    Restriction restriction = getRestriction();

    BulkRequest request;

    try {
      request = new ObjectMapper().readValue(requestPayload, BulkRequest.class);
    } catch (IOException e) {
      throw new BadRequestException(e);
    }

    /*
     *  Frontend sets the URL.  The backend service provides the UUID.
     */
    request.setUrlPrefix(this.request.getRequestURL().toString());

    BulkRequestMessage message = new BulkRequestMessage(request, restriction);
    message.setSubject(subject);
    message = service.send(message);

    return Response.status(Response.Status.CREATED)
                   .header("request-url", message.getRequestUrl())
                   .type(MediaType.APPLICATION_JSON)
                   .build();
  }

  /**
   * Get status information for an individual request.
   * <p>
   * NOTE: users logged in with the admin role can obtain info on any request.
   *
   * @param id of the request.
   * @return Object which describes the status of the request. See {@link BulkRequestStatus} for the
   * data fields.
   */
  @GET
  @ApiOperation("Get the status information for an individual bulk request.")
  @ApiResponses({
      @ApiResponse(code = 400, message = "Bad request"),
      @ApiResponse(code = 401, message = "Unauthorized"),
      @ApiResponse(code = 403, message = "Forbidden"),
      @ApiResponse(code = 404, message = "Not found"),
      @ApiResponse(code = 500, message = "Internal Server Error")
  })
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public BulkRequestStatus getBulkRequestStatus(@ApiParam("The unique id of the request.")
                                                @PathParam("id") String id) {
    Subject subject = getSubject();
    Restriction restriction = getRestriction();

    BulkRequestStatusMessage message = new BulkRequestStatusMessage(id, restriction);
    message.setSubject(subject);
    message = service.send(message);
    return message.getStatus();
  }

  /**
   * Currently supports only 'action: cancel'.
   * <p>
   * If the bulk operation is in state started then all dCache activity for this bulk request is
   * stopped.
   * <p>
   * The corresponding bulk request status is updated to cancelled if it is currently queued or
   * started.  It does not change in the status is cancelled or completed.
   * <p>
   * NOTE: users logged in with the admin role can cancel any request.
   *
   * @param id             of the request.
   * @param requestPayload A JSON Object with an 'action' item specifying an action to take.
   * @return response
   */
  @PATCH
  @ApiOperation("Take some action on a bulk request.")
  @ApiResponses({
      @ApiResponse(code = 200, message = "Successful"),
      @ApiResponse(code = 400, message = "Bad request"),
      @ApiResponse(code = 401, message = "Unauthorized"),
      @ApiResponse(code = 403, message = "Forbidden"),
      @ApiResponse(code = 404, message = "Not found"),
      @ApiResponse(code = 500, message = "Internal Server Error")
  })
  @Path("/{id}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response update(@ApiParam("The unique id of the request.")
                         @PathParam("id") String id,
                         @ApiParam(value = "A JSON Object with an 'action' item specifying an "
                                   + "action to take.", examples = @Example({
                                                                    @ExampleProperty("{\"action\" : "
                                                                    + "\"cancel\" }")}))
                                   String requestPayload) {
    Subject subject = getSubject();
    Restriction restriction = getRestriction();

    JSONObject reqPayload = new JSONObject(requestPayload);

    String action = reqPayload.getString("action");

    if (!"cancel".equalsIgnoreCase(action)) {
      throw new BadRequestException(action + " not supported.");
    }

    BulkRequestCancelMessage message = new BulkRequestCancelMessage(id, restriction);
    message.setSubject(subject);
    service.send(message);

    return Response.ok().build();
  }

  /**
   * If the bulk operation was in state started then all dCache activity triggered by this bulk
   * request is stopped.
   * <p>
   * The bulk request is cleared.  No further activity will take place for this request.  The server
   * will respond to subsequent GET requests targeting this resource with a 404 (Not Found) status
   * code.
   * <p>
   * NOTE: users logged in with the admin role can clear any request.
   *
   * @param id of the request.
   * @return response
   */
  @DELETE
  @ApiOperation("Clear all resources pertaining to the given bulk request id.")
  @ApiResponses({
      @ApiResponse(code = 204, message = "No content"),
      @ApiResponse(code = 400, message = "Bad request"),
      @ApiResponse(code = 401, message = "Unauthorized"),
      @ApiResponse(code = 403, message = "Forbidden"),
      @ApiResponse(code = 500, message = "Internal Server Error")
  })
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response clearRequest(@ApiParam("The unique id of the request.")
                               @PathParam("id") String id) {
    Subject subject = getSubject();
    Restriction restriction = getRestriction();

    BulkRequestClearMessage message = new BulkRequestClearMessage(id, restriction);
    message.setSubject(subject);
    service.send(message);

    return Response.noContent().build();
  }

  private Subject getSubject() {
    if (RequestUser.isAnonymous()) {
      throw new NotAuthorizedException("User cannot be anonymous.");
    }

    if (RequestUser.isAdmin()) {
      return Subjects.ROOT;
    }

    return RequestUser.getSubject();
  }

  private Restriction getRestriction() {
    if (RequestUser.isAdmin()) {
      return Restrictions.none();
    }

    return RequestUser.getRestriction();
  }
}
