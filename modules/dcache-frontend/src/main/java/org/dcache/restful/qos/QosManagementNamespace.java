package org.dcache.restful.qos;

import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import diskCacheV111.util.CacheException;

/**
 * Query current QoS for a file or change the current QoS
 */
@Component
@Path("/qos-management/namespace")
public class QosManagementNamespace
{
    @Context
    private HttpServletRequest request;

    /**
     * Gets the current status of the object, (including transition status), for the object specified by path.
     *
     * @param requestPath path to a file
     * @return JSONObject current QoS status
     * @throws CacheException
     */
    @Deprecated
    @GET
    @Path("{requestPath : .*}")
    @Produces(MediaType.APPLICATION_JSON)
    public BackendCapabilityResponse getQosStatus(@PathParam("requestPath") String requestPath) {
        throw new BadRequestException("Path is deprecated; please use 'GET /namespace/{path}' "
                                                      + "to retrieve metadata");
    }

    /**
     * Starts a transition to the specified QoS.
     *
     * @param requestPath path to a file
     * @param requestPath requestQuery
     * @return JSONObject current QoS status
     * @throws CacheException
     */
    @Deprecated
    @POST
    @Path("{requestPath : .*}")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public String changeQosStatus(@PathParam("requestPath") String requestPath,
                                  String requestQuery) {
        throw new BadRequestException("Path is deprecated; please use 'POST /namespace/{path}' "
                                                      + "to modify qos");
    }
}
