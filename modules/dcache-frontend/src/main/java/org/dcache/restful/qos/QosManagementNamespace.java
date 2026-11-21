package org.dcache.restful.qos;

import diskCacheV111.util.CacheException;
import io.swagger.v3.oas.annotations.Hidden;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import org.springframework.stereotype.Component;

/**
 * Query current QoS for a file or change the current QoS
 */
@Component
@Hidden
@Path("/qos-management/namespace")
public class QosManagementNamespace {

    @Context
    private HttpServletRequest request;

    /**
     * Gets the current status of the object, (including transition status), for the object
     * specified by path.
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
