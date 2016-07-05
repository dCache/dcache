package org.dcache.restful.qos;


import org.json.JSONException;
import org.json.JSONObject;

import javax.servlet.ServletContext;

import javax.ws.rs.Path;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Produces;
import javax.ws.rs.Consumes;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Context;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.BadRequestException;

import java.util.EnumSet;
import java.util.Set;


import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;

import org.dcache.auth.Subjects;
import org.dcache.namespace.FileAttribute;
import org.dcache.restful.util.ServletContextHandlerAttributes;
import org.dcache.vehicles.FileAttributes;

/**
 * Created by sahakya on 7/5/16.
 */

@Path("/qos-management/namespace")
public class QosManagementNamespace {

    @Context
    ServletContext ctx;

    /**
     * Gets the current status of the object, (including transition status), for the object specified by path.
     *
     * @param requestPath path to a file
     * @return JSONObject current QoS status
     * @throws CacheException
     */
    @GET
    @Path("{requestPath : .*}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getQosStatus(@PathParam("requestPath") String requestPath) throws CacheException {

        JSONObject jsonResponse = new JSONObject();


        try {
            if (Subjects.isNobody(ServletContextHandlerAttributes.getSubject())) {
                throw new PermissionDeniedCacheException("Permission denied");
            }

            //TODO  get QoS for the specified file
            getPath(requestPath);


        } catch (PermissionDeniedCacheException e) {
            if (Subjects.isNobody(ServletContextHandlerAttributes.getSubject())) {
                throw new NotAuthorizedException(e);
            } else {
                throw new ForbiddenException(e);
            }
        } catch (FileNotFoundCacheException e) {
            throw new NotFoundException(e);
        } catch (CacheException e) {
            throw new InternalServerErrorException(e);
        }

        jsonResponse.put("status", "200");
        jsonResponse.put("message", "successful");
        jsonResponse.put("current_qos", "-");
        return jsonResponse.toString();

    }

    /**
     * Starts a object transition to the specified QoS.
     *
     * @param requestPath path to a file
     * @param requestPath requestQuery
     * @return JSONObject current QoS status
     * @throws CacheException
     */
    @POST
    @Path("{requestPath : .*}")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public String changeQosStatus(@PathParam("requestPath") String requestPath, String requestQuery) throws CacheException {


        JSONObject jsonResponse = new JSONObject(requestQuery);

        try {
            if (Subjects.isNobody(ServletContextHandlerAttributes.getSubject())) {
                throw new PermissionDeniedCacheException("Permission denied");
            }

            JSONObject jsonRequest = new JSONObject(requestQuery);
            String currentQos = (String) jsonRequest.get("current_qos").toString();
            String target_Qos = (String) jsonRequest.get("target_Qos").toString();


            //TODO  get QoS (locality) for the specified file
            getPath(requestPath);

        } catch (PermissionDeniedCacheException e) {
            if (Subjects.isNobody(ServletContextHandlerAttributes.getSubject())) {
                throw new NotAuthorizedException(e);
            } else {
                throw new ForbiddenException(e);
            }
        } catch (FileNotFoundCacheException e) {
            throw new NotFoundException(e);
        } catch (CacheException e) {
            throw new InternalServerErrorException(e);
        } catch (BadRequestException | JSONException e) {
            throw new BadRequestException(e);
        }

        jsonResponse.put("status", "200");
        jsonResponse.put("message", "Transition was successful");
        return jsonResponse.toString();

    }

    //TODO  get QoS (locality) for the specified file
    public FileAttributes getPath(String requestPath) throws CacheException {


        PnfsHandler handler = ServletContextHandlerAttributes.getPnfsHandler(ctx);
        FsPath path;
        if (requestPath == null || requestPath.isEmpty()) {
            path = FsPath.ROOT;
        } else {
            path = FsPath.create(FsPath.ROOT + requestPath);
        }
        Set<FileAttribute> attributes = EnumSet.allOf(FileAttribute.class);

        FileAttributes namespaceAttrributes = handler.getFileAttributes(path, attributes);


        return namespaceAttrributes;
    }


}
