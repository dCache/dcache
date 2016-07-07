package org.dcache.restful.qos;




import org.json.JSONException;
import org.json.JSONObject;

import javax.servlet.ServletContext;

import javax.servlet.http.HttpServletRequest;
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

import java.net.InetSocketAddress;
import java.util.EnumSet;
import java.util.Set;


import org.dcache.auth.Subjects;
import org.dcache.namespace.FileAttribute;
import org.dcache.restful.util.ServletContextHandlerAttributes;
import org.dcache.vehicles.FileAttributes;
import diskCacheV111.vehicles.DCapProtocolInfo;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileLocality;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;

import org.dcache.cells.CellStub;
import org.dcache.namespace.FileType;
import org.dcache.pinmanager.PinManagerPinMessage;
import org.dcache.poolmanager.RemotePoolMonitor;


/**
 * Query current QoS for a file or  change the current QoS
 */

@Path("/qos-management/namespace")
public class QosManagementNamespace {

    @Context
    ServletContext ctx;

    @Context
    HttpServletRequest request;

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
    public BackendCapabilityResponse getQosStatus(@PathParam("requestPath") String requestPath) throws CacheException {

        BackendCapabilityResponse response = new BackendCapabilityResponse();
        try {
            if (Subjects.isNobody(ServletContextHandlerAttributes.getSubject())) {
                throw new PermissionDeniedCacheException("Permission denied");
            }

            //get the locality of the specified file
            RemotePoolMonitor remotePoolMonitor = ServletContextHandlerAttributes.getRemotePoolMonitor(ctx);
            FileLocality fileLocality = remotePoolMonitor.getFileLocality(getFileAttributes(requestPath), request.getRemoteHost());

            switch (fileLocality) {
                case NEARLINE:
                    response.setQoS(QosManagement.TAPE);
                    break;
                case ONLINE:
                    response.setQoS(QosManagement.DISK);
                    break;
                case ONLINE_AND_NEARLINE:
                    response.setQoS(QosManagement.DISK_TAPE);
                    break;
                default:
                    // error cases
                    throw new InternalServerErrorException();
            }
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
        return response;
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


        JSONObject jsonResponse = new JSONObject();

        try {
            if (Subjects.isNobody(ServletContextHandlerAttributes.getSubject())) {
                throw new PermissionDeniedCacheException("Permission denied");
            }

            JSONObject jsonRequest = new JSONObject(requestQuery);
            String update = jsonRequest.get("update").toString();


            FileAttributes fileAttributes = getFileAttributes(requestPath);

            if (fileAttributes.getFileType() == FileType.REGULAR) {

                //get the locality of the specified file
                RemotePoolMonitor remotePoolMonitor = ServletContextHandlerAttributes.getRemotePoolMonitor(ctx);
                String fileLocality = remotePoolMonitor.getFileLocality(fileAttributes, request.getRemoteHost()).toString();

                // "NEARLINE" = TAPE for CDMI spec. and tape can have only ["DISK + TAPE"] transitional status
                if ("NEARLINE".equals(fileLocality) && update.equalsIgnoreCase(QosManagement.DISK_TAPE)){
                    pin(fileAttributes);
                }else  {
                    throw new BadRequestException();
                }
            } else {
                // not supported for directories
                throw new BadRequestException();
            }

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

    public FileAttributes getFileAttributes(String requestPath) throws CacheException {

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

    public void pin(FileAttributes fileAttributes) {

        CellStub pinManagerStub = ServletContextHandlerAttributes.getPinManager(ctx);
        DCapProtocolInfo protocolInfo =
                new DCapProtocolInfo("HTTP", 3, 0, new InetSocketAddress(request.getRemoteHost(), 0));
        PinManagerPinMessage message =
                new PinManagerPinMessage(fileAttributes, protocolInfo, null, -1);
        pinManagerStub.notify(message);
    }



}
