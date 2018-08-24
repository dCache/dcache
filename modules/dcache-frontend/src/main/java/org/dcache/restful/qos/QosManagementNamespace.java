package org.dcache.restful.qos;

import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Named;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileLocality;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.HttpProtocolInfo;

import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.pinmanager.PinManagerCountPinsMessage;
import org.dcache.pinmanager.PinManagerPinMessage;
import org.dcache.pinmanager.PinManagerUnpinMessage;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.restful.util.HandlerBuilders;
import org.dcache.restful.util.RequestUser;
import org.dcache.vehicles.FileAttributes;

/**
 * Query current QoS for a file or  change the current QoS
 */

@Component
@Path("/qos-management/namespace")
public class QosManagementNamespace {

    @Context
    private HttpServletRequest request;

    @Inject
    private PoolMonitor poolMonitor;

    @Inject
    @Named("pinManagerStub")
    private CellStub pinmanager;

    @Inject
    @Named("pnfs-stub")
    private CellStub pnfsmanager;

    /** ID provided by the requestor eg. the SRM door or QoS.**/

    private final String requestId= "qos";

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
    public BackendCapabilityResponse getQosStatus(@PathParam("requestPath") String requestPath) throws CacheException, URISyntaxException {

        BackendCapabilityResponse response = new BackendCapabilityResponse();
        try {
            if (RequestUser.isAnonymous()) {
                throw new PermissionDeniedCacheException("Permission denied");
            }

            FileAttributes fileAttributes = getFileAttributes(requestPath);
            FileLocality fileLocality =  getLocality(fileAttributes);

            boolean isPinned = isPinned(fileAttributes, pinmanager);


            switch (fileLocality) {
                case NEARLINE:
                    if (isPinned) {
                        response.setQoS(QosManagement.TAPE);
                        response.setTargetQoS(QosManagement.DISK_TAPE);
                    } else {
                        response.setQoS(QosManagement.TAPE);
                    }
                    break;
                case ONLINE:
                    response.setQoS(QosManagement.DISK);
                    if (fileAttributes.isDefined(FileAttribute.RETENTION_POLICY)
                            && fileAttributes.getRetentionPolicy() == RetentionPolicy.CUSTODIAL) {
                        response.setTargetQoS(QosManagement.TAPE);
                    }
                    break;
                case ONLINE_AND_NEARLINE:
                    /* When the locality of the file is  NEARLINE_ONLINE and
                     * the object is not pinned the result for the user will be displayed as NEARLINE (Tape).
                     * else nearline_online (disk+tape)
                    */
                    if (isPinned) {
                        response.setQoS(QosManagement.DISK_TAPE);
                    } else {
                        response.setQoS(QosManagement.TAPE);
                    }
                    break;
                default:
                    // error cases
                    throw new InternalServerErrorException();
            }
        } catch (PermissionDeniedCacheException e) {
            if (RequestUser.isAnonymous()) {
                throw new NotAuthorizedException(e);
            } else {
                throw new ForbiddenException(e);
            }
        } catch (FileNotFoundCacheException e) {
            throw new NotFoundException(e);
        } catch (CacheException | NoRouteToCellException | InterruptedException e) {
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
    @Deprecated
    @POST
    @Path("{requestPath : .*}")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public String changeQosStatus(@PathParam("requestPath") String requestPath, String requestQuery) throws CacheException,
            URISyntaxException, InterruptedException {

        JSONObject jsonResponse = new JSONObject();

        try {
            if (RequestUser.isAnonymous()) {
                throw new PermissionDeniedCacheException("Permission denied");
            }

            JSONObject jsonRequest = new JSONObject(requestQuery);
            String update = jsonRequest.get("update").toString();


            FileAttributes fileAttributes = getFileAttributes(requestPath);
            FileLocality fileLocality =  getLocality(fileAttributes);

            switch (update) {
                // change QoS to "disk+tape"
                case QosManagement.DISK_TAPE:
                    makeDiskAndTape(fileLocality, fileAttributes, pinmanager);
                    break;
                // change QoS to "tape"
                case QosManagement.TAPE:
                    makeTape(fileLocality, fileAttributes, pinmanager);
                    break;
                default:
                    // error cases
                    throw new BadRequestException();
            }

        } catch (PermissionDeniedCacheException e) {
            if (RequestUser.isAnonymous()) {
                throw new NotAuthorizedException(e);
            } else {
                throw new ForbiddenException(e);
            }
        } catch (FileNotFoundCacheException e) {
            throw new NotFoundException(e);
        } catch (CacheException e) {
            throw new InternalServerErrorException(e);
        } catch (JSONException e) {
            throw new BadRequestException(e);
        }

        jsonResponse.put("status", "200");
        jsonResponse.put("message", "Transition was successful");
        return jsonResponse.toString();

    }

    public FileAttributes getFileAttributes(String requestPath) throws CacheException {

        PnfsHandler handler = HandlerBuilders.roleAwarePnfsHandler(pnfsmanager, request);
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

    /*
    * While pinning files this requestId value "qos" will be stored for the pin.
    * Storing requestId insures the possibility to filter files being pinned by Qos or SRM.
    */
    public void pin(FileAttributes fileAttributes,  CellStub cellStub) throws URISyntaxException {

        HttpProtocolInfo protocolInfo =
                new HttpProtocolInfo("Http", 1, 1,
                        new InetSocketAddress(request.getRemoteHost(), 0),
                        null, null, null,
                        new URI("http", request.getRemoteHost(), null, null));

        PinManagerPinMessage message =
                new PinManagerPinMessage(fileAttributes, protocolInfo, requestId, -1);

        cellStub.notify(message);

    }


    public FileLocality getLocality(FileAttributes fileAttributes) {

        return poolMonitor.getFileLocality(fileAttributes, request.getRemoteHost());
    }


    public boolean isPinned(FileAttributes fileAttributes, CellStub cellStub) throws CacheException, InterruptedException,
            URISyntaxException, NoRouteToCellException
    {
        boolean isPinned = false;

        PinManagerCountPinsMessage message =
                new PinManagerCountPinsMessage(fileAttributes.getPnfsId());

        message = cellStub.sendAndWait(message);
        if (message.getCount() != 0 ){
            isPinned = true;
        }
        return isPinned;
    }


    public void unpin(FileAttributes fileAttributes, CellStub cellStub ) {

        PinManagerUnpinMessage message = new PinManagerUnpinMessage(fileAttributes.getPnfsId());
        message.setRequestId(requestId);
        cellStub.notify(message);
    }


    public void makeDiskAndTape(FileLocality fileLocality,
                                FileAttributes fileAttributes,
                                CellStub cellStub) throws URISyntaxException, CacheException, InterruptedException {
        switch (fileLocality) {

            case NEARLINE:
                pin(fileAttributes, cellStub);
                break;

            case ONLINE_AND_NEARLINE:
                pin(fileAttributes, cellStub);
                break;
            default:
                // error cases
                throw new BadRequestException();
        }

    }

    public void makeTape(FileLocality fileLocality,
                         FileAttributes fileAttributes,
                         CellStub cellStub) {
        switch (fileLocality) {

            case NEARLINE:
                unpin(fileAttributes, cellStub);
                break;

            case ONLINE_AND_NEARLINE:
                unpin(fileAttributes, cellStub);
                break;
            default:
                // error cases
                throw new BadRequestException();
        }

    }
}
