package org.dcache.restful.resources.namespace;

import com.google.common.collect.Range;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileLocality;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.vehicles.HttpProtocolInfo;
import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.CellStub;
import org.dcache.http.PathMapper;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.pinmanager.PinManagerCountPinsMessage;
import org.dcache.pinmanager.PinManagerPinMessage;
import org.dcache.pinmanager.PinManagerUnpinMessage;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.poolmanager.RemotePoolMonitor;
import org.dcache.restful.policyengine.MigrationPolicyEngine;
import org.dcache.restful.providers.JsonFileAttributes;
import org.dcache.restful.qos.QosManagement;
import org.dcache.restful.util.HandlerBuilders;
import org.dcache.restful.util.HttpServletRequests;
import org.dcache.restful.util.RequestUser;
import org.dcache.restful.util.ServletContextHandlerAttributes;
import org.dcache.restful.util.namespace.NamespaceUtils;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryStream;
import org.dcache.util.list.ListDirectoryHandler;
import org.dcache.vehicles.FileAttributes;

import static diskCacheV111.util.FileLocality.NEARLINE;
import static diskCacheV111.util.FileLocality.ONLINE_AND_NEARLINE;
import static org.dcache.restful.providers.SuccessfulResponse.successfulResponse;

/**
 * RestFul API to  provide files/folders manipulation operations
 *
 * @version v1.0
 */
@Component
@Path("/namespace")
public class FileResources {

    private final String QOS_PIN_REQUEST_ID = "qos";
    private static final Logger LOG = LoggerFactory.getLogger(FileResources.class);


    @Context
    ServletContext ctx;

    /*
    This "request" parameter is used to get fully qualified name of the client
     * or the last proxy that sent the request. Later used for quering locality of the file.
     */
    @Context
    HttpServletRequest request;

    /**
     * The method offer to list the content of a directory or return metadata of
     * a specified file or directory.
     *
     * @param isList optional boolean parameter, set to false by default.
     *                 When set to true (e.g. /?children=true) the file attributes(e.g.  file size, locality, creation time... )
     *                 of the children (files/directories) of
     *                 the specified directory will be displayed.
     * @param isLocality optional boolean parameter, set to false by default.
     *                 When set to true the locality of file (ONLINE/NEARLINE) is displayed as a part of FileAttributes.
     * @return JsonFileAttributes  Json Object
     * <p>
     * <p>
     * * EXAMPLES
     * Return all the files in the given directory
     * @method GET
     * @Resources URL (default)
     * http://localhost:2880/api/v1/namespace/urlPath/?children=true&locality=true
     * @Request Header:
     * Accept: application/json
     * Content-Type: application/json
     * Method:
     * GET
     * URL:
     * http://localhost:2880/api/v1/namespace/replica/?children=true&locality=true
     * @Response {
     * "children":
     * [
     * {
     * "fileName": "test000001",
     * "fileLocality": "ONLINE",
     * "mtime": 1459959425090,
     * "fileType": "REGULAR",
     * "creationTime": 1459959409825,
     * "size": 378
     * },
     * {
     * "fileName": "test1",
     * "mtime": 1461000184802,
     * "fileType": "DIR",
     * "creationTime": 1461000167903,
     * "size": 512
     * }
     * ],
     * "mtime": 1461000173723,
     * "fileType": "DIR",
     * "creationTime": 1459949700167,
     * "size": 512
     * }
     */
    @GET
    @Path("{value : .*}")
    @Produces(MediaType.APPLICATION_JSON)
    public JsonFileAttributes getFileAttributes(@PathParam("value") String requestPath,
                                                @DefaultValue("false")
                                                @QueryParam("children") boolean isList,
                                                @DefaultValue("false")
                                                @QueryParam("locality") boolean isLocality,
                                                @QueryParam("locations") boolean isLocations,
                                                @DefaultValue("false")
                                                @QueryParam("qos") boolean isQos,
                                                @QueryParam("limit") String limit,
                                                @QueryParam("offset") String offset) throws CacheException
    {
        JsonFileAttributes fileAttributes = new JsonFileAttributes();
        Set<FileAttribute> attributes = EnumSet.allOf(FileAttribute.class);
        PnfsHandler handler = HandlerBuilders.roleAwarePnfsHandler(ctx, request);
        PathMapper pathMapper = ServletContextHandlerAttributes.getPathMapper(ctx);
        FsPath path = pathMapper.asDcachePath(request, requestPath, ForbiddenException::new);
        try {

            FileAttributes namespaceAttributes = handler.getFileAttributes(path, attributes);
            NamespaceUtils.chimeraToJsonAttributes(path.name(), fileAttributes,
                                                   namespaceAttributes,
                                                   isLocality, isLocations,
                                                   false,
                                                   request, ctx);
            if (isQos) {
                NamespaceUtils.addQoSAttributes(fileAttributes,
                                                namespaceAttributes,
                                                request, ctx);
            }

            // fill children list id it's a directory and listing is requested
            if (namespaceAttributes.getFileType() == FileType.DIR && isList) {
                Range<Integer> range;
                try {
                    int lower = (offset == null) ? 0 : Integer.parseInt(offset);
                    int ceiling = (limit == null) ? Integer.MAX_VALUE : Integer.parseInt(limit);
                    if (ceiling < 0 || lower < 0) {
                        throw new BadRequestException("limit and offset can not be less than zero.");
                    }
                    range = (Integer.MAX_VALUE - lower < ceiling) ? Range.atLeast(lower)
                            : Range.closedOpen(lower, lower+ceiling);
                } catch (NumberFormatException e) {
                    throw new BadRequestException("limit and offset must be an integer value.");
                }

                List<JsonFileAttributes> children = new ArrayList<>();

                ListDirectoryHandler listDirectoryHandler = ServletContextHandlerAttributes.getListDirectoryHandler(ctx);

                DirectoryStream stream = listDirectoryHandler.list(
                        HttpServletRequests.roleAwareSubject(request),
                        HttpServletRequests.roleAwareRestriction(request),
                        path,
                        null,
                        range,
                        attributes);

                for (DirectoryEntry entry : stream) {
                    String fName = entry.getName();

                    JsonFileAttributes childrenAttributes = new JsonFileAttributes();

                    NamespaceUtils.chimeraToJsonAttributes(fName,
                                                           childrenAttributes,
                                                           entry.getFileAttributes(),
                                                           isLocality, isLocations,
                                                           false,
                                                           request, ctx);
                    childrenAttributes.setFileName(fName);
                    if (isQos) {
                        NamespaceUtils.addQoSAttributes(childrenAttributes,
                                                        entry.getFileAttributes(),
                                                        request, ctx);
                    }
                    children.add(childrenAttributes);
                }

                fileAttributes.setChildren(children);
            }

        } catch (FileNotFoundCacheException e) {
            throw new NotFoundException(e);
        } catch (PermissionDeniedCacheException e) {
            if (RequestUser.isAnonymous()) {
                throw new NotAuthorizedException(e);
            } else {
                throw new ForbiddenException(e);
            }
        } catch (CacheException | InterruptedException | NoRouteToCellException ex) {
            throw new InternalServerErrorException(ex);
        }
        return fileAttributes;
    }

    @POST
    @Path("{value : .*}")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response cmrResources(@PathParam("value") String requestPath, String requestPayload)
    {
        try {
            JSONObject reqPayload = new JSONObject(requestPayload);
            String action = (String) reqPayload.get("action");
            PnfsHandler handler = HandlerBuilders.roleAwarePnfsHandler(ctx, request);
            PathMapper pathMapper = ServletContextHandlerAttributes.getPathMapper(ctx);
            FsPath path = pathMapper.asDcachePath(request, requestPath, ForbiddenException::new);

            // FIXME: which attributes do we actually need?
            FileAttributes attributes = handler.getFileAttributes(path, EnumSet.allOf(FileAttribute.class));

            switch (action) {
                case "mkdir":
                    String name = (String) reqPayload.get("name");
                    FsPath.checkChildName(name, BadRequestException::new);
                    handler = HandlerBuilders.pnfsHandler(ctx, request); // FIXME: non-role identity to ensure correct ownership
                    handler.createPnfsDirectory(path.child(name).toString());
                    break;

                case "mv":
                    String dest = (String) reqPayload.get("destination");
                    FsPath target = pathMapper.resolve(request, path, dest);
                    handler.renameEntry(path.toString(), target.toString(), true);
                    break;

                case "qos":
                    String targetQos = reqPayload.getString("target");
                    JsonFileAttributes fileAttributes = new JsonFileAttributes();
                    NamespaceUtils.addQoSAttributes(fileAttributes, attributes, request, ctx);
                    LOG.debug("Request received to change current QoS from {} to: {} for {}", fileAttributes.currentQos, targetQos, path);
                    RemotePoolMonitor monitor = ServletContextHandlerAttributes.getRemotePoolMonitor(ctx);
                    FileLocality locality = monitor.getFileLocality(attributes, request.getRemoteHost());
                    LOG.debug("The Locality of the file: {}", locality);
                    if (locality == FileLocality.NONE) {
                        throw new BadRequestException("Transition for directories not supported");
                    }

                    ProtocolInfo info = new HttpProtocolInfo("Http", 1, 1,
                            new InetSocketAddress(request.getRemoteHost(), 0),
                            null, null, null,
                            URI.create("http://"+request.getRemoteHost()+"/"));

                    CellStub cellStub = ServletContextHandlerAttributes.getPoolManger(ctx);
                    CellStub pinmanager = ServletContextHandlerAttributes.getPinManager(ctx);

                    PoolMonitor poolMonitor = ServletContextHandlerAttributes.getRemotePoolMonitor(ctx);
                    MigrationPolicyEngine migrationPolicyEngine =
                            new MigrationPolicyEngine(attributes, cellStub, poolMonitor);

                    switch (targetQos) {
                    case QosManagement.DISK_TAPE:
                        if (locality != NEARLINE && locality != ONLINE_AND_NEARLINE) {
                            migrationPolicyEngine.adjust();
                        }
                        boolean isPinned = pinmanager.sendAndWait(new PinManagerCountPinsMessage(attributes.getPnfsId())).getCount() != 0;
                        if (!isPinned) {
                            pinmanager.notify(new PinManagerPinMessage(attributes, info, QOS_PIN_REQUEST_ID, -1));
                        }
                        break;
                    case QosManagement.DISK:
                        switch (locality) {
                        case ONLINE:
                            // do nothing
                            break;

                        default:
                            throw new BadRequestException("Unsupported QoS transition");
                        }
                        break;
                    case QosManagement.TAPE:
                        if (locality != NEARLINE && locality != ONLINE_AND_NEARLINE) {
                            migrationPolicyEngine.adjust();
                        }
                        PinManagerUnpinMessage messageUnpin = new PinManagerUnpinMessage(attributes.getPnfsId());
                        messageUnpin.setRequestId(QOS_PIN_REQUEST_ID);
                        pinmanager.notify(messageUnpin);
                        break;

                    default:
                        throw new BadRequestException("Unknown target QoS: " + targetQos);
                    }
                    break;

                default:
                    throw new BadRequestException("Unknown action: " + action);
            }
        } catch (FileNotFoundCacheException e) {
            throw new NotFoundException(e);
        } catch (PermissionDeniedCacheException e) {
            if (RequestUser.isAnonymous()) {
                throw new NotAuthorizedException(e);
            } else {
                throw new ForbiddenException(e);
            }
        } catch (JSONException | CacheException | InterruptedException | NoRouteToCellException e) {
            throw new BadRequestException(e.getMessage(), e);
        }
        return successfulResponse(Response.Status.CREATED);
    }

    @DELETE
    @Path("{value : .*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteFileEntry(@PathParam("value") String requestPath) throws CacheException {

        PnfsHandler handler = HandlerBuilders.roleAwarePnfsHandler(ctx, request);
        PathMapper pathMapper = ServletContextHandlerAttributes.getPathMapper(ctx);
        FsPath path = pathMapper.asDcachePath(request, requestPath, ForbiddenException::new);

        try {
            handler.deletePnfsEntry(path.toString());

        } catch (FileNotFoundCacheException e) {
            throw new NotFoundException(e);
        } catch (PermissionDeniedCacheException e) {
            if (RequestUser.isAnonymous()) {
                throw new NotAuthorizedException(e);
            } else {
                throw new ForbiddenException(e);
            }
        } catch (JSONException | IllegalArgumentException | CacheException e) {
            throw new BadRequestException(e);
        } catch (Exception e) {
            throw new InternalServerErrorException(e);
        }
        return successfulResponse(Response.Status.OK);
    }
}