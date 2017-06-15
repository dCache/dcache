package org.dcache.restful.resources.namespace;

import com.google.common.collect.Range;
import org.json.JSONException;
import org.json.JSONObject;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileLocality;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.vehicles.HttpProtocolInfo;
import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.auth.Subjects;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.pinmanager.PinManagerCountPinsMessage;
import org.dcache.pinmanager.PinManagerPinMessage;
import org.dcache.pinmanager.PinManagerUnpinMessage;
import org.dcache.poolmanager.RemotePoolMonitor;
import org.dcache.restful.providers.JsonFileAttributes;
import org.dcache.restful.qos.QosManagement;
import org.dcache.restful.util.ServletContextHandlerAttributes;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryStream;
import org.dcache.util.list.ListDirectoryHandler;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.dcache.restful.providers.SuccessfulResponse.successfulResponse;
import static org.dcache.restful.util.Preconditions.checkRequest;

/**
 * RestFul API to  provide files/folders manipulation operations
 *
 * @version v1.0
 */
@Path("/namespace")
public class FileResources {

    private final String QOS_PIN_REQUEST_ID = "qos";

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
    public JsonFileAttributes getFileAttributes(@PathParam("value") String value,
                                                @DefaultValue("false")
                                                @QueryParam("children") boolean isList,
                                                @DefaultValue("false")
                                                @QueryParam("locality") boolean isLocality,
                                                @DefaultValue("false")
                                                @QueryParam("qos") boolean isQos) throws CacheException
    {
        JsonFileAttributes fileAttributes = new JsonFileAttributes();
        Set<FileAttribute> attributes = EnumSet.allOf(FileAttribute.class);
        PnfsHandler handler = ServletContextHandlerAttributes.getPnfsHandler(ctx);

        FsPath path;
        if (value == null || value.isEmpty()) {
            path = FsPath.ROOT;
        } else {
            path = FsPath.create(FsPath.ROOT + value);
        }


        try {

            FileAttributes namespaceAttrributes = handler.getFileAttributes(path, attributes);
            chimeraToJsonAttributes(fileAttributes, namespaceAttrributes, isLocality);
            if (isQos) {
                addQoSAttributes(fileAttributes, namespaceAttrributes);
            }


            // fill children list id it's a directory and listing is requested
            if (namespaceAttrributes.getFileType() == FileType.DIR && isList) {

                List<JsonFileAttributes> children = new ArrayList<>();

                ListDirectoryHandler listDirectoryHandler = ServletContextHandlerAttributes.getListDirectoryHandler(ctx);

                DirectoryStream stream = listDirectoryHandler.list(
                        ServletContextHandlerAttributes.getSubject(),
                        ServletContextHandlerAttributes.getRestriction(),
                        path,
                        null,
                        Range.<Integer>all(),
                        attributes);

                for (DirectoryEntry entry : stream) {
                    String fName = entry.getName();

                    JsonFileAttributes childrenAttributes = new JsonFileAttributes();

                    chimeraToJsonAttributes(childrenAttributes, entry.getFileAttributes(), isLocality);
                    childrenAttributes.setFileName(fName);
                    if (isQos) {
                        addQoSAttributes(childrenAttributes, entry.getFileAttributes());
                    }
                    children.add(childrenAttributes);
                }

                fileAttributes.setChildren(children);

            }

        } catch (FileNotFoundCacheException e) {
            throw new NotFoundException(e);
        } catch (PermissionDeniedCacheException e) {
            if (Subjects.isNobody(ServletContextHandlerAttributes.getSubject())) {
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
    public Response cmrResources (@PathParam("value") String path, String requestPayload)
    {
        try {
            JSONObject reqPayload = new JSONObject(requestPayload);
            String action = (String) reqPayload.get("action");
            PnfsHandler handler = ServletContextHandlerAttributes.getPnfsHandler(ctx);
            switch (action) {
                case "mkdir":
                    String folderName = (String) reqPayload.get("name");
                    checkRequest(!folderName.contains("/"), "The folderName cannot contain forward slash.");
                    checkRequest(!folderName.isEmpty(), "The folderName cannot be empty.");

                    String newPath;
                    if (path == null || path.isEmpty()) {
                        newPath = "/" + folderName;
                    } else {
                        newPath = "/" + path + "/" + folderName;
                    }

                    handler.createPnfsDirectory(newPath);
                    break;

                case "mv":
                    String dest = (String) reqPayload.get("destination");
                    FsPath source = FsPath.ROOT.resolve(path);
                    FsPath target = source.parent().resolve(dest);
                    handler.renameEntry(source.toString(), target.toString(), true);
                    break;

                case "qos":
                    String targetQos = reqPayload.getString("target");

                    // FIXME: which attributes do we actually need?
                    FileAttributes attributes = handler.getFileAttributes(FsPath.ROOT.resolve(path), EnumSet.allOf(FileAttribute.class));
                    RemotePoolMonitor monitor = ServletContextHandlerAttributes.getRemotePoolMonitor(ctx);
                    FileLocality locality = monitor.getFileLocality(attributes, request.getRemoteHost());

                    if (locality == FileLocality.NONE) {
                        throw new BadRequestException("Transition for directories not supported");
                    }

                    CellStub pinmanager = ServletContextHandlerAttributes.getPinManager(ctx);
                    switch (targetQos) {
                    case QosManagement.DISK_TAPE:
                        switch (locality) {
                        case NEARLINE:
                        case ONLINE_AND_NEARLINE:
                            ProtocolInfo info = new HttpProtocolInfo("Http", 1, 1,
                                            new InetSocketAddress(request.getRemoteHost(), 0),
                                            null, null, null,
                                            URI.create("http://"+request.getRemoteHost()+"/"));
                            PinManagerPinMessage message =
                                    new PinManagerPinMessage(attributes, info,
                                            QOS_PIN_REQUEST_ID, -1);
                            pinmanager.notify(message);
                            break;

                        default:
                            throw new BadRequestException("Unsupported QoS transition");
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

                    case QosManagement.TAPE:
                        switch (locality) {
                        case ONLINE_AND_NEARLINE:
                            PinManagerUnpinMessage message = new PinManagerUnpinMessage(attributes.getPnfsId());
                            message.setRequestId(QOS_PIN_REQUEST_ID);
                            pinmanager.notify(message);
                            break;

                        case NEARLINE:
                            break; // Nothing to do.

                        default:
                            throw new BadRequestException("Unsupported QoS transition");
                        }
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
            if (Subjects.isNobody(ServletContextHandlerAttributes.getSubject())) {
                throw new NotAuthorizedException(e);
            } else {
                throw new ForbiddenException(e);
            }
        } catch (JSONException | CacheException e) {
            throw new BadRequestException(e.getMessage(), e);
        }
        return successfulResponse(Response.Status.CREATED);
    }

    /**
     * Map  returned fileAttributes to JsonFileAttributes object.
     *
     * @param fileAttributes       will be mapped to the FileAttributes
     * @param namespaceAttrributes FileAttributes returned by the request
     * @param isLocality           used to check weather user queried  locality to the file
     */
    private void chimeraToJsonAttributes(JsonFileAttributes fileAttributes,
                                         FileAttributes namespaceAttrributes,
                                         boolean isLocality) throws CacheException {
        fileAttributes.setMtime(namespaceAttrributes.getModificationTime());
        fileAttributes.setCreationTime(namespaceAttrributes.getCreationTime());
        if (namespaceAttrributes.isDefined(FileAttribute.SIZE)) {
            fileAttributes.setSize(namespaceAttrributes.getSize());
        }
        fileAttributes.setFileType(namespaceAttrributes.getFileType());

        // when user set locality param. in the request, the locality should be returned only for directories
        if (isLocality && namespaceAttrributes.getFileType() != FileType.DIR) {

            String client = request.getRemoteHost();
            RemotePoolMonitor remotePoolMonitor = ServletContextHandlerAttributes.getRemotePoolMonitor(ctx);
            FileLocality fileLocality = remotePoolMonitor.getFileLocality(namespaceAttrributes, client);
            fileAttributes.setFileLocality(fileLocality);
        }
    }

    private void addQoSAttributes(JsonFileAttributes json, FileAttributes attributes)
            throws CacheException, NoRouteToCellException, InterruptedException
    {
        if (Subjects.isNobody(ServletContextHandlerAttributes.getSubject())) {
            throw new PermissionDeniedCacheException("Permission denied");
        }

        CellStub pinmanager = ServletContextHandlerAttributes.getPinManager(ctx);
        boolean isPinned = pinmanager.sendAndWait(new PinManagerCountPinsMessage(attributes.getPnfsId())).getCount() != 0;

        RemotePoolMonitor monitor = ServletContextHandlerAttributes.getRemotePoolMonitor(ctx);
        FileLocality locality = monitor.getFileLocality(attributes, request.getRemoteHost());
        switch (locality) {
        case NEARLINE:
            json.setCurrentQos(QosManagement.TAPE);
            if (isPinned) {
                json.setTargetQos(QosManagement.DISK_TAPE);
            }
            break;

        case ONLINE:
            json.setCurrentQos(QosManagement.DISK);
            break;

        case ONLINE_AND_NEARLINE:
            json.setCurrentQos(isPinned ? QosManagement.DISK_TAPE : QosManagement.TAPE);
            break;

        case NONE: // NONE implies the target is a directory.
            json.setCurrentQos(attributes.getAccessLatency() == AccessLatency.ONLINE ? QosManagement.DISK : QosManagement.TAPE);
            break;

        case UNAVAILABLE:
            json.setCurrentQos(QosManagement.UNAVAILABLE);
            break;

        // LOST is currently not used by dCache
        case LOST:
        default:
            // error cases
            throw new InternalServerErrorException("Unexpected file locality: " + locality);
        }
    }
}
