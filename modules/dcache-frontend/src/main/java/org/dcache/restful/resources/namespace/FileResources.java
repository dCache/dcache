package org.dcache.restful.resources.namespace;

import com.google.common.collect.Range;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.Example;
import io.swagger.annotations.ExampleProperty;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Named;
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

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;

import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.CellStub;
import org.dcache.http.PathMapper;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.qos.QoSTransitionEngine;
import org.dcache.restful.providers.JsonFileAttributes;
import org.dcache.restful.util.HandlerBuilders;
import org.dcache.restful.util.HttpServletRequests;
import org.dcache.restful.util.RequestUser;
import org.dcache.restful.util.namespace.NamespaceUtils;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryStream;
import org.dcache.util.list.ListDirectoryHandler;
import org.dcache.vehicles.FileAttributes;

import static org.dcache.restful.providers.SuccessfulResponse.successfulResponse;

/**
 * RestFul API to  provide files/folders manipulation operations.
 */
@Api(value = "namespace", authorizations = {@Authorization("basicAuth")})
@Component
@Path("/namespace")
public class FileResources {
    private static final Logger LOG = LoggerFactory.getLogger(FileResources.class);

    /*
     * Used to get fully qualified name of the client
     * or the last proxy that sent the request.
     * Later used for querying locality of the file.
     */
    @Context
    private HttpServletRequest request;

    @Inject
    private PoolMonitor poolMonitor;

    @Inject
    private PathMapper pathMapper;

    @Inject
    private ListDirectoryHandler listDirectoryHandler;

    @Inject
    @Named("pool-manager-stub")
    private CellStub poolmanager;

    @Inject
    @Named("pinManagerStub")
    private CellStub pinmanager;

    @Inject
    @Named("pnfs-stub")
    private CellStub pnfsmanager;

    @GET
    @ApiOperation(value="Find metadata and optionally directory contents.",
            notes="The method offers the possibility to list the content of a "
                    + "directory in addition to providing metadata of a "
                    + "specified file or directory.")
    @ApiResponses({
                @ApiResponse(code = 401, message = "Unauthorized"),
                @ApiResponse(code = 403, message = "Forbidden"),
                @ApiResponse(code = 404, message = "Not Found"),
                @ApiResponse(code = 500, message = "Internal Server Error"),
            })
    @Path("{path : .*}")
    @Produces(MediaType.APPLICATION_JSON)
    public JsonFileAttributes getFileAttributes(@ApiParam("Path of file or directory.")
                                                @PathParam("path") String requestPath,
                                                @ApiParam("Whether to include directory listing.")
                                                @DefaultValue("false")
                                                @QueryParam("children") boolean isList,
                                                @ApiParam("Whether to include file locality information.")
                                                @DefaultValue("false")
                                                @QueryParam("locality") boolean isLocality,
                                                @ApiParam(value="Whether to include replica locations.")
                                                @QueryParam("locations") boolean isLocations,
                                                @ApiParam(value="Whether to include quality of service.")
                                                @DefaultValue("false")
                                                @QueryParam("qos") boolean isQos,
                                                @ApiParam("Limit number of replies in directory listing.")
                                                @QueryParam("limit") String limit,
                                                @ApiParam("Number of entries to skip in directory listing.")
                                                @QueryParam("offset") String offset) throws CacheException
    {
        JsonFileAttributes fileAttributes = new JsonFileAttributes();
        Set<FileAttribute> attributes = EnumSet.allOf(FileAttribute.class);
        PnfsHandler handler = HandlerBuilders.roleAwarePnfsHandler(pnfsmanager);
        FsPath path = pathMapper.asDcachePath(request, requestPath, ForbiddenException::new);
        try {

            FileAttributes namespaceAttributes = handler.getFileAttributes(path, attributes);
            NamespaceUtils.chimeraToJsonAttributes(path.name(), fileAttributes,
                                                   namespaceAttributes,
                                                   isLocality, isLocations,
                                                   false,
                                                   request, poolMonitor);
            if (isQos) {
                NamespaceUtils.addQoSAttributes(fileAttributes,
                                                namespaceAttributes,
                                                request, poolMonitor, pinmanager);
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
                                                           request, poolMonitor);
                    childrenAttributes.setFileName(fName);
                    if (isQos) {
                        NamespaceUtils.addQoSAttributes(childrenAttributes,
                                                        entry.getFileAttributes(),
                                                        request, poolMonitor, pinmanager);
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
    @ApiOperation(value="Modify a file or directory.")
    @Path("{path : .*}")
    @ApiResponses({
                @ApiResponse(code = 400, message = "Transition for directories not supported"),
                @ApiResponse(code = 400, message = "Unsupported QoS transition"),
                @ApiResponse(code = 400, message = "Unknown target QoS"),
                @ApiResponse(code = 400, message = "Unknown action"),
                @ApiResponse(code = 401, message = "Unauthorized"),
                @ApiResponse(code = 403, message = "Forbidden"),
                @ApiResponse(code = 404, message = "Not Found"),
                @ApiResponse(code = 500, message = "Internal Server Error"),
            })
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response cmrResources(@ApiParam(value="Path of file or directory to be modified.", required=true)
                                 @PathParam("path") String requestPath,
                                 @ApiParam(value = "A JSON object that has an 'action' "
                                             + "item with a String value.\n"
                                             + "If the 'action' value is 'mkdir' "
                                             + "then a new directory is created "
                                             + "with the name taken from the "
                                             + "value of the JSON object 'name' "
                                             + "item.  This directory is created "
                                             + "within the supplied path parameter, "
                                             + "which must be an existing directory.\n"
                                             + "If action is 'mv' then the file "
                                             + "or directory specified by the path "
                                             + "parameter is moved and/or "
                                             + "renamed with the value of the JSON "
                                             + "object 'destination' item describing "
                                             + "the final location.  If the "
                                             + "'destination' value is a relative "
                                             + "path then it is resolved against "
                                             + "the path parameter value.\n"
                                             + "If action is 'qos' then the value "
                                             + "of the JSON object 'target' item "
                                             + "describes the desired QoS.",
                                         required = true,
                                         examples = @Example({
                                             @ExampleProperty("{\n"
                                                         + "    \"action\" : \"mv\""
                                                         + "    \"destination\" : \"../foo\""
                                                         + "}")}))
                                 String requestPayload)
    {
        try {
            JSONObject reqPayload = new JSONObject(requestPayload);
            String action = (String) reqPayload.get("action");
            PnfsHandler pnfsHandler = HandlerBuilders.roleAwarePnfsHandler(pnfsmanager);
            FsPath path = pathMapper.asDcachePath(request, requestPath, ForbiddenException::new);
            FileAttributes attributes
                            = pnfsHandler.getFileAttributes(path,
                                                            EnumSet.allOf(FileAttribute.class));
            switch (action) {
                case "mkdir":
                    String name = (String) reqPayload.get("name");
                    FsPath.checkChildName(name, BadRequestException::new);
                    pnfsHandler = HandlerBuilders.pnfsHandler(pnfsmanager); // FIXME: non-role identity to ensure correct ownership
                    pnfsHandler.createPnfsDirectory(path.child(name).toString());
                    break;
                case "mv":
                    String dest = (String) reqPayload.get("destination");
                    FsPath target = pathMapper.resolve(request, path, dest);
                    pnfsHandler.renameEntry(path.toString(), target.toString(), true);
                    break;
                case "qos":
                    String targetQos = reqPayload.getString("target");
                    new QoSTransitionEngine(poolmanager,
                                            poolMonitor,
                                            pnfsHandler,
                                            pinmanager)
                                    .adjustQoS(path, attributes,
                                               targetQos, request.getRemoteHost());
            }
        } catch (FileNotFoundCacheException e) {
            throw new NotFoundException(e);
        } catch (PermissionDeniedCacheException e) {
            if (RequestUser.isAnonymous()) {
                throw new NotAuthorizedException(e);
            } else {
                throw new ForbiddenException(e);
            }
        } catch (UnsupportedOperationException |
                        URISyntaxException |
                        JSONException |
                        CacheException |
                        InterruptedException |
                        NoRouteToCellException e) {
            throw new BadRequestException(e.getMessage(), e);
        }
        return successfulResponse(Response.Status.CREATED);
    }

    @DELETE
    @Path("{path : .*}")
    @ApiOperation(value="delete a file or directory",
            notes="If a directory is targeted then the directory must already be empty.")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiResponses({
                @ApiResponse(code = 401, message = "Unauthorized"),
                @ApiResponse(code = 403, message = "Forbidden"),
                @ApiResponse(code = 404, message = "Not Found"),
                @ApiResponse(code = 500, message = "Internal Server Error"),
            })
    public Response deleteFileEntry(@ApiParam(value="Path of file or directory.", required=true)
                                    @PathParam("path") String requestPath) throws CacheException
    {
        PnfsHandler handler = HandlerBuilders.roleAwarePnfsHandler(pnfsmanager);
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
