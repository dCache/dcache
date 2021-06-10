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
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Named;
import javax.naming.directory.NoSuchAttributeException;
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
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import diskCacheV111.util.AttributeExistsCacheException;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NoAttributeCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.vehicles.PnfsWriteExtendedAttributesMessage;
import diskCacheV111.vehicles.PnfsWriteExtendedAttributesMessage.Mode;

import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Exceptions;

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
                                                @ApiParam("Whether to include extended attributes.")
                                                @QueryParam("xattr") boolean isXattr,
                                                @ApiParam("Limit number of replies in directory listing.")
                                                @QueryParam("limit") String limit,
                                                @ApiParam("Number of entries to skip in directory listing.")
                                                @QueryParam("offset") String offset) throws CacheException
    {
        JsonFileAttributes fileAttributes = new JsonFileAttributes();
        Set<FileAttribute> attributes =
                        NamespaceUtils.getRequestedAttributes(isLocality,
                                                              isLocations,
                                                              isQos,
                                                              false);
        PnfsHandler handler = HandlerBuilders.roleAwarePnfsHandler(pnfsmanager);
        FsPath path = pathMapper.asDcachePath(request, requestPath, ForbiddenException::new);
        try {

            FileAttributes namespaceAttributes = handler.getFileAttributes(path, attributes);
            NamespaceUtils.chimeraToJsonAttributes(path.name(), fileAttributes,
                                                   namespaceAttributes,
                                                   isLocality, isLocations,
                                                   false, isXattr,
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
                                                           false, isXattr,
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
            LOG.warn(Exceptions.meaningfulMessage(ex));
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
                @ApiResponse(code = 409, message = "Attribute already exists"),
                @ApiResponse(code = 409, message = "No such attribute"),
                @ApiResponse(code = 500, message = "Internal Server Error"),
            })
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response cmrResources(@ApiParam(value="Path of file or directory to be modified.", required=true)
                                 @PathParam("path") String requestPath,
                                 @ApiParam(value = "A JSON object that has an 'action' "
                                             + "item with a String value.\n"
                                             + "\n"
                                             + "If the 'action' value is 'mkdir' "
                                             + "then a new directory is created "
                                             + "with the name taken from the "
                                             + "value of the JSON object 'name' "
                                             + "item.  This directory is created "
                                             + "within the supplied path parameter, "
                                             + "which must be an existing directory.\n"
                                             + "\n"
                                             + "If action is 'mv' then the file "
                                             + "or directory specified by the path "
                                             + "parameter is moved and/or "
                                             + "renamed with the value of the JSON "
                                             + "object 'destination' item describing "
                                             + "the final location.  If the "
                                             + "'destination' value is a relative "
                                             + "path then it is resolved against "
                                             + "the path parameter value.\n"
                                             + "\n"
                                             + "If action is 'qos' then the value "
                                             + "of the JSON object 'target' item "
                                             + "describes the desired QoS."
                                             + "\n"
                                             + "If action is 'rm-xattr' then "
                                             + "extended attributes of a file "
                                             + "or directory are removed as "
                                             + "given by the 'names' item.  The "
                                             + "'names' value is either a "
                                             + "string or an array of strings."
                                             + "\n"
                                             + "If action is 'set-xattr' then "
                                             + "extended attributes are created "
                                             + "or modified.  The optional "
                                             + "'mode' item controls whether to "
                                             + "create a new attribute (CREATE), "
                                             + "to modify an existing attribute "
                                             + "(MODIFY), or to assign the value "
                                             + "by either creating a new "
                                             + "attribute or modifying an "
                                             + "existing attribute (EITHER).  "
                                             + "EITHER is the default mode.  The "
                                             + "'attributes' item value is a JSON "
                                             + "Object with the new attributes,"
                                             + "where the JSON Object's key is "
                                             + "the attribute name and the "
                                             + "corresponding JSON Object's "
                                             + "value is this attribute's value."
                                             + "\n"
                                             + "If action is 'chgrp' then the "
                                             + "command requests the change of "
                                             + "group-owner of the target file "
                                             + "or directory.  The value of the "
                                             + "JSON object 'gid' item is the "
                                             + "numerical value of the desired "
                                             + "new group-owner."
                                             + "\n"
                                             + "If action is 'chmod' then the "
                                             + "command reqests the change of "
                                             + "the target file's or directory's "
                                             + "permissions.  The value of the "
                                             + "JSON object 'mode' item is the "
                                             + "numerical value of the desired "
                                             + "mode.",
                                         required = true,
                                         examples = @Example({
                                             @ExampleProperty(mediaType = "MV",
                                                     value = "{\n"
                                                         + "    \"action\" : \"mv\",\n"
                                                         + "    \"destination\" : \"../foo\"\n"
                                                         + "}"),
                                             @ExampleProperty(mediaType = "MKDIR",
                                                     value = "{\n"
                                                         + "    \"action\" : \"mkdir\",\n"
                                                         + "    \"name\" : \"new-subdir\"\n"
                                                         + "}"),
                                             @ExampleProperty(mediaType = "QOS",
                                                     value = "{\n"
                                                         + "    \"action\" : \"qos\",\n"
                                                         + "    \"target\" : \"DISK+TAPE\"\n"
                                                         + "}"),
                                             @ExampleProperty(mediaType = "SET-XATTR",
                                                     value = "{\n"
                                                         + "    \"action\" : \"set-xattr\",\n"
                                                         + "    \"mode\" : \"CREATE\",\n"
                                                         + "    \"attributes\" : {\n"
                                                         + "        \"attr-1\": \"First attribute\",\n"
                                                         + "        \"attr-2\": \"Second attribute\"\n"
                                                         + "    }\n"
                                                         + "}"),
                                             @ExampleProperty(mediaType = "RM-XATTR",
                                                     value = "{\n"
                                                         + "    \"action\" : \"rm-xattr\",\n"
                                                         + "    \"names\" : [\n"
                                                         + "        \"attr-1\",\n"
                                                         + "        \"attr-2\"\n"
                                                         + "    ]\n"
                                                         + "}"),
                                             @ExampleProperty(mediaType = "CHGRP",
                                                     value = "{\n"
                                                         + "    \"action\" : \"chgrp\",\n"
                                                         + "    \"gid\" : 1000\n"
                                                         + "}"),
                                             @ExampleProperty(mediaType = "CHMOD",
                                                     value = "{\n"
                                                         + "    \"action\" : \"chmod\",\n"
                                                         + "    \"mode\" : 493\n"
                                                         + "}")}))
                                 String requestPayload)
    {
        try {
            JSONObject reqPayload = new JSONObject(requestPayload);
            String action = (String) reqPayload.get("action");
            PnfsHandler pnfsHandler = HandlerBuilders.roleAwarePnfsHandler(pnfsmanager);
            FsPath path = pathMapper.asDcachePath(request, requestPath, ForbiddenException::new);
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
                                    .adjustQoS(path,
                                               targetQos, request.getRemoteHost());
                    break;
                case "rm-xattr":
                    Object namesArgument = reqPayload.get("names");
                    if (namesArgument instanceof String) {
                        pnfsHandler.removeExtendedAttribute(path, (String)namesArgument);
                    } else if (namesArgument instanceof JSONArray) {
                        JSONArray namesArray = (JSONArray)namesArgument;
                        List<String> names = new ArrayList<>(namesArray.length());
                        for (int i = 0; i < namesArray.length(); i++) {
                            names.add(namesArray.getString(i));
                        }
                        pnfsHandler.removeExtendedAttribute(path, names);
                    } else {
                        throw new JSONException("\"names\" is not a String or an array");
                    }
                    break;
                case "set-xattr":
                    String modeString = reqPayload.optString("mode", "EITHER");
                    Mode xattrSetMode = modeOf(modeString);

                    JSONObject attributeOject = reqPayload.getJSONObject("attributes");
                    Map<String,byte[]> attributes = new HashMap<>(attributeOject.length());
                    for (String key : attributeOject.keySet()) {
                        String value = attributeOject.getString(key);
                        attributes.put(key, value.getBytes(StandardCharsets.UTF_8));
                    }
                    pnfsHandler.writeExtendedAttribute(path, attributes, xattrSetMode);
                    break;
                case "chgrp":
                    int gid = reqPayload.getInt("gid");
                    pnfsHandler.setFileAttributes(path, FileAttributes.ofGid(gid));
                    break;
                case "chmod":
                    int mode = reqPayload.getInt("mode");
                    pnfsHandler.setFileAttributes(path, FileAttributes.ofMode(mode));
                    break;
                default:
                    throw new UnsupportedOperationException("No such action " + action);
            }
        } catch (FileNotFoundCacheException e) {
            throw new NotFoundException(e);
        } catch (PermissionDeniedCacheException e) {
            if (RequestUser.isAnonymous()) {
                throw new NotAuthorizedException(e);
            } else {
                throw new ForbiddenException(e);
            }
        } catch (AttributeExistsCacheException e) {
            throw new WebApplicationException(Response.status(409, "Attribute already exist")
                    .build());
        } catch (NoAttributeCacheException e) {
            throw new WebApplicationException(Response.status(409, "No such attribute")
                    .build());
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

    private Mode modeOf(String value) throws JSONException
    {
        try {
            return Mode.valueOf(value);
        } catch (IllegalArgumentException e) {
            throw new JSONException("Unknown mode \"" + value + "\", must be"
                    + " one of " + Arrays.asList(Mode.values()));
        }
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
            LOG.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        }
        return successfulResponse(Response.Status.OK);
    }
}
