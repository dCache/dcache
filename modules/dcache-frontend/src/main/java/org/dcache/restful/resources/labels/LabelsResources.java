/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.dcache.restful.resources.labels;

import com.google.common.collect.Range;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Exceptions;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Named;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import org.dcache.cells.CellStub;
import org.dcache.http.PathMapper;
import org.dcache.namespace.FileAttribute;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.restful.providers.JsonFileAttributes;
import org.dcache.restful.util.HttpServletRequests;
import org.dcache.restful.util.RequestUser;
import org.dcache.restful.util.namespace.NamespaceUtils;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryStream;
import org.dcache.util.list.ListDirectoryHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;


/**
 * RestFul API to  provide files/folders manipulation operations.
 */
@Api(value = "labels", authorizations = {@Authorization("basicAuth")})
@Component
@Path("/labels")
public class LabelsResources {

    private static final Logger LOGGER = LoggerFactory.getLogger(LabelsResources.class);

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
    @ApiOperation(value = "Find metadata and optionally virtual directory contents.",
          notes = "The method offers the possibility to list the content of a virtual"
                + "directory for labels.")
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
          @ApiParam("List virtual dir.")
          @DefaultValue("false")
          @QueryParam("locality") boolean isLocality,
          @ApiParam(value = "Whether to include replica locations.")
          @QueryParam("locations") boolean isLocations,
          @ApiParam(value = "Whether to include quality of service.")
          @DefaultValue("false")
          @QueryParam("qos") boolean isQos,
          @ApiParam("Whether to include extended attributes.")
          @QueryParam("xattr") boolean isXattr,
          @ApiParam("Whether to include labels.")
          @QueryParam("labels") boolean isLabels,
          @ApiParam("Limit number of replies in directory listing.")
          @QueryParam("limit") String limit,
          @ApiParam("Number of entries to skip in directory listing.")
          @QueryParam("offset") String offset) throws CacheException {
        JsonFileAttributes fileAttributes = new JsonFileAttributes();
        Set<FileAttribute> attributes =
              NamespaceUtils.getRequestedAttributes(isLocality,
                    isLocations,
                    false,
                    false);

        FsPath path = pathMapper.asDcachePath(request, requestPath, ForbiddenException::new);

        Range<Integer> range;
        try {
            int lower = (offset == null) ? 0 : Integer.parseInt(offset);
            int ceiling = (limit == null) ? Integer.MAX_VALUE : Integer.parseInt(limit);
            if (ceiling < 0 || lower < 0) {
                throw new BadRequestException("limit and offset can not be less than zero.");
            }
            range = (Integer.MAX_VALUE - lower < ceiling) ? Range.atLeast(lower)
                  : Range.closedOpen(lower, lower + ceiling);
        } catch (NumberFormatException e) {
            throw new BadRequestException("limit and offset must be an integer value.");
        }
        try {
            List<JsonFileAttributes> children = new ArrayList<>();

            DirectoryStream stream = listDirectoryHandler.listVirtualDirectory(
                  HttpServletRequests.roleAwareSubject(request),
                  HttpServletRequests.roleAwareRestriction(request),
                  path,
                  range,
                  attributes);

            for (DirectoryEntry entry : stream) {
                String fPath = entry.getName();
                String fName = entry.getName();


                JsonFileAttributes childrenAttributes = new JsonFileAttributes();

                NamespaceUtils.chimeraToJsonAttributes(fPath,
                      childrenAttributes,
                      entry.getFileAttributes(),
                      isLocality, isLocations, isLabels,
                      false, isXattr,
                      request, poolMonitor);
                childrenAttributes.setSourcePath(fPath);
                childrenAttributes.setFileName(fName);

                if (isQos) {
                    NamespaceUtils.addQoSAttributes(childrenAttributes,
                          entry.getFileAttributes(),
                          request, poolMonitor, pinmanager);
                }
                children.add(childrenAttributes);
            }

            fileAttributes.setChildren(children);


        } catch (FileNotFoundCacheException e) {
            throw new NotFoundException(e);
        } catch (PermissionDeniedCacheException e) {
            if (RequestUser.isAnonymous()) {
                throw new NotAuthorizedException(e);
            } else {
                throw new ForbiddenException(e);
            }
        } catch (CacheException | InterruptedException | NoRouteToCellException ex) {
            LOGGER.warn(Exceptions.meaningfulMessage(ex));
            throw new InternalServerErrorException(ex);
        }
        return fileAttributes;
    }
}
