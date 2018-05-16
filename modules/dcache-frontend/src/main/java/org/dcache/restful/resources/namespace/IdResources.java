/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.restful.resources.namespace;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Named;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import java.util.EnumSet;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;

import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.CellStub;
import org.dcache.http.PathMapper;
import org.dcache.namespace.FileAttribute;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.restful.providers.JsonFileAttributes;
import org.dcache.restful.util.HandlerBuilders;
import org.dcache.restful.util.namespace.NamespaceUtils;
import org.dcache.vehicles.FileAttributes;

/**
 * <p>RESTful API to which allows one to map the pnfsid to (a) path.</p>
 *
 * @version v1.0
 */
@Component
@Api(value = "namespace", authorizations = {@Authorization("basicAuth")})
@Path("/id")
public class IdResources {
    @Context
    private HttpServletRequest request;

    @Inject
    private PathMapper pathMapper;

    @Inject
    private PoolMonitor poolMonitor;

    @Inject
    @Named("pinManagerStub")
    private CellStub pinmanager;

    @Inject
    @Named("pnfs-stub")
    private CellStub pnfsmanager;


    @GET
    @ApiOperation(value="Discover information about a file from the PNFS-ID.",
            notes="Retrieve all file attributes plus the file's path from the "
                    + "given PNFS-ID.")
    @ApiResponses({
                @ApiResponse(code = 400, message = "Bad pnsfid"),
                @ApiResponse(code = 404, message = "Not Found"),
                @ApiResponse(code = 500, message = "Internal Server Error"),
            })
    @Path("{pnfsid}")
    @Produces(MediaType.APPLICATION_JSON)
    public JsonFileAttributes getAttributes(@ApiParam("The PNFS-ID of a file or directory.")
                                            @PathParam("pnfsid") String value)
    {
        Set<FileAttribute> attributeSet = EnumSet.allOf(FileAttribute.class);
        JsonFileAttributes result = new JsonFileAttributes();
        PnfsHandler handler = HandlerBuilders.roleAwarePnfsHandler(pnfsmanager);

        try {
            PnfsId id = new PnfsId(value);
            FileAttributes attributes = handler.getFileAttributes(id, attributeSet);

            /*
             * Caveat: Because there is a possibility that a given file could have
             * a number of hard-linked paths, and that the current path finder
             * code selects only the most recently created path/link, there
             * is a possibility of getting a path which may not correspond
             * to the expected one.
             */
            FsPath path = handler.getPathByPnfsId(id);

            /*
             * Since FileResources maps according to the effective root,
             * we should return the path in the same form here.
             */
            result.setPath(pathMapper.asRequestPath(request, path));

            String name = path.name();
            result.setFileName(name);

            NamespaceUtils.chimeraToJsonAttributes(name,
                                                   result,
                                                   attributes,
                                                   true,
                                                   true,
                                                   true,
                                                   request,
                                                   poolMonitor);

            NamespaceUtils.addQoSAttributes(result, attributes, request, poolMonitor, pinmanager);
        } catch (IllegalArgumentException e) {
            throw new BadRequestException("Bad pnsfid " + value, e);
        } catch (FileNotFoundCacheException e) {
            throw new NotFoundException(e);
        } catch (CacheException | InterruptedException | NoRouteToCellException e) {
            throw new InternalServerErrorException(e);
        }

        return result;
    }
}
