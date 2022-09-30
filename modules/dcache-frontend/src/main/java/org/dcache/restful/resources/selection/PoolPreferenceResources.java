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
package org.dcache.restful.resources.selection;

import diskCacheV111.poolManager.PoolPreferenceLevel;
import diskCacheV111.util.CacheException;
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
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.dcache.cells.CellStub;
import org.dcache.restful.providers.selection.PreferenceResult;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * <p>RESTful API to the {@link diskCacheV111.poolManager.PoolManagerV5}, in
 * order to deliver pool preference (matching) information.</p>
 *
 * @version v1.0
 */
@Component
@Api(value = "poolmanager", authorizations = {@Authorization("basicAuth")})
@Path("/pool-preferences")
public final class PoolPreferenceResources {

    private static final Logger LOGGER = LoggerFactory.getLogger(PoolPreferenceResources.class);

    @Inject
    @Named("pool-manager-stub")
    private CellStub poolManager;

    @GET
    @ApiOperation("Describe the pools selected by a particular request.")
    @ApiResponses({
          @ApiResponse(code = 400, message = "Bad Request"),
          @ApiResponse(code = 500, message = "Internal Server Error"),
    })
    @Produces(MediaType.APPLICATION_JSON)
    public List<PreferenceResult> match(
          @ApiParam(value = "The I/O direction.", allowableValues = "READ,CACHE,WRITE,P2P")
          @DefaultValue("READ")
          @QueryParam("ioDirection") String ioDirection,
          @ApiParam("The name of the storage class.")
          @QueryParam("storageClass") String storageClass,
          @ApiParam("The name of the hsm type.")
          @QueryParam("hsm") String hsm,
          @ApiParam("The name of the cache class.")
          @DefaultValue("")
          @QueryParam("cacheClass") String cacheClass,
          @ApiParam("The link group unit.")
          @DefaultValue("")
          @QueryParam("linkGroup") String linkGroup,
          @ApiParam("The pnfsId of a file.")
          @DefaultValue("")
          @QueryParam("pnfsId") String pnfsId,
          @ApiParam("The path of a file.")
          @DefaultValue("")
          @QueryParam("path") String path,
          @ApiParam("The name of the net unit.")
          @DefaultValue("*")
          @QueryParam("net") String net,
          @ApiParam("The name of the protocol unit.")
          @DefaultValue("*")
          @QueryParam("protocol") String protocol) {
        try {
            StringBuilder command = new StringBuilder("psux match ")
                  .append(ioDirection)
                  .append(storageClass == null ? "" : " -storageClass=" + storageClass)
                  .append(hsm == null ? "" : " -hsm=" + hsm)
                  .append(cacheClass.isBlank() ? "" : " -cacheClass=" + cacheClass)
                  .append(linkGroup.isBlank() ? "" : " -linkGroup=" + linkGroup)
                  .append(pnfsId.isBlank() ? "" : " -pnfsId=" + pnfsId)
                  .append(path.isBlank() ? "" : " -path=" + path)
                  .append(" ").append(net).append(" ").append(protocol);

            PoolPreferenceLevel[] poolPreferenceLevels =
                  poolManager.sendAndWait(command.toString(), PoolPreferenceLevel[].class);

            List<PreferenceResult> results = new ArrayList<>();

            for (PoolPreferenceLevel level : poolPreferenceLevels) {
                results.add(new PreferenceResult(level));
            }

            return results;
        } catch (JSONException | IllegalArgumentException e) {
            throw new BadRequestException(e);
        } catch (CacheException | InterruptedException | NoRouteToCellException e) {
            LOGGER.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        }
    }
}
