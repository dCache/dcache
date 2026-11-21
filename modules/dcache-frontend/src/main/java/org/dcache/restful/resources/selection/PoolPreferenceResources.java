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
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.InternalServerErrorException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import org.dcache.cells.CellStub;
import org.dcache.restful.providers.selection.PreferenceResult;
import org.dcache.restful.util.Responses;
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
@Tag(name = "poolmanager")
@SecurityRequirement(name = "basicAuth")
@Path("/pool-preferences")
public final class PoolPreferenceResources {

    private static final Logger LOGGER = LoggerFactory.getLogger(PoolPreferenceResources.class);

    @Inject
    @Named("pool-manager-stub")
    private CellStub poolManager;

    @GET
    @Operation(summary = "Describe the pools selected by a particular request.")
    @ApiResponses({
          @ApiResponse(responseCode = "400", description = "Bad Request"),
          @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Produces(MediaType.APPLICATION_JSON)
    public Response match(
          @Parameter(description = "The I/O direction.",
                schema = @Schema(allowableValues = {"READ", "CACHE", "WRITE", "P2P"}))
          @DefaultValue("READ")
          @QueryParam("ioDirection") String ioDirection,
          @Parameter(description = "The name of the storage class.")
          @QueryParam("storageClass") String storageClass,
          @Parameter(description = "The name of the hsm type.")
          @QueryParam("hsm") String hsm,
          @Parameter(description = "The name of the cache class.")
          @DefaultValue("")
          @QueryParam("cacheClass") String cacheClass,
          @Parameter(description = "The link group unit.")
          @DefaultValue("")
          @QueryParam("linkGroup") String linkGroup,
          @Parameter(description = "The pnfsId of a file.")
          @DefaultValue("")
          @QueryParam("pnfsId") String pnfsId,
          @Parameter(description = "The path of a file.")
          @DefaultValue("")
          @QueryParam("path") String path,
          @Parameter(description = "The name of the net unit.")
          @DefaultValue("*")
          @QueryParam("net") String net,
          @Parameter(description = "The name of the protocol unit.")
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

            return Responses.buildResponse(results);
        } catch (JSONException | IllegalArgumentException e) {
            throw new BadRequestException(e);
        } catch (CacheException | InterruptedException | NoRouteToCellException e) {
            LOGGER.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        }
    }
}
