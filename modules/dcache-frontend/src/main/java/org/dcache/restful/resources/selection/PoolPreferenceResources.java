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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import org.json.JSONException;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Named;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import java.util.ArrayList;
import java.util.List;

import diskCacheV111.poolManager.PoolPreferenceLevel;
import diskCacheV111.util.CacheException;

import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.CellStub;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.restful.providers.selection.PreferenceResult;

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
    @Inject
    private PoolMonitor poolMonitor;

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
    public List<PreferenceResult> match(@ApiParam(value = "The operation type.",
                                                allowableValues = "READ,CACHE,WRITE,P2P,ANY")
                                        @DefaultValue("READ")
                                        @QueryParam("type") String type,
                                        @ApiParam("The name of the matching store unit.")
                                        @DefaultValue("*")
                                        @QueryParam("store") String store,
                                        @ApiParam("The name of the matching dcache unit.")
                                        @DefaultValue("*")
                                        @QueryParam("dcache") String dcache,
                                        @DefaultValue("*")
                                        @ApiParam("The name of the matching net unit.")
                                        @QueryParam("net") String net,
                                        @DefaultValue("*")
                                        @ApiParam("The matching protocol unit.")
                                        @QueryParam("protocol") String protocol,
                                        @ApiParam("The linkgroup unit, or 'none' for a request outside of a linkgroup.")
                                        @DefaultValue("none")
                                        @QueryParam("linkGroup") String linkGroup) {
        try {
            String command = "psux match " + type + " " + store + " "
                            + dcache + " " + net  + " " + protocol
                            + (linkGroup.equals("none") ?
                            "" : " -linkGroup=" + linkGroup);

            PoolPreferenceLevel[] poolPreferenceLevels =
                            poolManager.sendAndWait(command,
                                                    PoolPreferenceLevel[].class);

            List<PreferenceResult> results = new ArrayList<>();

            for (PoolPreferenceLevel level: poolPreferenceLevels) {
                results.add(new PreferenceResult(level));
            }

            return results;
        } catch (JSONException | IllegalArgumentException e) {
            throw new BadRequestException(e);
        } catch (CacheException | InterruptedException | NoRouteToCellException e) {
            throw new InternalServerErrorException(e);
        }
    }
}
