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
package org.dcache.restful.resources.restores;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import java.util.UUID;

import diskCacheV111.util.CacheException;

import org.dcache.restful.providers.SnapshotList;
import org.dcache.restful.providers.restores.RestoreInfo;
import org.dcache.restful.services.restores.RestoresInfoService;
import org.dcache.restful.util.RequestUser;

/**
 * <p>RESTful API to the {@link RestoresInfoService} service.</p>
 *
 * @version v1.0
 */
@Component
@Api(value = "pools", authorizations = {@Authorization("basicAuth")})
@Path("/restores")
public final class RestoreResources {
    @Inject
    private RestoresInfoService service;

    private boolean unlimitedOperationVisibility;

    @GET
    @ApiOperation("Obtain a (potentially partial) list of restore operations"
            + " from some snapshot, along with a token that identifies the snapshot.  Note:"
                    + " the output to this request represents all the staging operations"
                    + " triggered through the pool manager (via read requests through"
                    + " the doors); cf the admin command '\\sp rc ls'.  Stage operations"
                    + " initiated directly on a pool via 'rh restore <pnfsid>' do not"
                    + " appear here.  To see a listing of all stages/restores on a given"
                    + " pool, use the API for /pools/{pool}/nearline/queues?type=stage).")
    @ApiResponses({
                @ApiResponse(code = 403, message = "Restores can only be accessed by admin users."),
                @ApiResponse(code = 500, message = "Internal Server Error"),
            })
    @Produces(MediaType.APPLICATION_JSON)
    public SnapshotList<RestoreInfo> getRestores(@ApiParam("Use the snapshot "
                                                    + "corresponding to this UUID.  The "
                                                    + "contract with the service is that if the "
                                                    + "parameter value is null, the current snapshot "
                                                    + "will be used, regardless of whether offset and "
                                                    + "limit are still valid.  Initial/refresh "
                                                    + "calls should always be without a token.  "
                                                    + "Subsequent calls should send back the "
                                                    + "current token; in the case that it no "
                                                    + "longer corresponds to the current list, "
                                                    + "the service will return a null token "
                                                    + "and an empty list, and the client will "
                                                    + "need to recall the method without a "
                                                    + "token (refresh).")
                                                 @QueryParam("token") UUID token,
                                                 @ApiParam("The number of restores to skip.")
                                                 @QueryParam("offset") Integer offset,
                                                 @ApiParam("The maximum number of restores to return.")
                                                 @QueryParam("limit") Integer limit,
                                                 @ApiParam("Select only restores that affect this PNFS-ID.")
                                                 @QueryParam("pnfsid") String pnfsid,
                                                 @ApiParam("Select only restores triggered by clients from this subnet.")
                                                 @QueryParam("subnet") String subnet,
                                                 @ApiParam("Select only restores on this pool.")
                                                 @QueryParam("pool") String pool,
                                                 @ApiParam("Select only restores with this status.")
                                                 @QueryParam("status") String status,
                                                 @ApiParam("A comma-seperated list of fields on which to sort the results.")
                                                 @DefaultValue("pool,started")
                                                 @QueryParam("sort") String sort) {
        try {
            if (!RequestUser.canViewFileOperations(unlimitedOperationVisibility)) {
                throw new ForbiddenException("Restores can only be accessed by admin users.");
            }

            return service.get(token,
                               offset,
                               limit,
                               pnfsid,
                               subnet,
                               pool,
                               status,
                               sort);
        } catch (CacheException e) {
            throw new InternalServerErrorException(e);
        }
    }

    @Required
    public void setUnlimitedOperationVisibility(boolean visibility) {
        unlimitedOperationVisibility = visibility;
    }
}