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
obligated to secure anyce necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.restful.resources.domains;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;
import org.dcache.cells.json.DomainData;
import org.dcache.restful.services.cells.DomainInfoService;
import org.dcache.restful.util.Responses;
import org.springframework.stereotype.Component;

/**
 * RESTful API to the {@link DomainInfoService} service.
 */
@Component
@Tag(name = "domains")
@SecurityRequirement(name = "basicAuth")
@Path("/domains")
public final class DomainInfoResources {

    @Inject
    private DomainInfoService service;

    @GET
    @Operation(summary = "Provide information about all domains. "
          + "Results sorted lexicographically by domain name.")
    @ApiResponses({
          @ApiResponse(responseCode = "403",
                description = "Domain info service only accessible to admin users."),
    })
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDomains() {
        DomainData[] result = Arrays.stream(service.getAddresses())
              .map(service::getDomainData)
              .sorted(Comparator.comparing(DomainData::getDomainName))
              .toArray(DomainData[]::new);
        return Responses.buildResponse(result, service.getLastUpdated());
    }

    @GET
    @Operation(summary = "Provide information about a specific domain.")
    @ApiResponses({
          @ApiResponse(responseCode = "403",
                description = "Domain info service only accessible to admin users."),
          @ApiResponse(responseCode = "404", description = "Domain not found."),
    })
    @Path("/{domain}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDomain(
          @Parameter(description = "The domain name to query", example = "dCacheDomain")
          @PathParam("domain") String domain) {
        Set<String> known = Set.of(service.getAddresses());
        if (!known.contains(domain)) {
            throw new NotFoundException("Domain not found: " + domain);
        }
        DomainData domainData = service.getDomainData(domain);
        return Responses.buildResponse(domainData, service.getLastUpdated());
    }
}
