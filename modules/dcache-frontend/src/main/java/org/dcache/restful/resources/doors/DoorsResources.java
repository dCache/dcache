package org.dcache.restful.resources.doors;

import dmg.cells.services.login.LoginBrokerSubscriber;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.dcache.restful.providers.doors.Door;
import org.dcache.restful.util.HttpServletRequests;
import org.dcache.restful.util.Responses;
import org.springframework.stereotype.Component;

@Component
@Path("doors")
@Tag(name = "doors")
@SecurityRequirement(name = "basicAuth")
@Produces(MediaType.APPLICATION_JSON)
public class DoorsResources {

    @Context
    private HttpServletRequest request;

    private final LoginBrokerSubscriber loginBrokerSubscriber;

    @Inject
    public DoorsResources(LoginBrokerSubscriber subscriber) {
        loginBrokerSubscriber = subscriber;
    }

    @GET
    @Operation(summary = "Obtain a list of available dCache doors.")
    @ApiResponses({
          @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    public Response getDoors() {
        Boolean isAdmin = HttpServletRequests.isAdmin(request);

        List<Door> result = loginBrokerSubscriber.doors().stream()
              .map(info -> new Door(isAdmin, info))
              .collect(Collectors.toList());
        return Responses.buildResponse(result);
    }
}
