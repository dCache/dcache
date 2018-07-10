package org.dcache.restful.resources.doors;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import java.util.List;
import java.util.stream.Collectors;

import dmg.cells.services.login.LoginBrokerSubscriber;

import org.dcache.restful.providers.doors.Door;
import org.dcache.restful.util.HttpServletRequests;

@Component
@Path("doors")
@Api(value = "doors", description = "Operations about doors",
        authorizations = {
            @Authorization("basicAuth")
        }
)
@Produces(MediaType.APPLICATION_JSON)
public class DoorsResources
{
    @Context
    private HttpServletRequest request;

    private final LoginBrokerSubscriber loginBrokerSubscriber;

    @Inject
    public DoorsResources(LoginBrokerSubscriber subscriber)
    {
        loginBrokerSubscriber = subscriber;
    }

    @GET
    @ApiOperation(value = "Obtain a list of available dCache doors.")
    @ApiResponses({
            @ApiResponse(code = 500, message = "Internal Server Error"),
    })
    public List<Door> getDoors()
    {
        Boolean isAdmin = HttpServletRequests.isAdmin(request);

        return loginBrokerSubscriber.doors().stream()
                .map(info -> new Door(isAdmin, info))
                .collect(Collectors.toList());
    }
}
