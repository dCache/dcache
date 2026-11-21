/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2026 Deutsches Elektronen-Synchrotron
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
package org.dcache.restful.resources.identity;


import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.dcache.restful.providers.UserAttributes;
import org.dcache.restful.util.Responses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * This resource is used to test the authentication and session management of the RESTful service.
 * It is not intended to be used by clients and thus is not documented in the API.
 * It is also not intended to be used in production and thus is
 * not protected by any authentication or authorization mechanism.
 *
 * This class is temporal, because  in dCache-view there are now to way of filling in the user data
 * code-flow and non code flow.This should be fixed latter.
 */
@Component
@Tag(name = "identity")
@SecurityRequirement(name = "basicAuth")
@Path("/usersession")
//TODO this class will be modified or deleted  it is user session used indcache view has nothing to do with code flow
public class UserSession {
    private final static Logger LOG = LoggerFactory.getLogger(UserSession.class);


    @GET
    @Operation(summary = "Provide information about the current user.",
          description = "An introspection endpoint to allow the client to discover "
                + "information about the current user.")
    @Produces(MediaType.APPLICATION_JSON)

    public Response me(@Context HttpServletRequest request) {


        HttpSession session = request.getSession(false);

        LOG.debug("URI=" + request.getRequestURI());
        LOG.debug("COOKIE Me=" + request.getHeader("Cookie"));
        LOG.debug("SESSION=" + (session == null ? "null" : session.getId()));

        if (session == null) {
            LOG.warn("Unauthorized access attempt - uri={} ", request.getRequestURI());
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }

        UserAttributes user = (UserAttributes) session.getAttribute("user");
        if (user == null) {
            LOG.warn("No user attribute in session - sessionId={}", session.getId());
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }

        session.setAttribute("user", user);
        return Responses.buildResponse(user);

    }

}