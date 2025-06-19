/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 - 2025 Deutsches Elektronen-Synchrotron
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

import static org.dcache.restful.util.HttpServletRequests.getLoginAttributes;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import org.dcache.auth.RolePrincipal;
import org.dcache.auth.RolePrincipal.Role;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.restful.providers.UserAttributes;
import org.dcache.restful.util.RequestUser;
import org.springframework.stereotype.Component;

/**
 * Provide services related to the identity the user is currently operating.
 */
@Component
@Api(value = "identity", authorizations = {@Authorization("basicAuth")})
@Path("/user")
public class UserResource {

    @GET
    @ApiOperation(value = "Provide information about the current user.",
          notes = "An introspection endpoint to allow the client to discover "
                + "information about the current user.")
    @Produces(MediaType.APPLICATION_JSON)
    public UserAttributes getUserAttributes(@Context HttpServletRequest request) {
        UserAttributes user = new UserAttributes();

        Subject subject = RequestUser.getSubject();
        if (Subjects.isNobody(subject)) {
            user.setStatus(UserAttributes.AuthenticationStatus.ANONYMOUS);
            user.setUid(null);
            user.setGids(null);
            user.setRoles(null);
        } else {
            user.setStatus(UserAttributes.AuthenticationStatus.AUTHENTICATED);
            user.setUid(Subjects.getUid(subject));
            user.setUsername(Subjects.getUserName(subject));
            List<Long> gids = Arrays.stream(Subjects.getGids(subject))
                  .boxed()
                  .collect(Collectors.toList());
            user.setGids(gids);
            List<String> emails = Subjects.getEmailAddresses(subject);
            user.setEmail(emails.isEmpty() ? null : emails);

            List<String> roles = new ArrayList<>();
            for (LoginAttribute attribute : getLoginAttributes(request)) {
                if (attribute instanceof HomeDirectory) {
                    user.setHomeDirectory(((HomeDirectory) attribute).getHome());
                } else if (attribute instanceof RootDirectory) {
                    user.setRootDirectory(((RootDirectory) attribute).getRoot());
                } else if (attribute instanceof org.dcache.auth.attributes.Role) {
                    roles.add(((org.dcache.auth.attributes.Role) attribute).getRole());
                }
            }

            if (!roles.isEmpty()) {
                user.setRoles(roles);
            }

            Optional<Principal> principal
                  = subject.getPrincipals().stream().filter(p->p instanceof RolePrincipal)
                  .findFirst();

            if (principal.isPresent()) {
                RolePrincipal rolePrincipal = (RolePrincipal) principal.get();
                user.setRoles(rolePrincipal.getRoles().stream().map(Role::getTag)
                      .collect(Collectors.toList()));
            }
        }

        return user;
    }
}