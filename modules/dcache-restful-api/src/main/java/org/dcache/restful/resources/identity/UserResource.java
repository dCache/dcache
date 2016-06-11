/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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

import jersey.repackaged.com.google.common.collect.Lists;

import javax.security.auth.Subject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.dcache.auth.Subjects;
import org.dcache.restful.providers.UserAttributes;
import org.dcache.restful.util.ServletContextHandlerAttributes;

/**
 * Provide services related to the identity the user is currently
 * operating.
 */
@Path("/user")
public class UserResource
{
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public UserAttributes getUserAttributes()
    {
        UserAttributes user = new UserAttributes();

        Subject subject = ServletContextHandlerAttributes.getSubject();
        if (Subjects.isNobody(subject)) {
            user.setStatus(UserAttributes.AuthenticationStatus.ANONYMOUS);
            user.setUid(null);
            user.setGids(null);
        } else {
            user.setStatus(UserAttributes.AuthenticationStatus.AUTHENTICATED);
            user.setUid(Subjects.getUid(subject));
            List<Long> gids = Arrays.stream(Subjects.getGids(subject))
                    .boxed()
                    .collect(Collectors.toList());
            user.setGids(gids);
        }

        return user;
    }
}
