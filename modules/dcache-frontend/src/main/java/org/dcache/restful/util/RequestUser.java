/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2001 - 2017 Deutsches Elektronen-Synchrotron
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
package org.dcache.restful.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;

import java.io.IOException;
import java.security.AccessController;

import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Restriction;

import static com.google.common.base.Preconditions.checkState;
import static org.dcache.util.Exceptions.genericCheck;

/**
 * Utility class to handle the identity of the user issuing the request.
 * It also acts as a JAX-RS Filter to populate thread-local to avoid having
 * to work with HttpServletRequest objects.
 */
public class RequestUser implements ContainerRequestFilter, ContainerResponseFilter
{
    private static final ThreadLocal<Restriction> RESTRICTIONS = new ThreadLocal<>();
    private static final ThreadLocal<Boolean> IS_ADMIN = new ThreadLocal<>();

    @Inject
    private HttpServletRequest request;

    public RequestUser()
    {
        // Allow instantiation as a JAX-RS Filter.
    }

    public static Subject getSubject()
    {
        return Subject.getSubject(AccessController.getContext());
    }

    public static boolean isAnonymous()
    {
        return Subjects.isNobody(getSubject());
    }

    public static Restriction getRestriction()
    {
        Restriction restriction = RESTRICTIONS.get();
        checkState(restriction != null, "RequestUser#getRestriction called outside of REST request");
        return restriction;
    }

    public static boolean isAdmin()
    {
        Boolean isAdmin = IS_ADMIN.get();
        checkState(isAdmin != null, "RequestUser#isAdmin called outside of REST request");
        return isAdmin;
    }

    public static void checkAuthenticated() throws NotAuthorizedException
    {
        genericCheck(!isAnonymous(), NotAuthorizedException::new,
                "anonymous access not allowed");
    }

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException
    {
        RESTRICTIONS.set(HttpServletRequests.getRestriction(request));
        IS_ADMIN.set(HttpServletRequests.isAdmin(request));
    }

    @Override
    public void filter(ContainerRequestContext requestContext,
            ContainerResponseContext responseContext) throws IOException
    {
        RESTRICTIONS.set(null);
        IS_ADMIN.set(null);
    }
}
