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

import javax.security.auth.Subject;
import javax.ws.rs.NotAuthorizedException;

import java.security.AccessController;

import org.dcache.auth.Subjects;

import static org.dcache.util.Exceptions.genericCheck;

/**
 * Utility class to handle the identity of the user issuing the request.
 */
public class RequestUser
{
    private RequestUser()
    {
        // Prevent instantiation.
    }

    public static Subject getSubject()
    {
        return Subject.getSubject(AccessController.getContext());
    }

    public static boolean isAnonymous()
    {
        return Subjects.isNobody(getSubject());
    }

    public static void checkAuthenticated() throws NotAuthorizedException
    {
        genericCheck(!isAnonymous(), NotAuthorizedException::new,
                "anonymous access not allowed");
    }
}
