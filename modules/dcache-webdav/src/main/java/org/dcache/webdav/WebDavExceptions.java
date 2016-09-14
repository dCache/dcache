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
package org.dcache.webdav;

import io.milton.resource.Resource;

import javax.security.auth.Subject;

import java.security.AccessController;

import org.dcache.auth.Subjects;

/**
 *
 */
public class WebDavExceptions
{
    private WebDavExceptions()
    {
    }

    /**
     * Returns either an UnauthorizedException or a ForbiddenException depending
     * on whether the user is authenticated.
     */
    public static WebDavException permissionDenied(Resource resource)
    {
        Subject subject = Subject.getSubject(AccessController.getContext());
        if (Subjects.isNobody(subject)) {
            return new UnauthorizedException(resource);
        } else {
            return new ForbiddenException(resource);
        }
    }

    /**
     * Returns either an UnauthorizedException or a ForbiddenException depending
     * on whether the user is authenticated.
     */
    public static WebDavException permissionDenied(String message, Throwable cause, Resource resource)
    {
        Subject subject = Subject.getSubject(AccessController.getContext());
        if (Subjects.isNobody(subject)) {
            return new UnauthorizedException(message, cause, resource);
        } else {
            return new ForbiddenException(message, cause, resource);
        }
    }
}
