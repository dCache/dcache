/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017 Deutsches Elektronen-Synchrotron
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

import javax.servlet.http.HttpServletRequest;

import java.util.Set;

import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.Restriction;
import org.dcache.http.AuthenticationHandler;

import static org.dcache.http.AuthenticationHandler.DCACHE_LOGIN_ATTRIBUTES;

/**
 * Utility class for methods that operate on an HttpServletRequest object.
 */
public class HttpServletRequests
{
    private HttpServletRequests()
    {
        // Prevent instantiation.
    }

    public static Set<LoginAttribute> getLoginAttributes(HttpServletRequest request)
    {
        return (Set<LoginAttribute>) request.getAttribute(DCACHE_LOGIN_ATTRIBUTES);
    }

    public static Restriction getRestriction(HttpServletRequest request)
    {
        return (Restriction) request.getAttribute(AuthenticationHandler.DCACHE_RESTRICTION_ATTRIBUTE);
    }
}
