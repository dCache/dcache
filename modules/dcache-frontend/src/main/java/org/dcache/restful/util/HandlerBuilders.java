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

import diskCacheV111.util.PnfsHandler;

import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.cells.CellStub;

/**
 * Utility class for building classes that facilitate interacting with dCache.
 */
public class HandlerBuilders
{
    public static PnfsHandler pnfsHandler(CellStub pnfsManager, HttpServletRequest request)
    {
        PnfsHandler handler = new PnfsHandler(pnfsManager);
        handler.setSubject(RequestUser.getSubject());
        handler.setRestriction(HttpServletRequests.getRestriction(request));
        return handler;
    }

    public static PnfsHandler roleAwarePnfsHandler(CellStub pnfsManager, HttpServletRequest request)
    {
        PnfsHandler handler = pnfsHandler(pnfsManager, request);

        if (HttpServletRequests.isAdmin(request)) {
            handler.setSubject(Subjects.ROOT);
            handler.setRestriction(Restrictions.none());
        }

        return handler;
    }
}
