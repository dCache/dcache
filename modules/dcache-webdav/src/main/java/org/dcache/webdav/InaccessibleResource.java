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

import io.milton.http.Auth;
import io.milton.http.Request;
import io.milton.resource.Resource;

import java.util.Date;

import diskCacheV111.util.FsPath;

/**
 * Base class for resource objects for dCache files in the Milton
 * WebDAV frame work.
 */
public class InaccessibleResource
    implements Comparable<InaccessibleResource>, Resource
{
    private final FsPath _path;

    public InaccessibleResource(FsPath path)
    {
        _path = path;
    }

    @Override
    public Object authenticate(String user, String password)
    {
        return user;
    }

    @Override
    public boolean authorise(Request request, Request.Method method, Auth auth)
    {
        return true;
    }

    @Override
    public String checkRedirect(Request request)
    {
        return null;
    }

    @Override
    public Date getModifiedDate()
    {
        return null;
    }

    @Override
    public String getName()
    {
        return _path.name();
    }

    @Override
    public String getRealm()
    {
        return "dCache";
    }

    @Override
    public String getUniqueId()
    {
        return null;
    }

    @Override
    public int compareTo(InaccessibleResource that)
    {
        return getName().compareTo(that.getName());
    }
}
