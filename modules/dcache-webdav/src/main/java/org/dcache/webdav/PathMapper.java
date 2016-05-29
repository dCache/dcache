/* dCache - http://www.dcache.org/
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

import javax.servlet.http.HttpServletRequest;

import java.io.PrintWriter;

import diskCacheV111.util.FsPath;

import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellInfoProvider;

/**
 * This Class is responsible for the mapping between client requested paths and
 * corresponding dCache internal paths.
 */
public class PathMapper implements CellInfoProvider
{
    private FsPath _rootPath = FsPath.ROOT;

    public void setRootPath(String path)
    {
        _rootPath = FsPath.create(path);
    }

    public String getRootPath()
    {
        return _rootPath.toString();
    }

    /**
     * The dCache path that corresponds to the supplied client request path.
     */
    public FsPath asDcachePath(HttpServletRequest request, String path)
    {
        return _rootPath.chroot(path);
    }

    /**
     * Calculate the path that a client would request that would correspond to
     * the supplied dCache path.  This method is the inverse of the
     * {@link #asDcachePath} method.
     */
    public String asRequestPath(FsPath path)
    {
        return path.stripPrefix(_rootPath);
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println("Root path : " + getRootPath());
    }

    @Override
    public CellInfo getCellInfo(CellInfo info)
    {
        return info;
    }
}
