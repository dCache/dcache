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
package org.dcache.webdav.owncloud;

import javax.servlet.http.HttpServletRequest;

import java.util.function.Function;

import diskCacheV111.util.FsPath;

import org.dcache.http.PathMapper;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Extends the original PathMapper to support mapping of resources at certain OwnCloud-specific paths.
 * @author ugrin
 */
public class OwnCloudPathMapper extends PathMapper
{
    private String _prefix;

    public void setOwnCloudPrefix(String owncloudWebDAVPath)
    {
        _prefix = owncloudWebDAVPath;
    }

    private String removePrefix(HttpServletRequest request, String path)
    {
        if (OwncloudClients.isSyncClient(request)) {
            checkArgument(!path.isEmpty(), "empty path");

            if (path.startsWith(_prefix)) {
                if (path.length() == _prefix.length()) {
                    path = "/";
                } else if (path.charAt(_prefix.length()) == '/') {
                    path = path.substring(_prefix.length());
                }
            }
        }

        return path;
    }

    @Override
    public FsPath asDcachePath(HttpServletRequest request, String path)
    {
        return super.asDcachePath(request, removePrefix(request, path));
    }

    @Override
    public <E extends Exception> FsPath asDcachePath(HttpServletRequest request, String path, Function<String,E> asException) throws E
    {
        return super.asDcachePath(request, removePrefix(request, path), asException);
    }
}
