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
package org.dcache.http;

import javax.servlet.http.HttpServletRequest;

import java.io.PrintWriter;
import java.util.function.Function;

import diskCacheV111.util.FsPath;

import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellInfoProvider;

import org.dcache.auth.attributes.LoginAttributes;

import static org.dcache.http.AuthenticationHandler.getLoginAttributes;
import static org.dcache.util.Exceptions.genericCheck;

/**
 * This Class is responsible for the mapping between client requested paths and
 * corresponding dCache internal paths.  This class takes into account
 * the users root directory.
 * <p>
 * The user's root and the door's root may be incompatible; for example, the
 * door may be configured to expose {@literal /data/experiment-1} and the user
 * has root {@literal /data/experiment-2}.  Since such a user cannot use this
 * door without relaxing one of the two root conditions, an exception is
 * thrown as provided by the {@literal asException} argument.
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

    public FsPath getRoot()
    {
        return _rootPath;
    }

    /**
     * Deduce the effective root for this user.  The effective root is either
     * the door's root or the user's root, whichever is longer.  This method
     * will throw an exception if the user is not allowed to use this door.
     */
    public <E extends Exception> FsPath effectiveRoot(FsPath userRoot, Function<String,E> asException) throws E
    {
        genericCheck(userRoot.hasPrefix(_rootPath) || _rootPath.hasPrefix(userRoot),
                asException, "User not allowed to use this door");
        return userRoot.length() > _rootPath.length() ? userRoot : _rootPath;
    }

    private <E extends Exception> FsPath effectiveRoot(HttpServletRequest request, Function<String,E> asException) throws E
    {
        FsPath userRoot = LoginAttributes.getUserRoot(getLoginAttributes(request));
        return effectiveRoot(userRoot, asException);
    }

    /**
     * Return the dCache path that corresponds to the request path.  This is
     * evaluated relative to the effective root.  An exception is thrown if the
     * user is not allowed to use this door.
     */
    public <E extends Exception> FsPath asDcachePath(HttpServletRequest request, String path, Function<String,E> asException) throws E
    {
        return effectiveRoot(request, asException).chroot(path);
    }

    /**
     * The dCache path that corresponds to the supplied client request path.
     * It is expected that the caller has already checked whether the user
     * is allowed to use this door.
     */
    public FsPath asDcachePath(HttpServletRequest request, String path)
    {
        return asDcachePath(request, path, RuntimeException::new);
    }

    /**
     * Resolve an arbitrary path reference against some source while enforcing
     * a root directory.  This method ensures that ".." path elements cannot
     * walk outside the effective root directory.  It is expected that the
     * caller has already checked whether the user is allowed to use this door.
     */
    public FsPath resolve(HttpServletRequest request, FsPath source, String path)
    {
        FsPath root = effectiveRoot(request, RuntimeException::new);
        String requestAbsolutePath = path.startsWith("/") ? path : (source.stripPrefix(root) + "/" + path);
        return root.chroot(requestAbsolutePath);
    }

    /**
     * Calculate the path that a client would request that would correspond to
     * the supplied dCache path.  This method is the inverse of the
     * {@link #asDcachePath} method.  It is expected that the caller has already
     * check that the user is allowed to use this door.
     */
    public String asRequestPath(HttpServletRequest request, FsPath path)
    {
        return path.stripPrefix(effectiveRoot(request, RuntimeException::new));
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
