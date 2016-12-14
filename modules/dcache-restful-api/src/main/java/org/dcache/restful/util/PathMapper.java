package org.dcache.restful.util;


import javax.servlet.http.HttpServletRequest;

import diskCacheV111.util.FsPath;

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

}
