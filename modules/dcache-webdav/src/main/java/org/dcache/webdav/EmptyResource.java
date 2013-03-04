package org.dcache.webdav;

import io.milton.http.Auth;
import io.milton.http.Request;
import io.milton.resource.Resource;

import java.io.File;
import java.util.Date;

/**
 * A resource without any content.
 *
 * Intended as a placeholder when calling the ResponseHandler without
 * a reference to the real resource.
 */
public class EmptyResource implements Resource
{
    private String _name;

    public EmptyResource(String name)
    {
        _name = name;
    }

    public EmptyResource(Request request)
    {
        this(new File(request.getAbsolutePath()).getName());
    }

    @Override
    public String getUniqueId()
    {
        return null;
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public Object authenticate(String user, String password)
    {
        return null;
    }

    @Override
    public boolean authorise(Request request, Request.Method method, Auth auth)
    {
        return false;
    }

    @Override
    public String getRealm()
    {
        return null;
    }

    @Override
    public Date getModifiedDate()
    {
        return new Date();
    }

    @Override
    public String checkRedirect(Request request)
    {
        return null;
    }
}
