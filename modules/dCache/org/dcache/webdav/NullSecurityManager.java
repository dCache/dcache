package org.dcache.webdav;

import com.bradmcevoy.http.Auth;
import com.bradmcevoy.http.Request;
import com.bradmcevoy.http.Request.Method;
import com.bradmcevoy.http.Resource;
import com.bradmcevoy.http.SecurityManager;

public class NullSecurityManager implements SecurityManager
{
    private String _realm;

    @Override
    public Object authenticate(String user, String password)
    {
        return user;
    }

    @Override
    public boolean authorise(Request request, Method method, Auth auth, Resource resource)
    {
        return true;
    }

    @Override
    public String getRealm()
    {
        return _realm;
    }

    public void setRealm(String realm)
    {
        _realm = realm;
    }
}
