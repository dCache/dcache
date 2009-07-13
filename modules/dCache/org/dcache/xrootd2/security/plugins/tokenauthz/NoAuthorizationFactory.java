package org.dcache.xrootd2.security.plugins.tokenauthz;

import org.dcache.xrootd2.security.AbstractAuthorizationFactory;
import org.dcache.xrootd2.security.AuthorizationHandler;

public class NoAuthorizationFactory implements AbstractAuthorizationFactory
{
    public AuthorizationHandler getAuthzHandler()
    {
        return null;
    }

    public void init()
    {
    }

    public void setKeystore(String path)
    {
    }

    public void setNoStrongAuthorization(String auth)
    {
    }
}
