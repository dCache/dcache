package org.dcache.xrootd2.security.plugins.tokenauthz;

import org.dcache.xrootd2.security.AuthorizationFactory;
import org.dcache.xrootd2.security.AuthorizationHandler;

public class NoAuthorizationFactory implements AuthorizationFactory
{
    public final static String NAME = "none";

    private final static AuthorizationHandler HANDLER =
        new NoAuthorizationHandler();

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public String getDescription()
    {
        return "Authorizes all requests";
    }

    @Override
    public AuthorizationHandler createHandler()
    {
        return HANDLER;
    }
}
