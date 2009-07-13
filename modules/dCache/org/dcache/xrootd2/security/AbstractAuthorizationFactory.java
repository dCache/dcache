package org.dcache.xrootd2.security;

public interface AbstractAuthorizationFactory
{
    /**
     * Produces a concrete authorization handler instance.
     * @return the new authz handler instance
     */
    AuthorizationHandler getAuthzHandler();
}
