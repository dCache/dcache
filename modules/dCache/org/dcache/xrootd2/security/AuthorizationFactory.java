package org.dcache.xrootd2.security;

public interface AuthorizationFactory
{
    /**
     * Returns the name under which this plugin can be loaded.
     */
    String getName();

    /**
     * Returns a human readable description of the authorization
     * plugin.
     */
    String getDescription();

    /**
     * Creates a new authorization handler. The authorization handler
     * is only valid for a single request.
     *
     * @return the new authorization handler instance
     */
    AuthorizationHandler createHandler();
}
