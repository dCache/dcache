package org.dcache.xrootd2.security;

import java.util.Properties;

/**
 * A factory loader class for AuthorizationFactory.
 *
 * Implementations of this interface are usually obtained through
 * Java's ServiceLoader mechanism.
 */
public interface AuthorizationProvider
{
    /**
     * Creates and returns a new AuthorizationFactory.
     *
     * @param plugin name identifying a partcular type of AuthorizationFactory
     * @param properties configuration values
     * @return AuthorizationFactory instance or null if the provider
     * does not provide a matching AuthorizationFactory
     */
    AuthorizationFactory createFactory(String plugin, Properties properties)
        throws Exception;
}
