package org.dcache.xrootd2.security;

import org.dcache.xrootd2.security.plugins.authn.InvalidHandlerConfigurationException;

public interface AbstractAuthenticationFactory {
    public AuthenticationHandler getAuthnHandler()
        throws InvalidHandlerConfigurationException;
}
