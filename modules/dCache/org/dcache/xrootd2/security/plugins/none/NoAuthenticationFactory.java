package org.dcache.xrootd2.security.plugins.none;

import org.dcache.xrootd2.security.AbstractAuthenticationFactory;
import org.dcache.xrootd2.security.AuthenticationHandler;
import org.dcache.xrootd2.security.plugins.authn.InvalidHandlerConfigurationException;

/**
 * Dummy authentication factory that creates an authentication handler which
 * accepts all AuthenticationRequests
 *
 * @author tzangerl
 *
 */
public class NoAuthenticationFactory implements AbstractAuthenticationFactory {

    @Override
    public AuthenticationHandler getAuthnHandler()
            throws InvalidHandlerConfigurationException {
        return new NoAuthenticationHandler();
    }

}
