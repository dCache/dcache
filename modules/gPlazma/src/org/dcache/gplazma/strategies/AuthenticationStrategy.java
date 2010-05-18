package org.dcache.gplazma.strategies;

import java.security.Principal;

import java.util.Set;

import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.SessionID;
import org.dcache.gplazma.plugins.GPlazmaAuthenticationPlugin;
import org.dcache.gplazma.strategies.GPlazmaStrategy;

/**
 * Implementing classes will use (combinations of)
 * GPlazmaAuthenticationPlugins to extract principals from credentials.
 */
public interface AuthenticationStrategy
                 extends GPlazmaStrategy<GPlazmaAuthenticationPlugin> {

    public void authenticate(SessionID sID,
                             Set<Object> publicCredential,
                             Set<Object> privateCredential,
                             Set<Principal> identifiedPrincipals)
                throws AuthenticationException;
}
