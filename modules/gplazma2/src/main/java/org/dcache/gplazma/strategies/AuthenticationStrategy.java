package org.dcache.gplazma.strategies;

import java.security.Principal;
import java.util.Set;

import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.monitor.LoginMonitor;
import org.dcache.gplazma.plugins.GPlazmaAuthenticationPlugin;

/**
 * Implementing classes will use (combinations of)
 * GPlazmaAuthenticationPlugins to extract principals from credentials.
 */
public interface AuthenticationStrategy
                 extends GPlazmaStrategy<GPlazmaAuthenticationPlugin> {

    void authenticate(LoginMonitor monitor, Set<Object> publicCredential,
                      Set<Object> privateCredential,
                      Set<Principal> identifiedPrincipals)
                throws AuthenticationException;
}
