package org.dcache.gplazma.strategies;

import java.security.Principal;
import java.util.Set;

import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.monitor.LoginMonitor;
import org.dcache.gplazma.plugins.GPlazmaAccountPlugin;

/**
 * Implementing classes will use a (combination of) GPlazmaAccountPlugins for
 * account operations (that could e.g. be global blacklisting)
 *
 */
public interface AccountStrategy
                 extends GPlazmaStrategy<GPlazmaAccountPlugin> {

    void account(LoginMonitor monitor,
                 Set<Principal> authorizedPrincipals) throws AuthenticationException;
}
