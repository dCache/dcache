package org.dcache.gplazma.strategies;

import java.security.Principal;

import java.util.Set;

import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.plugins.GPlazmaIdentityPlugin;

/**
 * Implementing classes will use a (combination of) GPlazmaIdentityPlugin for
 * identity mapping operations (user name to uid)
 *
 */
public interface IdentityStrategy
                 extends GPlazmaStrategy<GPlazmaIdentityPlugin> {

    public Principal map(Principal principal) throws AuthenticationException;
    public Set<Principal> reverseMap(Principal principal) throws AuthenticationException;
}
