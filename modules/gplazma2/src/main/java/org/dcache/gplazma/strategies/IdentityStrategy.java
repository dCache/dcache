package org.dcache.gplazma.strategies;

import java.security.Principal;
import java.util.Set;

import org.dcache.gplazma.NoSuchPrincipalException;
import org.dcache.gplazma.plugins.GPlazmaIdentityPlugin;

/**
 * Implementing classes will use a (combination of) GPlazmaIdentityPlugin for
 * identity mapping operations (user name to uid)
 *
 */
public interface IdentityStrategy
                 extends GPlazmaStrategy<GPlazmaIdentityPlugin> {

    Principal map(Principal principal) throws NoSuchPrincipalException;
    Set<Principal> reverseMap(Principal principal) throws NoSuchPrincipalException;
}
