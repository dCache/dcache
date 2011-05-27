package org.dcache.gplazma.plugins;

import java.security.Principal;
import java.util.Set;

import org.dcache.gplazma.AuthenticationException;

/**
 * Mapping which translates one type of {@link Principal} to a corresponding other type.
 */
public interface GPlazmaIdentityPlugin extends GPlazmaPlugin {

    /**
     * Forward mapping.
     * @param principal
     * @return
     * @throws AuthenticationException
     */
    public Principal map(Principal principal) throws AuthenticationException;

    /**
     * Reverse mapping. The resulting {@link Set} MUST contain only principals on which <code>map</code>
     * call will return the provided <code>principal</code>
     * @param principal
     * @return
     * @throws AuthenticationException
     */
    public Set<Principal> reverseMap(Principal principal) throws AuthenticationException;
}
