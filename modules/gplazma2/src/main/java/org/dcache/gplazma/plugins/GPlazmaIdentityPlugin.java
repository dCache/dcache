package org.dcache.gplazma.plugins;

import java.security.Principal;
import java.util.Set;

import org.dcache.gplazma.NoSuchPrincipalException;

/**
 * Mapping which translates one type of {@link Principal} to a corresponding other type.
 */
public interface GPlazmaIdentityPlugin extends GPlazmaPlugin {

    /**
     * Forward mapping.
     * @param principal
     * @return mapped principal
     * @throws NoSuchPrincipalException if mapping does not exists.
     */
    Principal map(Principal principal) throws NoSuchPrincipalException;

    /**
     * Reverse mapping. The resulting {@link Set} MUST contain only principals on which <code>map</code>
     * call will return the provided <code>principal</code>
     * @param principal
     * @return non empty {@link Set} of equivalent principals.
     * @throws NoSuchPrincipalException if mapping does not exists.
     */
    Set<Principal> reverseMap(Principal principal) throws NoSuchPrincipalException;
}
