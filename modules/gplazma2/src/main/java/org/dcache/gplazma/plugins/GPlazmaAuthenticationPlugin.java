package org.dcache.gplazma.plugins;

import java.security.Principal;
import java.util.Set;

import org.dcache.gplazma.AuthenticationException;

/**
 * Plugin extracting principals from a set of given credentials.
 */
public interface GPlazmaAuthenticationPlugin extends GPlazmaPlugin {
    void authenticate(Set<Object> publicCredentials,
                      Set<Object> privateCredentials,
                      Set<Principal> identifiedPrincipals)
                throws AuthenticationException;
}
