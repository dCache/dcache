package org.dcache.gplazma.plugins;

import java.security.Principal;
import java.util.Set;

import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.SessionID;

/**
 * Plugin extracting principals from a set of given credentials.
 */
public interface GPlazmaAuthenticationPlugin extends GPlazmaPlugin {
    public void authenticate(SessionID sID,
                             Set<Object> publicCredential,
                             Set<Object> privateCredential,
                             Set<Principal> identifiedPrincipals)
                throws AuthenticationException;
}
