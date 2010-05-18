package org.dcache.gplazma.plugins;

import java.security.Principal;
import java.util.Set;

import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.SessionID;

/**
 * Plugin checking the principals authorized by the authentication and mapping
 * plugins against global settings and policies. Can be used for
 *  - global blacklisting of users
 *  - rejecting users with illegal or non-authorized principal sets
 */
public interface GPlazmaAccountPlugin extends GPlazmaPlugin {
    public void account(SessionID sID,
                        Set<Principal> authorizedPrincipals)
                throws AuthenticationException;
}
