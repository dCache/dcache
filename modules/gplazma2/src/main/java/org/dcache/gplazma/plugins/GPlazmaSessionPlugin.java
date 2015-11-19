package org.dcache.gplazma.plugins;

import java.security.Principal;
import java.util.Set;

import org.dcache.gplazma.AuthenticationException;

/**
 * Plugins to obtain session metadata upon login operations. Some protocols
 * need such metadata, like home directory and root directory, that will be
 * tied to a certain SessionID.
 *
 */
public interface GPlazmaSessionPlugin extends GPlazmaPlugin
{
    void session(Set<Principal> authorizedPrincipals,
                 Set<Object> attrib)
        throws AuthenticationException;
}
