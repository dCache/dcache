package org.dcache.gplazma.plugins;

import java.security.Principal;
import java.util.Set;

import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.SessionAttribute;
import org.dcache.gplazma.SessionID;

/**
 * Plugins to obtain session metadata upon login operations. Some protocols
 * need such metadata, like home directory and root directory, that will be
 * tied to a certain SessionID.
 *
 */
public interface GPlazmaSessionPlugin extends GPlazmaPlugin {
    public void session(SessionID sID,
                        Set<Principal> authorizedPrincipals,
                        Set<SessionAttribute> attrib)
                throws AuthenticationException;
}
