package org.dcache.gplazma.strategies;

import java.security.Principal;

import java.util.Set;

import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.SessionAttribute;
import org.dcache.gplazma.SessionID;
import org.dcache.gplazma.plugins.GPlazmaSessionPlugin;

/**
 * Implementing classes will bind session information to the session ID and
 * the principals by using a (combination of) GPlazmaSessionPlugins.
 *
 */
public interface SessionStrategy
                 extends GPlazmaStrategy<GPlazmaSessionPlugin> {

    public void session(SessionID sID,
                        Set<Principal> authorizedPrincipals,
                        Set<SessionAttribute> attrib)
                throws AuthenticationException;
}
