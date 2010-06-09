package org.dcache.gplazma.strategies;

import java.security.Principal;
import java.util.Set;

import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.SessionID;
import org.dcache.gplazma.plugins.GPlazmaMappingPlugin;

/**
 * Implementing classes will use a (combination of) GPlazmaMappingPlugins to
 * perform mapping and reverse-mapping between principals.
 *
 */
public interface MappingStrategy
                 extends GPlazmaStrategy<GPlazmaMappingPlugin> {

    public void map(SessionID sID,
                    Set<Principal> principals,
                    Set<Principal> authorizedPrincipals)
                throws AuthenticationException;

    public void reverseMap(SessionID sID,
                           Principal sourcePrincipal,
                           Set<Principal> principals)
               throws AuthenticationException;
}
