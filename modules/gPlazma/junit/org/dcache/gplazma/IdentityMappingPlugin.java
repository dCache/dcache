package org.dcache.gplazma;

import java.security.Principal;
import java.util.Set;
import org.dcache.gplazma.plugins.GPlazmaMappingPlugin;

public class IdentityMappingPlugin implements GPlazmaMappingPlugin {

    public IdentityMappingPlugin(String[] args) {
    }

    @Override
    public void map(SessionID sID, Set<Principal> principals, Set<Principal> authorizedPrincipals) throws AuthenticationException {
        for (Principal p : principals) {
            authorizedPrincipals.add(p);
        }
    }

    @Override
    public void reverseMap(SessionID sID, Principal sourcePrincipal, Set<Principal> principals) throws AuthenticationException {
        principals.add(sourcePrincipal);
    }
}
