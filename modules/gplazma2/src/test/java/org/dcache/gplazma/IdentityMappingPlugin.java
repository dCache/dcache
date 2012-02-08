package org.dcache.gplazma;

import java.security.Principal;
import java.util.Properties;
import java.util.Set;

import org.dcache.gplazma.plugins.GPlazmaMappingPlugin;

public class IdentityMappingPlugin implements GPlazmaMappingPlugin {

    public IdentityMappingPlugin(Properties properties) {
        //
    }

    @Override
    public void map(Set<Principal> principals, Set<Principal> authorizedPrincipals) throws AuthenticationException {
        for (Principal p : principals) {
            authorizedPrincipals.add(p);
        }
    }
}
