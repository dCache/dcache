package org.dcache.gplazma;
import java.security.Principal;
import java.util.Properties;
import java.util.Set;

import org.dcache.gplazma.plugins.GPlazmaAccountPlugin;
import org.dcache.gplazma.plugins.GPlazmaAuthenticationPlugin;
import org.dcache.gplazma.plugins.GPlazmaMappingPlugin;
import org.dcache.gplazma.plugins.GPlazmaSessionPlugin;

/**
 * Fails (throws AuthenticationException) from every
 * method
 * @author timur
 */
public class AlwaysFailPlugin implements
        GPlazmaAccountPlugin,
        GPlazmaMappingPlugin,
        GPlazmaAuthenticationPlugin,
        GPlazmaSessionPlugin {

    private static final String FAIL_MSG = "Not this time, mate";

    /**
     * Plugin ignores arguments.
     * @param args
     */
    public AlwaysFailPlugin(Properties properties) {
        //
    }

    @Override
    public void account(Set<Principal> authorizedPrincipals) throws AuthenticationException {
        throw new AuthenticationException(FAIL_MSG);
    }

    @Override
    public void map(Set<Principal> principals) throws AuthenticationException {
        throw new AuthenticationException(FAIL_MSG);
    }

    @Override
    public void authenticate(Set<Object> publicCredentials, Set<Object> privateCredentials, Set<Principal> identifiedPrincipals) throws AuthenticationException {
        throw new AuthenticationException(FAIL_MSG);
    }

    @Override
    public void session(Set<Principal> authorizedPrincipals, Set<Object> attrib) throws AuthenticationException {
        throw new AuthenticationException(FAIL_MSG);
    }


}
