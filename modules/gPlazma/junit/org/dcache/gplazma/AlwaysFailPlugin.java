package org.dcache.gplazma;
import java.security.Principal;
import java.util.Set;
import org.dcache.gplazma.plugins.GPlazmaAccountPlugin;
import org.dcache.gplazma.plugins.GPlazmaMappingPlugin;
import org.dcache.gplazma.plugins.GPlazmaAuthenticationPlugin;
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
    public AlwaysFailPlugin(String[] args) {

    }

    @Override
    public void account(SessionID sID, Set<Principal> authorizedPrincipals) throws AuthenticationException {
        throw new AuthenticationException(FAIL_MSG);
    }

    @Override
    public void map(SessionID sID, Set<Principal> principals, Set<Principal> authorizedPrincipals) throws AuthenticationException {
        throw new AuthenticationException(FAIL_MSG);
    }

    @Override
    public void reverseMap(SessionID sID, Principal sourcePrincipal, Set<Principal> principals) throws AuthenticationException {
        throw new AuthenticationException(FAIL_MSG);
    }

    @Override
    public void authenticate(SessionID sID, Set<Object> publicCredentials, Set<Object> privateCredentials, Set<Principal> identifiedPrincipals) throws AuthenticationException {
        throw new AuthenticationException(FAIL_MSG);
    }

    @Override
    public void session(SessionID sID, Set<Principal> authorizedPrincipals, Set<SessionAttribute> attrib) throws AuthenticationException {
        throw new AuthenticationException(FAIL_MSG);
    }


}
