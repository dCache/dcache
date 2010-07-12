package org.dcache.gplazma.plugins;

import java.security.Principal;
import java.util.Set;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.HomeDirectory;
import org.dcache.gplazma.RootDirectory;
import org.dcache.gplazma.SessionAttribute;
import org.dcache.gplazma.SessionID;

/**
 * This is a helper-class for Unittesting. It implements a
 * UsernamePasswordAuthenticationPlugin with an external-setable response to an
 * authenticate call.
 * behaves as AuthState is set:
 * SUCCESS simulates success
 * EXCEPTION throws an AuthenticationException
 * @author jans
 */
public class SwitchableReplyUsernamePasswordAuthenticationPluginHelper
        extends UsernamePasswordAuthenticationPlugin {

    public static final String EXAMPLE_HOMEDIRECTORY = "home";
    public static final String EXAMPLE_ROOTDIRECTORY = "root";
    private AuthState _authState;

    public SwitchableReplyUsernamePasswordAuthenticationPluginHelper() {
        super();
    }

    public SwitchableReplyUsernamePasswordAuthenticationPluginHelper(String[] arguments) {
        super(arguments);
        if (arguments[0].equals("EXCEPTION")) {
            _authState = AuthState.EXCEPTION;
        } else {
            _authState = AuthState.SUCCESS;
        }
    }

    @Override
    public void authenticate(String username, char[] password)
            throws AuthenticationException {
        switch (_authState) {
            case SUCCESS:
                return;
            case EXCEPTION:
                throw new AuthenticationException("couldn't authenticate");
            default:
                throw new RuntimeException();
        }
    }

    public void setAuthState(AuthState authState) {
        _authState = authState;
    }

    @Override
    protected void session(String username, Set<SessionAttribute> attrib) throws AuthenticationException {
        attrib.add(new HomeDirectory(EXAMPLE_HOMEDIRECTORY));
        attrib.add(new RootDirectory(EXAMPLE_ROOTDIRECTORY));
    }

    @Override
    public void reverseMap(SessionID sID, Principal sourcePrincipal, Set<Principal> principals) throws AuthenticationException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public enum AuthState {

        SUCCESS, EXCEPTION
    }
}

