package org.dcache.gplazma.plugins;

import java.security.Principal;
import java.util.HashSet;
import java.util.Set;
import org.dcache.auth.Password;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.SessionID;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.auth.VerifiedUserPincipal;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.gplazma.loader.PluginLoader;
import org.dcache.gplazma.loader.StaticClassPluginLoader;
import org.dcache.gplazma.plugins.SwitchableReplyUsernamePasswordAuthenticationPluginHelper.AuthState;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author jans
 */
public class UsernamePasswordAuthenticationPluginTest {

    public static final String PASSWORD = "test";
    public static final String USERNAME = "testname";
    private PluginLoader _loader;
    private SwitchableReplyUsernamePasswordAuthenticationPluginHelper _authPlugin;

    @Before
    public void setUp() {
        _loader = StaticClassPluginLoader.newPluginLoader(
                SwitchableReplyUsernamePasswordAuthenticationPluginHelper.class);
        _loader.init();
        String[] arguments = {"SUCCESS"};
        _authPlugin = (SwitchableReplyUsernamePasswordAuthenticationPluginHelper) _loader.newPluginByName(
                SwitchableReplyUsernamePasswordAuthenticationPluginHelper.class.getSimpleName(),
                arguments);
        _authPlugin.setAuthState(AuthState.SUCCESS);
    }

    @Test
    public void testAuthenticateProperInputsSuccess() throws AuthenticationException {
        InputWrapper inputs = getProperInputs();
        Set<Principal> identifiedPrincipals = inputs.getPrincipals();
        doAuthentication(inputs, identifiedPrincipals);
        VerifiedUserPincipal user = null;
        for (Principal principal : identifiedPrincipals) {
            if (principal instanceof VerifiedUserPincipal) {
                user = (VerifiedUserPincipal) principal;
            }
        }
        assertEquals(USERNAME, user.getName());
    }

    @Test
    public void testMappingAfterSuccess() throws AuthenticationException {
        InputWrapper inputs = getProperInputs();
        doAuthentication(inputs, inputs.getPrincipals());
        Set<Principal> authorizedPrincipals = new HashSet<Principal>();
        SessionID se = null;
        _authPlugin.map(se, inputs.getPrincipals(), authorizedPrincipals);
        UserNamePrincipal user = ((UserNamePrincipal) authorizedPrincipals.toArray()[0]);
        assertEquals(USERNAME, user.getName());
    }

    @Test
    public void testSessionAfterSuccess() throws AuthenticationException {
        InputWrapper inputs = getProperInputs();
        doAuthentication(inputs, inputs.getPrincipals());
        Set<Object> attributes = new HashSet<Object>();
        _authPlugin.session(null, inputs.getPrincipals(), attributes);
        if (attributes.isEmpty()) {
            fail("couldn't find session attributes");
        }
        for (Object attribute : attributes) {
            if (!(attribute.equals(new HomeDirectory(SwitchableReplyUsernamePasswordAuthenticationPluginHelper.EXAMPLE_HOMEDIRECTORY)) ||
                  attribute.equals(new RootDirectory(SwitchableReplyUsernamePasswordAuthenticationPluginHelper.EXAMPLE_ROOTDIRECTORY)))) {
                fail("couldn't find session attributes");
            }
        }
    }

    @Test(expected = AuthenticationException.class)
    public void testAuthenticateProperInputsException() throws AuthenticationException {
        InputWrapper inputs = getProperInputs();
        _authPlugin.setAuthState(AuthState.EXCEPTION);
        Set<Principal> identifiedPrincipals = inputs.getPrincipals();
        doAuthentication(inputs, identifiedPrincipals);
    }

    @Test(expected = NullPointerException.class)
    public void testAuthenticateNullInputs() throws AuthenticationException {
        InputWrapper inputs = new InputWrapper();
        Set<Principal> identifiedPrincipals = inputs.getPrincipals();
        doAuthentication(inputs, identifiedPrincipals);
    }

    @Test(expected = AuthenticationException.class)
    public void testAuthenticateNoInputs() throws AuthenticationException {
        InputWrapper inputs = getProperInputs();
        inputs.getPrivateCredentials().clear();
        inputs.getPublicCredentials().clear();
        inputs.getPrincipals().clear();
        Set<Principal> identifiedPrincipals = inputs.getPrincipals();
        doAuthentication(inputs, identifiedPrincipals);
    }

    private void doAuthentication(InputWrapper inputs, Set<Principal> identifiedPrincipals)
            throws AuthenticationException {
        _authPlugin.authenticate(inputs.getSessionId(),
                inputs.getPublicCredentials(), inputs.getPrivateCredentials(),
                identifiedPrincipals);
    }

    private InputWrapper getProperInputs() {
        InputWrapper input = new InputWrapper();
        Set<Object> privateCredentials = new HashSet<Object>();
        Set<Object> publicCredentials = new HashSet<Object>();
        privateCredentials.add(new Password(PASSWORD));
        Set<Principal> principals = new HashSet<Principal>();
        principals.add(new UserNamePrincipal(USERNAME));
        input.setPrivateCredentials(privateCredentials);
        input.setPublicCredentials(publicCredentials);
        input.setPrincipals(principals);
        return input;

    }

    /*
     * just a helper to make the inputs for authenticate method more handy
     */
    private class InputWrapper {

        private SessionID _sessionId;
        private Set<Object> _publicCredentials;
        private Set<Object> _privateCredentials;
        private Set<Principal> _principals;

        public Set<Object> getPrivateCredentials() {
            return _privateCredentials;
        }

        public void setPrivateCredentials(Set<Object> privateCredentials) {
            _privateCredentials = privateCredentials;
        }

        public Set<Object> getPublicCredentials() {
            return _publicCredentials;
        }

        public void setPublicCredentials(Set<Object> publicCredentials) {
            _publicCredentials = publicCredentials;
        }

        public SessionID getSessionId() {
            return _sessionId;
        }

        public void setSessionId(SessionID sessionId) {
            _sessionId = sessionId;
        }

        public Set<Principal> getPrincipals() {
            return _principals;
        }

        public void setPrincipals(Set<Principal> principals) {
            _principals = principals;
        }
    }
}
