package org.dcache.gplazma.plugins;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.TextOutputCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import java.security.Principal;
import java.util.Properties;
import java.util.Set;

import org.dcache.auth.PasswordCredential;
import org.dcache.gplazma.AuthenticationException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.getFirst;
import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

/**
 * A {@link GPlazmaAuthenticationPlugin} implementation which verifies
 * user/pasword credentials using JAAS, the Jave Authentication and
 * Authorization Services.
 *
 * A valid JAAS setup for password authentication has to be in place.
 */
public class JaasPlugin implements GPlazmaAuthenticationPlugin
{
    private static final String NAME = "gplazma.jaas.name";

    private final String _name;

    public JaasPlugin(Properties properties)
    {
        String name = properties.getProperty(NAME);
        checkArgument(name != null, "Undefined property: " + NAME);
        _name = name;
    }

    @Override
    public void authenticate(Set<Object> publicCredentials,
                             Set<Object> privateCredentials,
                             Set<Principal> identifiedPrincipals)
        throws AuthenticationException
    {
        PasswordCredential password =
            getFirst(filter(privateCredentials, PasswordCredential.class), null);

        checkAuthentication(password != null, "no login name");

        try {
            LoginContext loginContext =
                new LoginContext(_name,
                                 new PasswordCallbackHandler(password));
            loginContext.login();
            identifiedPrincipals.addAll(loginContext.getSubject().getPrincipals());
            tryToLogout(loginContext);
        } catch (LoginException e) {
            throw new AuthenticationException(e.getMessage(), e);
        }
    }

    private static void tryToLogout(LoginContext loginContext) {
        if (loginContext == null) {
            return;
        }
        try {
            loginContext.logout();
        } catch (LoginException e) {
            // ignored
        }
    }

    private static class PasswordCallbackHandler implements CallbackHandler
    {
        private final String _userName;
        private final char _password[];

        public PasswordCallbackHandler(PasswordCredential credential)
        {
            _userName = credential.getUsername();
            _password = credential.getPassword().toCharArray();
        }

        @Override
        public void handle(Callback callbacks[])
            throws UnsupportedCallbackException
        {
            for (Callback callback: callbacks) {
                if (callback instanceof NameCallback) {
                    ((NameCallback) callback).setName(_userName);
                } else if (callback instanceof PasswordCallback) {
                    ((PasswordCallback) callback).setPassword(_password);
                } else if (callback instanceof TextOutputCallback) {
                    // do nothing
                } else {
                    throw new UnsupportedCallbackException(callback, "Unrecognized Callback");
                }
            }
        }
    }
}
