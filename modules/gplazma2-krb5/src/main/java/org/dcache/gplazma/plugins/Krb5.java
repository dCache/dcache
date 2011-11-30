package org.dcache.gplazma.plugins;

import java.security.Principal;
import java.util.Properties;
import java.util.Set;
import javax.security.auth.callback.*;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import org.dcache.auth.PasswordCredential;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.SessionID;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.getFirst;
import java.util.HashSet;

/**
 * A {@link GPlazmaMappingPlugin} and {@link GPlazmaAuthenticationPlugin}
 * implementation which verifies user/pasword combination against kerberos5
 * server and converts {@code user@DOMAIN.COM} to {@link UserNamePrincipal}
 * corresponding to {@code user} ( without domain ).
 * For more complex mappings, like {@code remte-user@DOMAIN.COM} to {@code local-user},
 * {@link GridMapFilePlugin} can be used.
 *
 * To enable, add following likes into gplazma.conf:
 * <pre>
 *     <b>auth optional krb5</b>
 *     <b>map requisite krb5</b>
 * </pre>
 */
public class Krb5 implements GPlazmaMappingPlugin, GPlazmaAuthenticationPlugin {

    private static final String JAAS_CONFIG_NAME = "Krb5Gplazma";

    public Krb5(Properties properties) {
        /*
         * enforced by pluggin interface
         */
    }

    @Override
    public void authenticate(SessionID sID, Set<Object> publicCredentials, Set<Object> privateCredentials, Set<Principal> identifiedPrincipals) throws AuthenticationException {
        LoginContext loginContext = null;

        PasswordCredential password =
                getFirst(filter(privateCredentials, PasswordCredential.class), null);
        if (password == null) {
            throw new AuthenticationException("No login name provided");
        }

        try {

            loginContext = new LoginContext(JAAS_CONFIG_NAME, new PasswordCallbackHandler(password) );

            loginContext.login();
            identifiedPrincipals.addAll(loginContext.getSubject().getPrincipals());
            tryToLogout(loginContext);

        } catch (LoginException e) {
            throw new AuthenticationException("Failed to autorize:", e);
        }
    }

    private static void tryToLogout(LoginContext loginContext) {
        if(loginContext == null)
            return;

        try {
            loginContext.logout();
        } catch (LoginException e) {
            // ignored
        }
    }

    @Override
    public void map(SessionID sID, Set<Principal> principals, Set<Principal> authorizedPrincipals) throws AuthenticationException {
        Set<Principal> kerberosPrincipals = new HashSet<Principal>();
        for (Principal principal : principals) {
            if (principal instanceof KerberosPrincipal) {
                kerberosPrincipals.add(new UserNamePrincipal(stripDomain(principal.getName())));
            }
        }
        principals.addAll(kerberosPrincipals);
    }

    private String stripDomain(String s) {
        int n = s.indexOf('@');
        if (n != -1) {
            return s.substring(0, n);
        }
        return s;
    }

    private static class PasswordCallbackHandler implements CallbackHandler {

        private final String _userName;
        private final char _password[];

        public PasswordCallbackHandler(PasswordCredential credential) {
            _userName = credential.getUsername();
            _password = credential.getPassword().toCharArray();
        }

        @Override
        public void handle(Callback callbacks[]) throws UnsupportedCallbackException {
            for (Callback callback : callbacks) {
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
