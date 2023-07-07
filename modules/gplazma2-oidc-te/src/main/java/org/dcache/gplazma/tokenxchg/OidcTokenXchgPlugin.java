package org.dcache.gplazma.tokenxchg;

import java.security.Principal;
import java.util.Properties;
import java.util.Set;

import org.dcache.auth.UserNamePrincipal;
import org.dcache.auth.attributes.Restriction;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.plugins.GPlazmaAuthenticationPlugin;
import org.dcache.gplazma.plugins.GPlazmaMappingPlugin;

import static org.dcache.gplazma.util.Preconditions.checkAuthentication;


/**
 * A {@link GPlazmaMappingPlugin} converts {@code user@DOMAIN.COM} to {@link UserNamePrincipal}
 * corresponding to {@code user} ( without domain ).  For more complex mappings, like {@code
 * remte-user@DOMAIN.COM} to {@code local-user}, {@link GridMapFilePlugin} can be used.
 * <p>
 * To enable, add following likes into gplazma.conf:
 * <pre>
 *     <b>map requisite krb5</b>
 * </pre>
 */
public class OidcTokenXchgPlugin implements GPlazmaAuthenticationPlugin {

    public OidcTokenXchgPlugin(Properties properties) {
        /*
         * enforced by pluggin interface
         */
    }

    @Override
    public void authenticate(Set<Object> publicCredentials, Set<Object> privateCredentials,
          Set<Principal> identifiedPrincipals, Set<Restriction> restrictions)
          throws AuthenticationException {


        System.out.println("=================================");
        System.out.println("authenticate");
        System.out.println("publicCredentials:");
        System.out.println(publicCredentials);
        System.out.println("privateCredentials:");
        System.out.println(privateCredentials);

        String token = null;

        // throw new AuthenticationException("foo: ");
        checkAuthentication(token != null, "No bearer token in the credentials");

    }

}
