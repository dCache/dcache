package org.dcache.gplazma.tokenx;

import java.io.IOException;
import java.security.Principal;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import org.dcache.auth.UserNamePrincipal;
import org.dcache.auth.attributes.Restriction;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.plugins.GPlazmaAuthenticationPlugin;
import org.dcache.gplazma.plugins.GPlazmaMappingPlugin;
import org.dcache.gplazma.util.JsonWebToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class TokenExchange implements GPlazmaAuthenticationPlugin {

    private final static Logger LOG = LoggerFactory.getLogger(TokenExchange.class);

    public TokenExchange (Properties properties) {
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
        checkValid(token);

    }

    // redundant code from OidcAuthPlugin
    private static void checkValid(String token) throws AuthenticationException {
        if (JsonWebToken.isCompatibleFormat(token)) {
            try {
                JsonWebToken jwt = new JsonWebToken(token);

                Instant now = Instant.now();

                Optional<Instant> exp = jwt.getPayloadInstant("exp");
                checkAuthentication(!exp.isPresent() || now.isBefore(exp.get()),
                      "expired");

                Optional<Instant> nbf = jwt.getPayloadInstant("nbf");
                checkAuthentication(!nbf.isPresent() || now.isAfter(nbf.get()),
                      "not yet valid");
            } catch (IOException e) {
                LOG.debug("Failed to parse token: {}", e.toString());
            }
        }
    }

}
