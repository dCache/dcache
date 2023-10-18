package org.dcache.gplazma.tokenx;

import java.io.IOException;
import java.security.Principal;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;

import org.dcache.auth.BearerTokenCredential;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.auth.attributes.Restriction;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.plugins.GPlazmaAuthenticationPlugin;
import org.dcache.gplazma.plugins.GPlazmaMappingPlugin;
import org.dcache.gplazma.util.JsonWebToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

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

    /**
     * TODO:
     * set properties
     * "gplazma.oidc.token-exchange-url"
     * "gplazma.oidc.token-exchange-client-id"
     * "gplazma.oidc.token-exchange-client-secret"
     * "gplazma.oidc.token-exchange-grant-type"
     * "gplazma.oidc.token-exchange-subject-issuer"
     * "gplazma.oidc.token-exchange-subject-token-type"
     * "gplazma.oidc.token-exchange-audience"
     * */

    private final static String TOKEN_EXCHANGE_URL = "https://dev-keycloak.desy.de/auth/realms/desy-test/protocol/openid-connect/token";
    private final static String CLIENT_ID = "exchange-test";
    private final static String CLIENT_SECRET = "S0iO4EcUyn0m4b4TgSqDYViDeo9vorAs";
    private final static String GRANT_TYPE = "urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange";
    private final static String SUBJECT_ISSUER = "oidc";
    private final static String SUBJECT_TOKEN_TYPE = "urn%3Aietf%3Aparams%3Aoauth%3Atoken-type%3Aaccess_token";
    private final static String AUDIENCE = "exchange-test";

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
        for (Object credential : privateCredentials) {
            if (credential instanceof BearerTokenCredential) {
                checkAuthentication(token == null, "Multiple bearer tokens");

                token = ((BearerTokenCredential) credential).getToken();
                LOG.debug("Found bearer token: {}", token);
            }
        }

        // throw new AuthenticationException("foo: ");
        checkAuthentication(token != null, "No bearer token in the credentials");
        checkValid(token);

        String exchangedToken = "";

        try {
            exchangedToken = tokenExchange(token);
        } catch (Exception e) {
            System.out.println("Do proper exception handling");
            e.printStackTrace();
        }

    }


    @VisibleForTesting
    public String tokenExchange(String token) throws Exception {
        String result = "";

        String postBody = "client_id=" + CLIENT_ID
            + "&client_secret=" +  CLIENT_SECRET
            + "&grant_type=" + GRANT_TYPE
            + "&subject_token=" + token
            + "&subject_issuer=" + SUBJECT_ISSUER
            + "&subject_token_type=" + SUBJECT_TOKEN_TYPE
            + "&audience=" + AUDIENCE;

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(TOKEN_EXCHANGE_URL))
            .POST(BodyPublishers.ofString(postBody))
            .setHeader("Content-Type", "application/x-www-form-urlencoded")
            .build();

        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        System.out.println(response);

        return result;
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
