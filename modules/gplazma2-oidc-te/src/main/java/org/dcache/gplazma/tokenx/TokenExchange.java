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
import org.dcache.gplazma.oidc.OidcAuthPlugin;
import org.dcache.gplazma.oidc.TokenProcessor;
import org.dcache.gplazma.plugins.GPlazmaAuthenticationPlugin;
import org.dcache.gplazma.plugins.GPlazmaMappingPlugin;
import org.dcache.gplazma.util.JsonWebToken;
import org.dcache.util.Args;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

// import static org.dcache.gplazma.oidc.OidcAuthPlugin.OIDC_ALLOWED_AUDIENCES;
import static org.dcache.gplazma.util.Preconditions.checkAuthentication;


public class TokenExchange extends OidcAuthPlugin {

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

    private final static String OIDC_ALLOWED_AUDIENCES = "gplazma.oidc.audience-targets";

    // private final TokenProcessor tokenProcessor;
    // private final Set<String> audienceTargets;

    public TokenExchange (Properties properties) {
        /*
         * enforced by pluggin interface
         */
        super(properties);
    }

    // @VisibleForTesting
    // TokenExchange(Properties properties, TokenProcessor processor) {
    //     tokenProcessor = processor;

    //     // String targets = properties.getProperty(OIDC_ALLOWED_AUDIENCES);
    //     // audienceTargets = Set.copyOf(new Args(targets).getArguments());
    //     super(properties, processor);
    // }

    @Override
    public void authenticate(Set<Object> publicCredentials, Set<Object> privateCredentials,
        Set<Principal> identifiedPrincipals, Set<Restriction> restrictions)
        throws AuthenticationException {

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
        // checkValid(token);

        String exchangedToken = "";

        try {
            System.out.println("calling method: tokenExchange()");
            exchangedToken = tokenExchange(token);

            privateCredentials.clear();
            privateCredentials.add(new BearerTokenCredential(exchangedToken));

            // goal:
            // - swap exchanged token with existing token
            // Plan b: 
            // - identifiedPrinciples.addAll()
        } catch (Exception e) {
            System.out.println("Do proper exception handling");
            e.printStackTrace();
        }

        super.authenticate(publicCredentials, privateCredentials, identifiedPrincipals, restrictions);
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

}