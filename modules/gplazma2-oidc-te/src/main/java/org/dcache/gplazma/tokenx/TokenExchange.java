package org.dcache.gplazma.tokenx;

import java.io.IOException;
import java.security.Principal;
import java.time.Instant;
import java.util.Enumeration;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;

import org.json.JSONObject;

import org.dcache.auth.BearerTokenCredential;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.auth.attributes.Restriction;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.oidc.OidcAuthPlugin;
import org.dcache.gplazma.oidc.TokenProcessor;
import org.dcache.gplazma.oidc.UnableToProcess;
import org.dcache.gplazma.plugins.GPlazmaAuthenticationPlugin;
import org.dcache.gplazma.plugins.GPlazmaMappingPlugin;
import org.dcache.gplazma.util.JsonWebToken;
import org.dcache.util.Args;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

// import static org.dcache.gplazma.oidc.OidcAuthPlugin.OIDC_ALLOWED_AUDIENCES;
import static org.dcache.gplazma.util.Preconditions.checkAuthentication;


// public class TokenExchange extends OidcAuthPlugin {
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

    private final static String TOKEN_EXCHANGE_URL = "https://keycloak.desy.de/auth/realms/production/protocol/openid-connect/token";
    private final static String CLIENT_ID = "token-exchange";
    private final static String CLIENT_SECRET = "tj05R7fKtV0Pqkxxnby5aic1AsiiROHy";
    private final static String GRANT_TYPE = "urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange";
    private final static String SUBJECT_ISSUER = "oidc";
    private final static String SUBJECT_TOKEN_TYPE = "urn%3Aietf%3Aparams%3Aoauth%3Atoken-type%3Aaccess_token";
    private final static String AUDIENCE = "token-exchange";

    private final static String OIDC_ALLOWED_AUDIENCES = "gplazma.oidc.audience-targets";

    // private final TokenProcessor tokenProcessor;
    // private final Set<String> audienceTargets;

    public TokenExchange (Properties properties) {
        /*
         * enforced by pluggin interface
         */
        // super(properties);
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
        // throws Exception {

        BearerTokenCredential credential = null;
        for (Object c : privateCredentials) {
            if (c instanceof BearerTokenCredential) {
                // checkAuthentication(token == null, "Multiple bearer tokens");

                credential = (BearerTokenCredential) c;
                checkAuthentication(credential == null, "Multiple bearer token credentials");
            }
        }

        String token = credential.getToken();
        LOG.debug("Found bearer token: {}", token);

        // throw new AuthenticationException("foo: ");
        checkAuthentication(token != null, "No bearer token in the credentials");
        // checkValid(token);

        String exchangedToken = null;

        try {
            // trace
            System.out.println("calling method: tokenExchange()");
            
            exchangedToken = tokenExchange(token);
            checkAuthentication(exchangedToken == null, "Token not exchangeable");

            // goal:
            // - swap exchanged token with existing token
            // instead of clear remove original token
            //privateCredentials.clear();
            // privateCredentials.remove(new BearerTokenCredential(token));
            privateCredentials.remove(credential);

            // System.out.println("size of credential list:" + privateCredentials.size());
            privateCredentials.add(new BearerTokenCredential(exchangedToken));

        } catch (UnableToProcess | IOException | InterruptedException e) {
            // System.out.println("Do proper exception handling");
            // e.printStackTrace();
            throw new AuthenticationException("Unable to process token: " + e.getMessage());
        }

        // super.authenticate(publicCredentials, privateCredentials, identifiedPrincipals, restrictions);
    }

    @VisibleForTesting
    public String tokenExchange(String token) 
        throws UnableToProcess, IOException, InterruptedException {
        // throws Exception {
        
        String result = null;

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

        System.out.println("request:");
        System.out.println("toString: " + request.toString());
        System.out.println("headers: " + request.headers());
        System.out.println("bodyPublisher: " + request.bodyPublisher().toString());

        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<String> response = null;
        String response_body = null;

        try {
            response = client.send(request, HttpResponse.BodyHandlers.ofString());
            System.out.println("response:");
            System.out.println("respone: " + response.toString());
            // System.out.println("response body: " + response.body());

            response_body = response.body();

            System.out.println("response.body(): " + response_body);

        } catch (IOException | InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            LOG.debug("Failed to send/receive request to Idp: {}", e.toString());
        }


        // extract access_token from result:

        JSONObject result_json = new JSONObject(response_body);

        if (!result_json.has("access_token")) {
            throw new IOException("response has no access_token");
        }

        // if (result_json.has("access_token")) {

        result = result_json.get("access_token").toString(); 

        System.out.println("Access Token:");
        System.out.println(result);

        if (JsonWebToken.isCompatibleFormat(result)) {
            try {
                JsonWebToken jwt = new JsonWebToken(result);

                System.out.println("issuer: " + jwt.getPayloadString("iss"));
        
            } catch (IOException e) {
                LOG.debug("Failed to parse token: {}", e.toString());
            }
        }
        // } 

        // TODO Optional.ofNullable
        return result;
    }

}