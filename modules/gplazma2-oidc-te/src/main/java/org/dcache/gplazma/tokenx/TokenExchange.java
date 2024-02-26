package org.dcache.gplazma.tokenx;

import static java.util.Objects.requireNonNull;
import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;
import java.util.Set;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.dcache.auth.BearerTokenCredential;
import org.dcache.auth.attributes.Restriction;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.plugins.GPlazmaAuthenticationPlugin;
import org.dcache.gplazma.util.JsonWebToken;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;


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

    private final CloseableHttpClient client;

    public TokenExchange () {
        this.client = HttpClients.createDefault();
    }

    @VisibleForTesting
    public TokenExchange (CloseableHttpClient client) {
        this.client = requireNonNull(client);
    }

    @Override
    public void authenticate(Set<Object> publicCredentials, Set<Object> privateCredentials,
        Set<Principal> identifiedPrincipals, Set<Restriction> restrictions)
        throws AuthenticationException {

        BearerTokenCredential credential = null;
        for (Object c : privateCredentials) {
            if (c instanceof BearerTokenCredential) {

                credential = (BearerTokenCredential) c;
                checkAuthentication(credential == null, "Multiple bearer token credentials");

            }
        }

        String token = credential.getToken();
        LOG.debug("Found bearer token: {}", token);

        checkAuthentication(token != null, "No bearer token in the credentials");

        String exchangedToken = null;

        try {
            exchangedToken = tokenExchange(token);
            privateCredentials.remove(credential);
            privateCredentials.add(new BearerTokenCredential(exchangedToken));

        } catch ( IOException | URISyntaxException e ) {
            throw new AuthenticationException("Unable to process token: " + e.getMessage());
        }

    }


    @VisibleForTesting
    public String tokenExchange(String token) throws IOException, URISyntaxException {
        String postBody = "client_id=" + CLIENT_ID
                + "&client_secret=" + CLIENT_SECRET
                + "&grant_type=" + GRANT_TYPE
                + "&subject_token=" + token
                + "&subject_issuer=" + SUBJECT_ISSUER
                + "&subject_token_type=" + SUBJECT_TOKEN_TYPE
                + "&audience=" + AUDIENCE;
    
        URI uri = new URIBuilder(TOKEN_EXCHANGE_URL).build();
        HttpPost httpPost = new HttpPost(uri);
    
        httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded");
    
        StringEntity stringEntity = new StringEntity(postBody);
        httpPost.setEntity(stringEntity);
    
        String responseBody = null;

        try (CloseableHttpResponse response = this.client.execute(httpPost)) {
    
            HttpEntity responseEntity = response.getEntity();
            responseBody = EntityUtils.toString(responseEntity);
    
            LOG.debug("Response: " + response);
            LOG.debug("Response body: " + responseBody);
        }

        JSONObject result_json = new JSONObject(responseBody);

        if (!result_json.has("access_token")) {
            throw new IOException("response has no access_token");
        }

        String result = result_json.get("access_token").toString(); 

        LOG.debug("Exchanged Access Token: {}", result);

        if (JsonWebToken.isCompatibleFormat(result)) {
            try {
                JsonWebToken jwt = new JsonWebToken(result);
                LOG.debug("Found issuer: {}", jwt.getPayloadString("iss"));
        
            } catch (IOException e) {
                LOG.debug("Failed to parse token: {}", e.toString());
            }
        }

        return result;
    }
}