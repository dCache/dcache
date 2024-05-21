package org.dcache.gplazma.tokenx;

import static java.util.Objects.requireNonNull;
import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;
import java.util.Properties;
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

    public final static String TOKEN_EXCHANGE_URL = "gplazma.oidc-te.url";
    public final static String CLIENT_ID = "gplazma.oidc-te.client-id";
    public final static String CLIENT_SECRET = "gplazma.oidc-te.client-secret";
    public final static String GRANT_TYPE = "gplazma.oidc-te.grant-type";
    public final static String SUBJECT_ISSUER = "gplazma.oidc-te.subject-issuer";
    public final static String SUBJECT_TOKEN_TYPE = "gplazma.oidc-te.subject-token-type";
    public final static String AUDIENCE = "gplazma.oidc-te.audience";

    private final CloseableHttpClient client;

    private final String tokenExchangeURL;
    private final String clientID; 
    private final String clientSecret; 
    private final String grantType; 
    private final String subjectIssuer; 
    private final String subjectTokenType; 
    private final String audience; 

    public TokenExchange (Properties properties) {
        this(properties, HttpClients.createDefault());

    }

    @VisibleForTesting
    TokenExchange (Properties properties, CloseableHttpClient client) {

        tokenExchangeURL = properties.getProperty(TOKEN_EXCHANGE_URL);
        clientID = properties.getProperty(CLIENT_ID);
        clientSecret = properties.getProperty(CLIENT_SECRET);
        grantType = properties.getProperty(GRANT_TYPE);
        subjectIssuer = properties.getProperty(SUBJECT_ISSUER);
        subjectTokenType = properties.getProperty(SUBJECT_TOKEN_TYPE);
        audience = properties.getProperty(AUDIENCE);

        this.client = requireNonNull(client);

    }

    @Override
    public void authenticate(Set<Object> publicCredentials, Set<Object> privateCredentials,
        Set<Principal> identifiedPrincipals, Set<Restriction> restrictions)
        throws AuthenticationException {

        BearerTokenCredential credential = null;
        for (Object c : privateCredentials) {
            if (c instanceof BearerTokenCredential) {

                checkAuthentication(credential == null, "Multiple bearer token credentials");
                credential = (BearerTokenCredential) c;

            }
        }

        if (credential == null) {
            throw new AuthenticationException("No bearer token credential found");
        }

        String token = credential.getToken();
        LOG.debug("Found bearer token: {}", token);

        checkAuthentication(token != null, "No bearer token in the credentials");


        try {
            String exchangedToken = tokenExchange(token);
            privateCredentials.remove(credential);
            privateCredentials.add(new BearerTokenCredential(exchangedToken));

        } catch ( IOException | URISyntaxException e ) {
            throw new AuthenticationException("Unable to process token: " + e.getMessage());
        }

    }


    @VisibleForTesting
    String tokenExchange(String token) throws IOException, URISyntaxException {
        String postBody = "client_id=" + clientID 
                + "&client_secret=" + clientSecret
                + "&grant_type=" + grantType
                + "&subject_token=" + token
                + "&subject_issuer=" + subjectIssuer
                + "&subject_token_type=" + subjectTokenType
                + "&audience=" + audience;

    
        URI uri = new URIBuilder(tokenExchangeURL).build();
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