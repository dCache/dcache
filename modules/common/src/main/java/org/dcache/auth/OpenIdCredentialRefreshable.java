package org.dcache.auth;

import com.google.common.base.Charsets;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.BasicHttpContext;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class OpenIdCredentialRefreshable extends WrappingOpenIdCredential
{
    private static final Logger LOG =
            LoggerFactory.getLogger(OpenIdCredentialRefreshable.class);
    private final HttpClient client;

    public OpenIdCredentialRefreshable(OpenIdCredential credential, HttpClient client) {
        super(checkNotNull(credential, "OpenId Credential can't be null"));
        this.client = checkNotNull(client, "Http Client can't be null");
    }

    @Override
    public String getBearerToken()
    {
        if (timeToRefresh()) {
            try {
                refreshOpenIdCredentials();
            } catch (IOException | AuthenticationException e) {
                LOG.warn("Error Refreshing OpenId Bearer Token with {}: {}",
                        credential.getOpenidProvider(), e.getMessage());
            }
        }
        return credential.getBearerToken();
    }

    private synchronized void refreshOpenIdCredentials() throws IOException, AuthenticationException
    {
        HttpPost post = new HttpPost(credential.getOpenidProvider());
        BasicScheme scheme = new BasicScheme(Charsets.UTF_8);
        UsernamePasswordCredentials clientCreds = new UsernamePasswordCredentials(
                credential.getClientCredential().getId(),
                credential.getClientCredential().getSecret());

        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("client_id", credential.getClientCredential().getId()));
        params.add(new BasicNameValuePair("client_secret", credential.getClientCredential().getSecret()));
        params.add(new BasicNameValuePair("grant_type", "refresh_token"));
        params.add(new BasicNameValuePair("refresh_token", credential.getRefreshToken()));
        params.add(new BasicNameValuePair("scope", credential.getScope()));
        post.setEntity(new UrlEncodedFormEntity(params));
        post.addHeader(scheme.authenticate(clientCreds, post, new BasicHttpContext()) );

        HttpResponse response = client.execute(post);
        if (response.getStatusLine().getStatusCode() == 200) {
            updateCredential(parseResponseToJson(response));
        } else {
            throw new IOException(String.format("Error Refreshing OpenId Bearer Token [%s]: %s",
                                                    response.getStatusLine().getStatusCode(),
                                                    credential.getOpenidProvider()));
        }
    }

    private boolean timeToRefresh() {
        return (credential.getExpiresAt() - System.currentTimeMillis()) < 60*1000L;
    }

    private JSONObject parseResponseToJson(HttpResponse response) throws IOException
    {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        response.getEntity().writeTo(os);
        return new JSONObject(new String(os.toByteArray(), Charsets.UTF_8));
    }

    private void updateCredential(JSONObject json) throws IOException
    {
        try {
            this.credential = StaticOpenIdCredential.copyOf(credential)
                                                    .accessToken(json.getString("access_token"))
                                                    .expiry(json.getLong("expires_in"))
                                                    .build();
        } catch (JSONException je) {
            throw new IOException("Error Parsing response of OpenId Bearer Token Refresh: " + je.getMessage());
        }
    }
}
