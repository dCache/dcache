/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.webdav.transfer;

import com.google.common.base.Charsets;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import eu.emi.security.authn.x509.X509Credential;
import eu.emi.security.authn.x509.impl.KeyAndCertCredential;
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
import org.springframework.beans.factory.annotation.Required;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.security.KeyStoreException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import diskCacheV111.srm.CredentialServiceAnnouncement;
import diskCacheV111.srm.CredentialServiceRequest;
import diskCacheV111.srm.dcache.SrmRequestCredentialMessage;
import diskCacheV111.util.CacheException;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.auth.OpenIdClientSecret;
import org.dcache.auth.StaticOpenIdCredential;
import org.dcache.auth.StaticOpenIdCredential.Builder;
import org.dcache.cells.CellStub;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This class acts as a client to credential services.
 */
public class CredentialServiceClient
    implements CellMessageReceiver, CellLifeCycleAware
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CredentialServiceClient.class);

    private static final String GRANT_TYPE = "urn:ietf:params:oauth:grant-type:token-exchange";
    private static final String TOKEN_TYPE = "urn:ietf:params:oauth:token-type:access_token";
    private static final String SCOPE = "offline_access openid profile email";

    private CellStub topic;

    private Cache<CellAddressCore, URI> cache = CacheBuilder.newBuilder().expireAfterWrite(70, SECONDS).build();

    @Required
    public void setTopicStub(CellStub topic)
    {
        this.topic = topic;
    }

    @Override
    public void afterStart()
    {
        topic.notify(new CredentialServiceRequest());
    }

    public void messageArrived(CredentialServiceAnnouncement message)
    {
        cache.put(message.getCellAddress(), message.getDelegationEndpoint());
    }

    public Collection<URI> getDelegationEndpoints()
    {
        return cache.asMap().values();
    }

    public X509Credential getDelegatedCredential(String dn, String primaryFqan,
            int minimumValidity, TimeUnit units) throws InterruptedException, ErrorResponseException
    {
        long bestRemainingLifetime = 0;
        X509Credential bestCredential = null;

        for (CellAddressCore address : cache.asMap().keySet()) {
            CellPath path = new CellPath(address);
            SrmRequestCredentialMessage msg = new SrmRequestCredentialMessage(dn, primaryFqan);
            try {
                msg = topic.sendAndWait(path, msg);

                if (!msg.hasCredential()) {
                    continue;
                }

                X509Certificate[] certificates = msg.getCertificateChain();
                long lifetime = calculateRemainingLifetime(certificates);
                if (lifetime > bestRemainingLifetime) {
                    bestCredential = new KeyAndCertCredential(msg.getPrivateKey(), certificates);
                    bestRemainingLifetime = lifetime;
                }
            } catch (CacheException | NoRouteToCellException e) {
                LOGGER.debug("failed to contact {} querying for {}, {}: {}",
                             path, dn, primaryFqan, e.getMessage());
            } catch (KeyStoreException e) {
                LOGGER.warn("Received invalid key pair from {} for {}, {}: {}",
                             path, dn, primaryFqan, e.getMessage());
            }
        }

        return bestRemainingLifetime < units.toMillis(minimumValidity) ? null : bestCredential;
    }

    public StaticOpenIdCredential getDelegatedCredential(String token,
                                                         ImmutableMap<String, OpenIdClientSecret> clientSecrets)
            throws InterruptedException, ErrorResponseException
    {
        HttpClient client = HttpClientBuilder.create().build();
        for (Map.Entry<String, OpenIdClientSecret> entry: clientSecrets.entrySet())
        {
            String host = entry.getKey();
            String id = entry.getValue().getId();
            String secret = entry.getValue().getSecret();

            try {
                JSONObject json = delegateOpenIdCredential(client,
                                                           buildRequest(token,
                                                                        host,
                                                                        id,
                                                                        secret));

                return createOidcCredential(host, id, secret, json);
            } catch (AuthenticationException | IOException | JSONException e) {
                LOGGER.warn("Fail Token Delegation with Openid Provider {}", host);
            }
        }
        return null;
    }

    private HttpPost buildRequest(String token, String host, String clientId, String clientSecret)
            throws UnsupportedEncodingException, AuthenticationException
    {
        UsernamePasswordCredentials clientCreds = new UsernamePasswordCredentials(clientId, clientSecret);
        BasicScheme scheme = new BasicScheme(Charsets.UTF_8);

        HttpPost post = new HttpPost(tokenEndPoint(host));
        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("grant_type", GRANT_TYPE));
        params.add(new BasicNameValuePair("audience", clientId));
        params.add(new BasicNameValuePair("subject_token", token));
        params.add(new BasicNameValuePair("subject_token_type", TOKEN_TYPE));
        params.add(new BasicNameValuePair("scope", SCOPE));

        post.setEntity(new UrlEncodedFormEntity(params));
        post.addHeader(scheme.authenticate(clientCreds, post, new BasicHttpContext()));
        return post;
    }

    private JSONObject delegateOpenIdCredential(HttpClient client, HttpPost post) throws IOException
    {
        HttpResponse response = client.execute(post);
        if (response.getStatusLine().getStatusCode() == 200) {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            response.getEntity().writeTo(os);
            return new JSONObject(new String(os.toByteArray(), Charsets.UTF_8));
        } else {
            throw new IOException("Http Request Error (" +
                    response.getStatusLine().getStatusCode() + "): [" +
                    response.getStatusLine().getReasonPhrase() + "]");
        }
    }

    private StaticOpenIdCredential createOidcCredential(String host,
                                                        String clientId,
                                                        String clientSecret,
                                                        JSONObject json)
    {
        return new Builder().accessToken(json.getString("access_token"))
                            .expiry(json.getLong("expires_in"))
                            .refreshToken(json.getString("refresh_token"))
                            .issuedTokenType(json.getString("issued_token_type"))
                            .scope(json.getString("scope"))
                            .tokenType(json.getString("token_type"))
                            .clientCredential(new OpenIdClientSecret(clientId, clientSecret))
                            .provider(tokenEndPoint(host))
                            .build();
    }

    private String tokenEndPoint(String hostname)
    {
        return "https://" + hostname + "/token";
    }

    private static long calculateRemainingLifetime(X509Certificate[] certificates)
    {
        long earliestExpiry = Long.MAX_VALUE;

        for (X509Certificate certificate : certificates) {
            earliestExpiry = Math.min(earliestExpiry, certificate.getNotAfter().getTime());
        }

        long now = System.currentTimeMillis();

        return (earliestExpiry <= now) ? 0 : earliestExpiry - now;
    }
}
