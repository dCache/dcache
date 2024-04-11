/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2022 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.tokenx;

import static org.dcache.gplazma.tokenx.TokenExchange.AUDIENCE;
import static org.dcache.gplazma.tokenx.TokenExchange.CLIENT_ID;
import static org.dcache.gplazma.tokenx.TokenExchange.CLIENT_SECRET;
import static org.dcache.gplazma.tokenx.TokenExchange.GRANT_TYPE;
import static org.dcache.gplazma.tokenx.TokenExchange.SUBJECT_ISSUER;
import static org.dcache.gplazma.tokenx.TokenExchange.SUBJECT_TOKEN_TYPE;
import static org.dcache.gplazma.tokenx.TokenExchange.TOKEN_EXCHANGE_URL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
// import org.apache.http.impl.client.HttpClients;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TokenExchangeTest {

    private TokenExchange plugin; 
    private CloseableHttpClient httpClient;

    @Before
    public void setup() {
        plugin = null;
        httpClient = null;
    }

    @Test(expected=IOException.class)
    public void shouldFailWithInvalidToken() throws Exception {
        given(aPlugin().withAuthorizationServer((aAuthorizationServer().thatThrowsIOException())));

        plugin.tokenExchange("InvalidToken"); 
    }

    @Test
    public void tokenExchangeTest() throws Exception {

        given(aPlugin().withAuthorizationServer(aAuthorizationServer().thatExchanges()));
        // httpClient = HttpClients.createDefault(); plugin = aPlugin().build();

        String token = "eyJ0eXAiOiJhdCtqd3QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIzNWFkOWJlOC0zOThkLTQzNjMtYjhlYS05MDJmYjU1YWM3YzUiLCJhdWQiOiJwdWJsaWMtb2lkYy1hZ2VudCIsInNjb3BlIjoiZWR1cGVyc29uX2VudGl0bGVtZW50IHN5czpzY2ltOnJlYWRfcHJvZmlsZSBlbnRpdGxlbWVudHMgZWR1cGVyc29uX2Fzc3VyYW5jZSB2b3BlcnNvbl9leHRlcm5hbF9hZmZpbGlhdGlvbiBlZHVwZXJzb25fc2NvcGVkX2FmZmlsaWF0aW9uIGVkdXBlcnNvbl9wcmluY2lwYWxfbmFtZSBwcm9maWxlIHN5czpzY2ltOnJlYWRfbWVtYmVyc2hpcHMgY3JlZGVudGlhbHMgc2luZ2xlLWxvZ291dCBzbiBlbWFpbCBvZmZsaW5lX2FjY2VzcyBvcGVuaWQgZWR1cGVyc29uX3VuaXF1ZV9pZCBkaXNwbGF5X25hbWUgdm9wZXJzb25faWQgc3lzOnNjaW06cmVhZF9zZWxmX2dyb3VwIiwiaXNzIjoiaHR0cHM6XC9cL2xvZ2luLmhlbG1ob2x0ei5kZVwvb2F1dGgyIiwiZXhwIjoxNzEyODQ5NjkxLCJpYXQiOjE3MTI4NDU2OTEsImp0aSI6ImFkYTAyMTdjLTMzMjEtNGE4Mi1iMTA3LWI3NjUxYmQ4Yzk1MyIsImNsaWVudF9pZCI6InB1YmxpYy1vaWRjLWFnZW50In0.l9xKp03pf64dBOb9IdzAN1RIJtUi0gWZNRg_k4rShcfIvoTkNf3rVRWK9JTO_fjG9XUwTCOxt6wdYt4eZWuTkQLfKPq6cUluKn6xRGGB60Fb5M9iY-coBSjwNNjqw4S6YpaAa7rfKG2TQsvRV-HRRNNnXWQHJ8flGfNaQu0Gu5HRGevJ5Ple1E6n7gi-H_3YkxRE2nA7K5ZizQMCoVlJFfSGr9UE65iJDNl_VDe_OmCL2wz02A7qC7sZDteMmoxl6zKA0tgF-AqlF1AJsRW8rWtEKoav6cKPVKzZQ21OTvmHuGAxQBZ0nC07StZztma2e3Arsn-wohdyoueaj9OqQw";

        String result = plugin.tokenExchange(token);

        // System.out.println("result: " + result);
        assertThat(result, equalToIgnoringCase("valid.access.token"));
    }

    private void given(PluginBuilder builder) {
        plugin = builder.build();
    }

    private PluginBuilder aPlugin() {
        return new PluginBuilder();
    }

    private AuthorizationServerBuilder aAuthorizationServer() {
        return new AuthorizationServerBuilder();
    }


    /**
     * A fluent class for building a (real) TokenExchange plugin.
     */
    private static class AuthorizationServerBuilder {

        private final CloseableHttpClient httpClient = mock(CloseableHttpClient.class);

        public AuthorizationServerBuilder thatExchanges() {
            CloseableHttpResponse mockResponse = Mockito.mock(CloseableHttpResponse.class);
            String jsonContent = "{\"access_token\": \"valid.access.token\"}";
            try {
                HttpEntity entity = new StringEntity(jsonContent);
                Mockito.when(mockResponse.getEntity()).thenReturn(entity);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("Impossible exception caught", e);
            }
        
            try {
                when(httpClient.execute(any()))
                        .thenReturn(mockResponse); 
            } catch (IOException e)
            {
                throw new RuntimeException("Impossible exception caught", e); 
            }

            return this;
        }

        public AuthorizationServerBuilder thatThrowsIOException() throws IOException {
            throw new IOException("IOException thrown");
        }

        public CloseableHttpClient build() {
            return httpClient;
        }

    }

    /**
     * A fluent class for building a (real) TokenExchange plugin.
     */
    private class PluginBuilder {

        private Properties properties = new Properties();

        public PluginBuilder() {
            properties.put(TOKEN_EXCHANGE_URL, "https://keycloak.desy.de/auth/realms/production/protocol/openid-connect/token");
            properties.put(CLIENT_ID, "token-exchange");
            properties.put(CLIENT_SECRET, "secret");
            properties.put(GRANT_TYPE, "urn:ietf:params:oauth:grant-type:token-exchange");
            properties.put(SUBJECT_ISSUER, "oidc");
            properties.put(SUBJECT_TOKEN_TYPE, "urn:ietf:params:oauth:token-type:access_token");
            properties.put(AUDIENCE, "token-exchange");
        }

        public PluginBuilder withAuthorizationServer(AuthorizationServerBuilder builder) {
            httpClient = builder.build();
            return this;
        }

        public TokenExchange build() {
            return new TokenExchange(properties, httpClient);
        }
    }

}