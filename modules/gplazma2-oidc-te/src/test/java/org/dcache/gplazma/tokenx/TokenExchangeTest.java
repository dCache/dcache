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
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;


public class TokenExchangeTest {

    private TokenExchange plugin; 
    private CloseableHttpClient httpClient;
    private Properties properties;

    @Before
    public void setup() {
        plugin = null;
        httpClient = null;

        properties = new Properties();
        properties.put(TOKEN_EXCHANGE_URL, "https://keycloak.desy.de/auth/realms/production/protocol/openid-connect/token");
        properties.put(CLIENT_ID, "token-exchange");
        properties.put(CLIENT_SECRET, "");
        properties.put(GRANT_TYPE, "urn:ietf:params:oauth:grant-type:token-exchange");
        properties.put(SUBJECT_ISSUER, "oidc");
        properties.put(SUBJECT_TOKEN_TYPE, "urn:ietf:params:oauth:token-type:access_token");
        properties.put(AUDIENCE, "token-exchange");

    }

    @Test(expected=IOException.class)
    public void shouldFailWithInvalidToken() throws Exception {
        plugin = new TokenExchange(properties);
        // plugin = aPlugin().build();

        plugin.tokenExchange("InvalidToken"); 
    }

    @Test
    public void tokenExchangeTest() throws Exception {

        // given(aPlugin().withAuthorizationServer(aAuthorizationServer().thatExchanges()));
        plugin = new TokenExchange(properties);

        String token = "eyJ0eXAiOiJhdCtqd3QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIzNWFkOWJlOC0zOThkLTQzNjMtYjhlYS05MDJmYjU1YWM3YzUiLCJhdWQiOiJwdWJsaWMtb2lkYy1hZ2VudCIsInNjb3BlIjoiZWR1cGVyc29uX2VudGl0bGVtZW50IHN5czpzY2ltOnJlYWRfcHJvZmlsZSBlbnRpdGxlbWVudHMgZWR1cGVyc29uX2Fzc3VyYW5jZSB2b3BlcnNvbl9leHRlcm5hbF9hZmZpbGlhdGlvbiBlZHVwZXJzb25fc2NvcGVkX2FmZmlsaWF0aW9uIGVkdXBlcnNvbl9wcmluY2lwYWxfbmFtZSBwcm9maWxlIHN5czpzY2ltOnJlYWRfbWVtYmVyc2hpcHMgY3JlZGVudGlhbHMgc2luZ2xlLWxvZ291dCBzbiBlbWFpbCBvZmZsaW5lX2FjY2VzcyBvcGVuaWQgZWR1cGVyc29uX3VuaXF1ZV9pZCBkaXNwbGF5X25hbWUgdm9wZXJzb25faWQgc3lzOnNjaW06cmVhZF9zZWxmX2dyb3VwIiwiaXNzIjoiaHR0cHM6XC9cL2xvZ2luLmhlbG1ob2x0ei5kZVwvb2F1dGgyIiwiZXhwIjoxNzEyNzY1ODgxLCJpYXQiOjE3MTI3NjE4ODEsImp0aSI6ImZlNGM0OTE4LTM1ZWQtNGJiNy1iOGVjLTk4YWVhZjQ3ODM5MyIsImNsaWVudF9pZCI6InB1YmxpYy1vaWRjLWFnZW50In0.JJBDSTEGXJzqhTG7VMSyljmWsFXtbs9oxd3U9XXGBZqAeTC28COGouzWLnqUJsCcY4e9ipoz9_Ti9L5nxkb_g_f4aP634GfkAmSopmPK_464NO_hOe3jsXl1sixo1KCQhkamxnDGA5MPoiiFM5Itqm0N9zLXWMD3r1BpH8P0HSKdLktduWkCZopKVRCGG3kirj3Aw8c2HKA-hZw16OFSdE52ZR-X5B05U8T9mL779HhRqTUJHtMoqjnUvwa7zPLCU1Cn7uDq_BitGKtbiQiRc1-8gI2DaWMfwzCELur7YLCnaveGvnCkbeVPuOWde_soUeRKGc3qgmAA8Np63_2YPg";

        String result = plugin.tokenExchange(token);

        System.out.println("result: " + result);
        // assertThat(result, equalToIgnoringCase("valid.access.token"));
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

        public CloseableHttpClient build() {
            return httpClient;
        }

    }

    /**
     * A fluent class for building a (real) TokenExchange plugin.
     */
    private class PluginBuilder {

        public PluginBuilder() {}

        public PluginBuilder withAuthorizationServer(AuthorizationServerBuilder builder) {
            httpClient = builder.build();
            return this;
        }

        public TokenExchange build() {
            return new TokenExchange(properties, httpClient);
        }
    }

}