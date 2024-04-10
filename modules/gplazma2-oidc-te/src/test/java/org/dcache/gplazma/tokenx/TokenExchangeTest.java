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
        properties.put(CLIENT_SECRET, "tj05R7fKtV0Pqkxxnby5aic1AsiiROHy");
        properties.put(GRANT_TYPE, "urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange");
        properties.put(SUBJECT_ISSUER, "oidc");
        properties.put(SUBJECT_TOKEN_TYPE, "urn%3Aietf%3Aparams%3Aoauth%3Atoken-type%3Aaccess_token");
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

        String token = "eyJ0eXAiOiJhdCtqd3QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIzNWFkOWJlOC0zOThkLTQzNjMtYjhlYS05MDJmYjU1YWM3YzUiLCJhdWQiOiJwdWJsaWMtb2lkYy1hZ2VudCIsInNjb3BlIjoiZWR1cGVyc29uX2VudGl0bGVtZW50IHN5czpzY2ltOnJlYWRfcHJvZmlsZSBlbnRpdGxlbWVudHMgZWR1cGVyc29uX2Fzc3VyYW5jZSB2b3BlcnNvbl9leHRlcm5hbF9hZmZpbGlhdGlvbiBlZHVwZXJzb25fc2NvcGVkX2FmZmlsaWF0aW9uIGVkdXBlcnNvbl9wcmluY2lwYWxfbmFtZSBwcm9maWxlIHN5czpzY2ltOnJlYWRfbWVtYmVyc2hpcHMgY3JlZGVudGlhbHMgc2luZ2xlLWxvZ291dCBzbiBlbWFpbCBvZmZsaW5lX2FjY2VzcyBvcGVuaWQgZWR1cGVyc29uX3VuaXF1ZV9pZCBkaXNwbGF5X25hbWUgdm9wZXJzb25faWQgc3lzOnNjaW06cmVhZF9zZWxmX2dyb3VwIiwiaXNzIjoiaHR0cHM6XC9cL2xvZ2luLmhlbG1ob2x0ei5kZVwvb2F1dGgyIiwiZXhwIjoxNzEyNzYxMjExLCJpYXQiOjE3MTI3NTcyMTEsImp0aSI6ImNiMWY1MjY3LTg5YWUtNDQ4NC1hNmM0LWFjNDQ0ZWI3MTIzZCIsImNsaWVudF9pZCI6InB1YmxpYy1vaWRjLWFnZW50In0.0zqUEvg-QjqQa8jh-ipqPa1JF1vmRIwJqth8wg4h4vXO7sUvqaNI2rp8kJB13te6Hw5ubRdzmbQe_So-Eql5NybKyEXPjui5uiMGQ13TI-_Byd48GQdRewki8cCGoQxJbKjBDY7KPxJ7Bsv5BkvHDvaIat_HV0_ykOWs_TvKyfMBBwcZVHnYWXzZr39ARHGFcLb9npHJtE-MCgDisQzsGKk98v0lAdI_aAoh49pw-QNVSeOLIoJwV6j0XqX02s-5kx8PgV7e78QMFAz7cgEwa3MAsHuOumOgiQdhxnUtaWDOHJ-JBSutvnR57D-DBKOog5-_bYMCx5npMv6d0xrDoA";

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