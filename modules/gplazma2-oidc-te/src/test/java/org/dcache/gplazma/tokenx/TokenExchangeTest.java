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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Test;
import org.mockito.Mockito;

public class TokenExchangeTest {

    private TokenExchange plugin; 
    private CloseableHttpClient httpClient;

    @Test(expected=IOException.class)
    public void shouldFailWithInvalidToken() throws Exception {
        plugin = new TokenExchange();
        // plugin = aPlugin().build();

        plugin.tokenExchange("InvalidToken"); 
    }

    @Test
    public void tokenExchangeTest() throws Exception {

        given(aPlugin().withAuthorizationServer(aAuthorizationServer().thatExchanges()));
        // plugin = new TokenExchange();

        String token = "eyJ0eXAiOiJhdCtqd3QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIzNWFkOWJlOC0zOThkLTQzNjMtYjhlYS05MDJmYjU1YWM3YzUiLCJhdWQiOiJwdWJsaWMtb2lkYy1hZ2VudCIsInNjb3BlIjoiZWR1cGVyc29uX2VudGl0bGVtZW50IHN5czpzY2ltOnJlYWRfcHJvZmlsZSBlbnRpdGxlbWVudHMgZWR1cGVyc29uX2Fzc3VyYW5jZSB2b3BlcnNvbl9leHRlcm5hbF9hZmZpbGlhdGlvbiBlZHVwZXJzb25fc2NvcGVkX2FmZmlsaWF0aW9uIGVkdXBlcnNvbl9wcmluY2lwYWxfbmFtZSBwcm9maWxlIHN5czpzY2ltOnJlYWRfbWVtYmVyc2hpcHMgY3JlZGVudGlhbHMgc2luZ2xlLWxvZ291dCBzbiBlbWFpbCBvZmZsaW5lX2FjY2VzcyBvcGVuaWQgZWR1cGVyc29uX3VuaXF1ZV9pZCBkaXNwbGF5X25hbWUgdm9wZXJzb25faWQgc3lzOnNjaW06cmVhZF9zZWxmX2dyb3VwIiwiaXNzIjoiaHR0cHM6XC9cL2xvZ2luLmhlbG1ob2x0ei5kZVwvb2F1dGgyIiwiZXhwIjoxNzA4OTU0MzQ1LCJpYXQiOjE3MDg5NTAzNDUsImp0aSI6IjFjYmZiNTlmLTgzMjMtNDYwYi1hMjZkLWQ2ZTVhYWNkZTYxNiIsImNsaWVudF9pZCI6InB1YmxpYy1vaWRjLWFnZW50In0.sm89O9sGSlE916ui1Epu8Ss9BtwV4jr9yHcckyZoSFF81z6xYgC6c2WLHKDKnmMI_5Brv7CMgOXXmMIZub_UTOFqtCY9TmMfkwO--oG7JXGd_vQegIZ3FMlHA4smY48f_nvj52q0hQhQ_Aj63BZ1EuwX4wwhEJQov_ndmCZytAPHeVNZu0eTWJmO0VBY4-pWH_C073BR5-9DsZPaz_ejkLdhw5MHZN5ko6IqU_O5crxlC-klpnskRmeRhmaHt6-9CXrdMA8iMqij3LFbG0WkllFFXX35GKWBT-iFdbztF7FlrMhSydSSjQ-j0p1dWc9e6HZfUr5hOQNfYThFkMA2MA";
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
            return new TokenExchange(httpClient);
        }
    }

}