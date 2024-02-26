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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.junit.Test;
import org.mockito.Mockito;

public class TokenExchangeTest {

    private TokenExchange plugin; 
    private CloseableHttpClient httpClient;

    // private final HttpClient client = mock(HttpClient.class);

    @Test
    public void testWithMock() throws IOException, InterruptedException, URISyntaxException {

        System.out.println("=================================[testWithMock():");
        CloseableHttpResponse mockResponse = Mockito.mock(CloseableHttpResponse.class);

        // Define the behavior of the mock object to return a StringEntity with your JSON content
        String jsonContent = "{\"key\": \"value\"}";
        HttpEntity entity = new StringEntity(jsonContent);
        Mockito.when(mockResponse.getEntity()).thenReturn(entity);
        

        // Mock HttpClient behavior
        CloseableHttpClient mockClient = mock(CloseableHttpClient.class);

        when(mockClient.execute(any()))
                .thenReturn(mockResponse);


        String postBody = "client_id="
                + "&client_secret=test"
                + "&grant_type=test"
                + "&subject_token=test"
                + "&subject_issuer=test"
                + "&subject_token_type=test"
                + "&audience=test";
    
        URI uri = new URIBuilder("http://example.org").build();

        HttpPost httpPost = new HttpPost(uri);
    
        // Set headers
        httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded");
    
        // Set request body
        StringEntity stringEntity = new StringEntity(postBody);
        httpPost.setEntity(stringEntity);

        String responseBody = null;

        try (CloseableHttpResponse response = mockClient.execute(httpPost)) {
    
            HttpEntity responseEntity = response.getEntity();
            responseBody = EntityUtils.toString(responseEntity);
    
            // Handle the response as needed
            System.out.println("Response: " + response);
            System.out.println("Response body: " + responseBody);
    
            // return responseBody;
        }

        System.out.println("=================================testWithMock():]");
    }

    @Test(expected=IOException.class)
    public void shouldFailWithInvalidToken() throws Exception {
        // plugin = new TokenExchange();
        plugin = aPlugin().withHttpClient().build();


        plugin.tokenExchange("InvalidToken"); 
        
    }

    @Test
    public void tokenExchangeTest() throws Exception {

        System.out.println("=================================[tokenExchangeTest():");
        // plugin = aPlugin().withHttpClient().build();
        
        plugin = aPlugin().withAuthorizationServer(aAuthorizationServer().thatExchanges()).build();
        // plugin = new TokenExchange();

        String token = "eyJ0eXAiOiJhdCtqd3QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIzNWFkOWJlOC0zOThkLTQzNjMtYjhlYS05MDJmYjU1YWM3YzUiLCJhdWQiOiJwdWJsaWMtb2lkYy1hZ2VudCIsInNjb3BlIjoiZWR1cGVyc29uX2VudGl0bGVtZW50IHN5czpzY2ltOnJlYWRfcHJvZmlsZSBlbnRpdGxlbWVudHMgZWR1cGVyc29uX2Fzc3VyYW5jZSB2b3BlcnNvbl9leHRlcm5hbF9hZmZpbGlhdGlvbiBlZHVwZXJzb25fc2NvcGVkX2FmZmlsaWF0aW9uIGVkdXBlcnNvbl9wcmluY2lwYWxfbmFtZSBwcm9maWxlIHN5czpzY2ltOnJlYWRfbWVtYmVyc2hpcHMgY3JlZGVudGlhbHMgc2luZ2xlLWxvZ291dCBzbiBlbWFpbCBvZmZsaW5lX2FjY2VzcyBvcGVuaWQgZWR1cGVyc29uX3VuaXF1ZV9pZCBkaXNwbGF5X25hbWUgdm9wZXJzb25faWQgc3lzOnNjaW06cmVhZF9zZWxmX2dyb3VwIiwiaXNzIjoiaHR0cHM6XC9cL2xvZ2luLmhlbG1ob2x0ei5kZVwvb2F1dGgyIiwiZXhwIjoxNzA4OTU0MzQ1LCJpYXQiOjE3MDg5NTAzNDUsImp0aSI6IjFjYmZiNTlmLTgzMjMtNDYwYi1hMjZkLWQ2ZTVhYWNkZTYxNiIsImNsaWVudF9pZCI6InB1YmxpYy1vaWRjLWFnZW50In0.sm89O9sGSlE916ui1Epu8Ss9BtwV4jr9yHcckyZoSFF81z6xYgC6c2WLHKDKnmMI_5Brv7CMgOXXmMIZub_UTOFqtCY9TmMfkwO--oG7JXGd_vQegIZ3FMlHA4smY48f_nvj52q0hQhQ_Aj63BZ1EuwX4wwhEJQov_ndmCZytAPHeVNZu0eTWJmO0VBY4-pWH_C073BR5-9DsZPaz_ejkLdhw5MHZN5ko6IqU_O5crxlC-klpnskRmeRhmaHt6-9CXrdMA8iMqij3LFbG0WkllFFXX35GKWBT-iFdbztF7FlrMhSydSSjQ-j0p1dWc9e6HZfUr5hOQNfYThFkMA2MA";

        String result = null;

        try {
            result = plugin.tokenExchange(token);
        }
        catch (IOException e) {
        
            // LOG.debug("Failed to parse token: {}", e.toString());
            System.out.println("Failed to parse token: " + e.toString());
        }


        System.out.println("result: " + result);

        assertThat(result, equalToIgnoringCase("valid.access.token"));

        System.out.println("=================================tokenExchangeTest():]");
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
        

            // httpClient = mock(CloseableHttpClient.class);
            // Mock HttpClient behavior

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
        private Properties properties = new Properties();

        public PluginBuilder() {
            // Use a reasonable default, just to keep tests a bit smaller.
            properties.setProperty("gplazma.oidc.audience-targets", "");
            // System.out.println("public PluginBuilder()");
        }

        public PluginBuilder withHttpClient() throws Exception {
            // httpClient = HttpClients.createDefault();
            // CloseableHttpClient mockClient = mock(CloseableHttpClient.class);

            CloseableHttpResponse mockResponse = Mockito.mock(CloseableHttpResponse.class);
            String jsonContent = "{\"key\": \"value\"}";
            HttpEntity entity = new StringEntity(jsonContent);
            Mockito.when(mockResponse.getEntity()).thenReturn(entity);
        

            httpClient = mock(CloseableHttpClient.class);
            // Mock HttpClient behavior

            when(httpClient.execute(any()))
                    .thenReturn(mockResponse);

            return this;
        }

        public PluginBuilder withAuthorizationServer(AuthorizationServerBuilder builder) {
            httpClient = builder.build();
            return this;
        }

        public TokenExchange build() {
            // return new TokenExchange(properties);
            return new TokenExchange(httpClient);
        }
    }

}