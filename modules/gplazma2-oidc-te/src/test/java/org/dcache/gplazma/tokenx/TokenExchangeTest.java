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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
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
        plugin = aPlugin().build();


        plugin.tokenExchange("InvalidToken"); 
        
    }

    @Test
    public void tokenExchangeTest() throws Exception {

        System.out.println("=================================[tokenExchangeTest():");
        // plugin = new TokenExchange();
        plugin = aPlugin().build();

        // String helmholtzToken = "eyJ0eXAiOiJhdCtqd3QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJiNWMzNTViNS0xY2NhLTRmZmEtYjRmNy02MDg4OGRhODc4ZmEiLCJhdWQiOiJwdWJsaWMtb2lkYy1hZ2VudCIsInNjb3BlIjoiZWR1cGVyc29uX2VudGl0bGVtZW50IGVtYWlsIGVkdXBlcnNvbl9zY29wZWRfYWZmaWxpYXRpb24gb2ZmbGluZV9hY2Nlc3MgcHJvZmlsZSBvcGVuaWQgZWR1cGVyc29uX3VuaXF1ZV9pZCIsImlzcyI6Imh0dHBzOlwvXC9sb2dpbi1kZXYuaGVsbWhvbHR6LmRlXC9vYXV0aDIiLCJleHAiOjE2OTc0NTA1MDYsImlhdCI6MTY5NzQ0NjUwNiwianRpIjoiYzM1MGI4NjYtOWMxNi00N2Q2LTg1ODMtMGY1YmYyMjQxZmU4IiwiY2xpZW50X2lkIjoicHVibGljLW9pZGMtYWdlbnQifQ.Ypifl_u_pBO2Kf65obgdzo-rbKSp35-GIq1fFk0nTTml9Ogl8LMY8wFAd-4Yhir3yCkvvDYzorzP8_NPkj_mvqxJRGGcku0Q80INTKTYew1eW4qlFhMqjRs9QCmVcpzYfmlBvlTfIYZ_oXr1SJAGJLfvzG2IyzGKr0_w3V-EgLEGuljhZAc9bibGbRn569_oX2n9TTqi-mGmJU72C4ssy88QK3WyieFGn0MQZdi95-WMyJG13Vo9qVAdgRdXTmOCzJdlvYLBRAWkUp6v9yEdxnbQb5REAakCirot1EBUOehpST_LrBjZCl0oQJa0kqrkpJKb7eejy9F1EISzuq7eTg";
        String token = "eyJ0eXAiOiJhdCtqd3QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIzNWFkOWJlOC0zOThkLTQzNjMtYjhlYS05MDJmYjU1YWM3YzUiLCJhdWQiOiJwdWJsaWMtb2lkYy1hZ2VudCIsInNjb3BlIjoiZWR1cGVyc29uX2VudGl0bGVtZW50IHN5czpzY2ltOnJlYWRfcHJvZmlsZSBlbnRpdGxlbWVudHMgZWR1cGVyc29uX2Fzc3VyYW5jZSB2b3BlcnNvbl9leHRlcm5hbF9hZmZpbGlhdGlvbiBlZHVwZXJzb25fc2NvcGVkX2FmZmlsaWF0aW9uIGVkdXBlcnNvbl9wcmluY2lwYWxfbmFtZSBwcm9maWxlIHN5czpzY2ltOnJlYWRfbWVtYmVyc2hpcHMgY3JlZGVudGlhbHMgc2luZ2xlLWxvZ291dCBzbiBlbWFpbCBvZmZsaW5lX2FjY2VzcyBvcGVuaWQgZWR1cGVyc29uX3VuaXF1ZV9pZCBkaXNwbGF5X25hbWUgdm9wZXJzb25faWQgc3lzOnNjaW06cmVhZF9zZWxmX2dyb3VwIiwiaXNzIjoiaHR0cHM6XC9cL2xvZ2luLmhlbG1ob2x0ei5kZVwvb2F1dGgyIiwiZXhwIjoxNzA4Njk3OTg1LCJpYXQiOjE3MDg2OTM5ODUsImp0aSI6ImQwOGU4OWVhLTM1OWItNGFmMC04YTk3LTFhNzJkODljZTc5NSIsImNsaWVudF9pZCI6InB1YmxpYy1vaWRjLWFnZW50In0.CKF_BJcQoXDYhREsLqPDznuLbQdqMfJpjBL7XuLL9GEYGPKJrouUFL-v3jMZoB2YqlUqAr-Dhl_X-KyQwLtEi1Xl7kvs8s8k0ZsYYV7INLZ9P4_41sxkh2LDE4nQuVfW_gUYFXsQb5dD6i4X7kbfdaMsNloA1GYP-pYPF15sOTtqKCXcmm_nBFFe6tJKkvE7rV75mCoEP-viTRRavsKxiuKVVQM_M_VqiZUX5J9ppGys4NwmANwVZuY57leoaDo6CFDxxt8A6MhdJ45cjUBd8Ls-iQf8sq5J8uNV4rIvuR1xapCDTuis9kw2ns60TIQupl0ZYOR_CCv_-K3qeqVdgA";

        String result = null;

        try {
            result = plugin.tokenExchange(token);
        }
        catch (IOException e) {
        
            // LOG.debug("Failed to parse token: {}", e.toString());
            System.out.println("Failed to parse token: " + e.toString());
        }

        System.out.println("result: " + result);

        System.out.println("=================================tokenExchangeTest():]");
    }

    private PluginBuilder aPlugin() {
        return new PluginBuilder();
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

        public TokenExchange build() {
            // return new TokenExchange(properties);
            return new TokenExchange();
        }
    }

}