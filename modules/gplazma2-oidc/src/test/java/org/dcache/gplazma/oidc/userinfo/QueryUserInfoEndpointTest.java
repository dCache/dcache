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
package org.dcache.gplazma.oidc.userinfo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.oidc.IdentityProvider;
import org.dcache.gplazma.oidc.MockHttpClientBuilder;
import org.dcache.gplazma.oidc.TokenProcessor;
import org.dcache.gplazma.oidc.helpers.JsonHttpClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.dcache.gplazma.oidc.MockHttpClientBuilder.aClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;

public class QueryUserInfoEndpointTest {

    /**
     * Fluent class for building QueryUserInfoEndpoint.
     */
    private class QueryUserInfoEndpointBuilder {
        private final Properties properties = new Properties();
        private final Set<IdentityProvider> providers = new HashSet<>();

        public QueryUserInfoEndpointBuilder() {

            // We assign various defaults (based on 'defaults/gplazma.properties') simply to
            // keep the size of various unit test methods reasonable.
            properties.setProperty("gplazma.oidc.http.slow-threshold", "2");
            properties.setProperty("gplazma.oidc.http.slow-threshold.unit", "SECONDS");
            properties.setProperty("gplazma.oidc.concurrent-requests", "1");
            properties.setProperty("gplazma.oidc.discovery-cache", "1");
            properties.setProperty("gplazma.oidc.discovery-cache.unit", "HOURS");
            properties.setProperty("gplazma.oidc.access-token-cache.size", "1000");
            properties.setProperty("gplazma.oidc.access-token-cache.refresh", "100");
            properties.setProperty("gplazma.oidc.access-token-cache.refresh.unit", "SECONDS");
            properties.setProperty("gplazma.oidc.access-token-cache.expire", "120");
            properties.setProperty("gplazma.oidc.access-token-cache.expire.unit", "SECONDS");
        }

        public QueryUserInfoEndpointBuilder withProperty(String key, String value) {
            properties.setProperty(key, value);
            return this;
        }

        public QueryUserInfoEndpointBuilder withIdentityProvider(IdentityProvider ip) {
            providers.add(ip);
            return this;
        }

        public QueryUserInfoEndpoint build() {
            JsonHttpClient httpClient = new JsonHttpClient(client);
            return new QueryUserInfoEndpoint(properties, httpClient, providers);
        }
    }

    private final ObjectMapper mapper = new ObjectMapper();

    private TokenProcessor processor;
    private HttpClient client;

    @Before
    public void setup() {
        processor = null;
        client = null;
    }

    @After
    public void teardown() {
        processor.shutdown();
    }

    @Test
    public void shouldParseSubClaim() throws Exception {
        given(aClient()
            .onGet("https://oidc.example.org/.well-known/openid-configuration").responds()
                .withEntity("{\"userinfo_endpoint\": \"https://oidc.example.org/oauth\"}")
            .onGet("https://oidc.example.org/oauth").responds()
                .withEntity("{\"sub\": \"ae7fb9688e0683999d864ab5618b92b9\","
                        + "\"another_claim\": \"another value\","
                        + "\"null_claim\": null,"
                        + "\"number_claim\": 42,"
                        + "\"array_claim\": [ \"first\", \"second\"]}"));
        given(aQueryUserInfoEndpoint()
                .withIdentityProvider(new IdentityProvider("EXAMPLE", "https://oidc.example.org/")));

        var result = processor.extract("an-access-token");

        assertThat(result.claims(), hasEntry("sub", jsonString("ae7fb9688e0683999d864ab5618b92b9")));
        assertThat(result.claims(), hasEntry("another_claim", jsonString("another value")));
        assertThat(result.claims(), hasEntry("null_claim", jsonNull()));
        assertThat(result.claims(), hasEntry("number_claim", jsonNumber(42)));
        assertThat(result.claims(), hasEntry("array_claim", jsonStringArray("first", "second")));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldThrowExceptionIfConfigEndpointHasError() throws Exception {
        given(aClient()
            .onGet("https://oidc.example.org/.well-known/openid-configuration").responds()
            .withStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR).withoutEntity());
        given(aQueryUserInfoEndpoint()
                .withIdentityProvider(new IdentityProvider("EXAMPLE", "https://oidc.example.org/")));

        processor.extract("an-access-token");
    }

    @Test(expected=AuthenticationException.class)
    public void shouldThrowExceptionIfUserInfoEndpointHasError() throws Exception {
        given(aClient()
            .onGet("https://oidc.example.org/.well-known/openid-configuration").responds()
                .withEntity("{\"userinfo_endpoint\": \"https://oidc.example.org/oauth\"}")
            .onGet("https://oidc.example.org/oauth").responds()
                .withStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR).withoutEntity());
        given(aQueryUserInfoEndpoint()
                .withIdentityProvider(new IdentityProvider("EXAMPLE", "https://oidc.example.org/")));

        processor.extract("an-access-token");
    }

    private QueryUserInfoEndpointBuilder aQueryUserInfoEndpoint() {
        return new QueryUserInfoEndpointBuilder();
    }

    private void given(QueryUserInfoEndpointBuilder builder) {
        processor = builder.build();
    }

    private void given(MockHttpClientBuilder builder) {
        client = builder.build();
    }

    private JsonNode jsonOf(String json) throws JsonProcessingException {
        return mapper.readTree(json);
    }

    private JsonNode jsonNull() throws JsonProcessingException {
        return jsonOf("null");
    }

    private JsonNode jsonNumber(int value) throws JsonProcessingException {
        return jsonOf(Integer.toString(value));
    }

    private JsonNode jsonString(String json) throws JsonProcessingException {
        return jsonOf("\"" + json + "\"");
    }

    private JsonNode jsonStringArray(String... json) throws JsonProcessingException {
        return jsonOf(Stream.of(json).collect(Collectors.joining("\", \"", "[\"", "\"]")));
    }
}