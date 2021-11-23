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
import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.http.client.HttpClient;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.oidc.IdentityProvider;
import org.dcache.gplazma.oidc.JwtFactory;
import org.dcache.gplazma.oidc.MockHttpClientBuilder;
import org.dcache.gplazma.oidc.Profile;
import org.dcache.gplazma.oidc.TokenProcessor;
import org.dcache.gplazma.oidc.helpers.JsonHttpClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.http.HttpStatus.SC_INTERNAL_SERVER_ERROR;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.dcache.gplazma.oidc.MockHttpClientBuilder.aClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;

public class QueryUserInfoEndpointTest {
    private static final Profile IGNORE_ALL = (i,c) -> Collections.emptySet();

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

    private final JwtFactory jwtFactory = new JwtFactory();
    private final ObjectMapper mapper = new ObjectMapper();

    private TokenProcessor processor;
    private HttpClient client;
    private String jwt;

    @Before
    public void setup() {
        processor = null;
        client = null;
        jwt = null;
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
                .withIdentityProvider(new IdentityProvider("EXAMPLE", URI.create("https://oidc.example.org/"), IGNORE_ALL)));

        var result = processor.extract("an-access-token");

        assertThat(result.claims(), hasEntry("sub", jsonString("ae7fb9688e0683999d864ab5618b92b9")));
        assertThat(result.claims(), hasEntry("another_claim", jsonString("another value")));
        assertThat(result.claims(), hasEntry("null_claim", jsonNull()));
        assertThat(result.claims(), hasEntry("number_claim", jsonNumber(42)));
        assertThat(result.claims(), hasEntry("array_claim", jsonStringArray("first", "second")));
    }

    @Test
    public void shouldTargetSingleIpForJwt() throws Exception {
        given(aClient()
            .onGet("https://oidc.example.org/.well-known/openid-configuration").responds()
                .withEntity("{\"userinfo_endpoint\": \"https://oidc.example.org/oauth\"}")
            .onGet("https://oidc.example.org/oauth").responds()
                .withEntity("{\"sub\": \"ae7fb9688e0683999d864ab5618b92b9\","
                        + "\"another_claim\": \"another value\","
                        + "\"null_claim\": null,"
                        + "\"number_claim\": 42,"
                        + "\"array_claim\": [ \"first\", \"second\"]}")
            .onGet("https://other-oidc.example.com/.well-known/openid-configuration").responds()
                .withEntity("{\"userinfo_endpoint\": \"https://other-oidc.example.com/userinfo\"}")
            .onGet("https://other-oidc.example.com/userinfo").responds().withStatusCode(404).withoutEntity());
        given(aQueryUserInfoEndpoint()
                .withIdentityProvider(new IdentityProvider("EXAMPLE-1", URI.create("https://oidc.example.org/"), IGNORE_ALL))
                .withIdentityProvider(new IdentityProvider("EXAMPLE-2", URI.create("https://other-oidc.example.com/"), IGNORE_ALL)));
        given(aJwt().withPayloadClaim("iss", "https://oidc.example.org/"));

        var result = processor.extract(jwt);

        assertThat(result.claims(), hasEntry("sub", jsonString("ae7fb9688e0683999d864ab5618b92b9")));
        assertThat(result.claims(), hasEntry("another_claim", jsonString("another value")));
        assertThat(result.claims(), hasEntry("null_claim", jsonNull()));
        assertThat(result.claims(), hasEntry("number_claim", jsonNumber(42)));
        assertThat(result.claims(), hasEntry("array_claim", jsonStringArray("first", "second")));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectJwtWithUnknownIss() throws Exception {
        given(aClient());
        given(aQueryUserInfoEndpoint()
                .withIdentityProvider(new IdentityProvider("EXAMPLE-1", URI.create("https://oidc.example.org/"), IGNORE_ALL)));
        given(aJwt().withPayloadClaim("iss", "https://another-oidc.example.coms/"));

        processor.extract(jwt);
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectJwtWithBadIssValue() throws Exception {
        given(aClient());
        given(aQueryUserInfoEndpoint()
                .withIdentityProvider(new IdentityProvider("EXAMPLE-1", URI.create("https://oidc.example.org/"), IGNORE_ALL)));
        given(aJwt().withPayloadClaim("iss", "0:0"));

        processor.extract(jwt);
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectMalformedJwt() throws Exception {
        given(aClient()
            .onGet("https://oidc.example.org/.well-known/openid-configuration").responds()
                .withEntity("{\"userinfo_endpoint\": \"https://oidc.example.org/oauth\"}")
            .onGet("https://oidc.example.org/oauth").responds()
                .withEntity("{\"sub\": \"ae7fb9688e0683999d864ab5618b92b9\","
                        + "\"another_claim\": \"another value\","
                        + "\"null_claim\": null,"
                        + "\"number_claim\": 42,"
                        + "\"array_claim\": [ \"first\", \"second\"]}")
            .onGet("https://other-oidc.example.com/.well-known/openid-configuration").responds()
                .withEntity("{\"userinfo_endpoint\": \"https://other-oidc.example.com/userinfo\"}")
            .onGet("https://other-oidc.example.com/userinfo").responds().withStatusCode(SC_NOT_FOUND).withoutEntity());
        given(aQueryUserInfoEndpoint()
                .withIdentityProvider(new IdentityProvider("EXAMPLE-1", URI.create("https://oidc.example.org/"), IGNORE_ALL))
                .withIdentityProvider(new IdentityProvider("EXAMPLE-2", URI.create("https://other-oidc.example.com/"), IGNORE_ALL)));
        given(aJwt().withPayload("jwt payload that is not a valid json object"));

        processor.extract(jwt);
    }

    @Test
    public void shouldAcceptJwtWithoutIss() throws Exception {
        given(aClient()
            .onGet("https://oidc.example.org/.well-known/openid-configuration").responds()
                .withEntity("{\"userinfo_endpoint\": \"https://oidc.example.org/oauth\"}")
            .onGet("https://oidc.example.org/oauth").responds()
                .withEntity("{\"sub\": \"ae7fb9688e0683999d864ab5618b92b9\","
                        + "\"another_claim\": \"another value\","
                        + "\"null_claim\": null,"
                        + "\"number_claim\": 42,"
                        + "\"array_claim\": [ \"first\", \"second\"]}")
            .onGet("https://other-oidc.example.com/.well-known/openid-configuration").responds()
                .withEntity("{\"userinfo_endpoint\": \"https://other-oidc.example.com/userinfo\"}")
            .onGet("https://other-oidc.example.com/userinfo").responds().withStatusCode(SC_NOT_FOUND).withoutEntity());
        given(aQueryUserInfoEndpoint()
                .withIdentityProvider(new IdentityProvider("EXAMPLE-1", URI.create("https://oidc.example.org/"), IGNORE_ALL))
                .withIdentityProvider(new IdentityProvider("EXAMPLE-2", URI.create("https://other-oidc.example.com/"), IGNORE_ALL)));
        given(aJwt());

        var result = processor.extract(jwt);

        assertThat(result.claims(), hasEntry("sub", jsonString("ae7fb9688e0683999d864ab5618b92b9")));
        assertThat(result.claims(), hasEntry("another_claim", jsonString("another value")));
        assertThat(result.claims(), hasEntry("null_claim", jsonNull()));
        assertThat(result.claims(), hasEntry("number_claim", jsonNumber(42)));
        assertThat(result.claims(), hasEntry("array_claim", jsonStringArray("first", "second")));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectTokenIfMultipleIpsReturnNotFound() throws Exception {
        given(aClient()
            .onGet("https://oidc.example.org/.well-known/openid-configuration").responds()
                .withEntity("{\"userinfo_endpoint\": \"https://oidc.example.org/oauth\"}")
            .onGet("https://oidc.example.org/oauth").responds()
                .withEntity("{\"error\": \"unknown\","
                        + "\"error_description\": \"the token is unknown\"}")
            .onGet("https://other-oidc.example.com/.well-known/openid-configuration").responds()
                .withEntity("{\"userinfo_endpoint\": \"https://other-oidc.example.com/userinfo\"}")
            .onGet("https://other-oidc.example.com/userinfo").responds()
                .withEntity("{\"error\": \"unknown\","
                        + "\"error_description\": \"the token is unknown\"}"));
        given(aQueryUserInfoEndpoint()
                .withIdentityProvider(new IdentityProvider("EXAMPLE-1", URI.create("https://oidc.example.org/"), IGNORE_ALL))
                .withIdentityProvider(new IdentityProvider("EXAMPLE-2", URI.create("https://other-oidc.example.com/"), IGNORE_ALL)));

        processor.extract("an-access-token");
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectTokenIfMultipleIpsReturnInformation() throws Exception {
        given(aClient()
            .onGet("https://oidc.example.org/.well-known/openid-configuration").responds()
                .withEntity("{\"userinfo_endpoint\": \"https://oidc.example.org/oauth\"}")
            .onGet("https://oidc.example.org/oauth").responds()
                .withEntity("{\"sub\": \"ae7fb9688e0683999d864ab5618b92b9\"}")
            .onGet("https://other-oidc.example.com/.well-known/openid-configuration").responds()
                .withEntity("{\"userinfo_endpoint\": \"https://other-oidc.example.com/userinfo\"}")
            .onGet("https://other-oidc.example.com/userinfo").responds()
                .withEntity("{\"sub\": \"another-identity\"}"));
        given(aQueryUserInfoEndpoint()
                .withIdentityProvider(new IdentityProvider("EXAMPLE-1", URI.create("https://oidc.example.org/"), IGNORE_ALL))
                .withIdentityProvider(new IdentityProvider("EXAMPLE-2", URI.create("https://other-oidc.example.com/"), IGNORE_ALL)));

        processor.extract("an-access-token");
    }

    @Test(expected=AuthenticationException.class)
    public void shouldThrowExceptionIfConfigEndpointHasError() throws Exception {
        given(aClient()
            .onGet("https://oidc.example.org/.well-known/openid-configuration").responds()
            .withStatusCode(SC_INTERNAL_SERVER_ERROR).withoutEntity());
        given(aQueryUserInfoEndpoint()
                .withIdentityProvider(new IdentityProvider("EXAMPLE", URI.create("https://oidc.example.org/"), IGNORE_ALL)));

        processor.extract("an-access-token");
    }

    @Test(expected=AuthenticationException.class)
    public void shouldThrowExceptionIfUserInfoEndpointHasError() throws Exception {
        given(aClient()
            .onGet("https://oidc.example.org/.well-known/openid-configuration").responds()
                .withEntity("{\"userinfo_endpoint\": \"https://oidc.example.org/oauth\"}")
            .onGet("https://oidc.example.org/oauth").responds()
                .withStatusCode(SC_INTERNAL_SERVER_ERROR).withoutEntity());
        given(aQueryUserInfoEndpoint()
                .withIdentityProvider(new IdentityProvider("EXAMPLE", URI.create("https://oidc.example.org/"), IGNORE_ALL)));

        processor.extract("an-access-token");
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectIfUserInfoEndpointReturnsNonObject() throws Exception {
        given(aClient()
            .onGet("https://oidc.example.org/.well-known/openid-configuration").responds()
                .withEntity("{\"userinfo_endpoint\": \"https://oidc.example.org/oauth\"}")
            .onGet("https://oidc.example.org/oauth").responds()
                .withEntity("\"a valid json string\""));
        given(aQueryUserInfoEndpoint()
                .withIdentityProvider(new IdentityProvider("EXAMPLE", URI.create("https://oidc.example.org/"), IGNORE_ALL)));

        processor.extract("an-access-token");
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectIfUserInfoEndpointResponseMissingSubClaim() throws Exception {
        given(aClient()
            .onGet("https://oidc.example.org/.well-known/openid-configuration").responds()
                .withEntity("{\"userinfo_endpoint\": \"https://oidc.example.org/oauth\"}")
            .onGet("https://oidc.example.org/oauth").responds()
                .withEntity("{}"));
        given(aQueryUserInfoEndpoint()
                .withIdentityProvider(new IdentityProvider("EXAMPLE", URI.create("https://oidc.example.org/"), IGNORE_ALL)));

        processor.extract("an-access-token");
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectIfUserInfoEndpointResponseMalformed() throws Exception {
        given(aClient()
            .onGet("https://oidc.example.org/.well-known/openid-configuration").responds()
                .withEntity("{\"userinfo_endpoint\": \"https://oidc.example.org/oauth\"}")
            .onGet("https://oidc.example.org/oauth").responds()
                .withEntity("Not valid JSON"));
        given(aQueryUserInfoEndpoint()
                .withIdentityProvider(new IdentityProvider("EXAMPLE", URI.create("https://oidc.example.org/"), IGNORE_ALL)));

        processor.extract("an-access-token");
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectIfDiscoveryDocMissingUserInfoEndpoint() throws Exception {
        given(aClient()
            .onGet("https://oidc.example.org/.well-known/openid-configuration").responds()
                .withEntity("{}"));
        given(aQueryUserInfoEndpoint()
                .withIdentityProvider(new IdentityProvider("EXAMPLE", URI.create("https://oidc.example.org/"), IGNORE_ALL)));

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

    private void given(JwtFactory.Builder builder) {
        jwt = builder.build();
    }

    private JwtFactory.Builder aJwt() {
        return jwtFactory.aJwt();
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