/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 - 2022 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.oidc;

import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Objects.requireNonNull;
import static org.dcache.gplazma.oidc.MockHttpClientBuilder.aClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.common.collect.ImmutableList;
import java.net.URI;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.junit.Before;
import org.junit.Test;

public class IdentityProviderTests {

    private static final Profile IGNORE_ALL = (i, c) -> new ProfileResult(Collections.emptySet());
    private static final List<String> NO_SUPPRESSION = List.of();

    private IdentityProvider identityProvider;
    private HttpClient client;

    @Before
    public void setup() {
        client = null;
        identityProvider = null;
    }

    @Test(expected = NullPointerException.class)
    public void shouldFailWithNullName() throws Exception {
        IdentityProvider ignored = new IdentityProvider(null, URI.create("http://example.org/"),
              IGNORE_ALL, aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailWithEmptyName() throws Exception {
        IdentityProvider ignored = new IdentityProvider("", URI.create("http://example.org/"),
              IGNORE_ALL, aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION);
    }

    @Test(expected = NullPointerException.class)
    public void shouldFailWithNullUri() throws Exception {
        IdentityProvider ignored = new IdentityProvider("null-provider", null, IGNORE_ALL,
              aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION);
    }

    @Test(expected = NullPointerException.class)
    public void shouldFailWithNullProfile() throws Exception {
        IdentityProvider ignored = new IdentityProvider("null-profile",
              URI.create("http://example.org/"),
              null, aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION);
    }

    @Test
    public void shouldEqualReflectively() throws Exception {
        IdentityProvider google = new IdentityProvider("GOOGLE",
              URI.create("https://accounts.google.com/"),
              IGNORE_ALL, aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION);

        assertTrue(google.equals(google));
    }

    @Test
    public void shouldEqualAnotherWithSameNameAndUrl() throws Exception {
        IdentityProvider google1 = new IdentityProvider("GOOGLE",
              URI.create("https://accounts.google.com/"),
              IGNORE_ALL, aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION);
        IdentityProvider google2 = new IdentityProvider("GOOGLE",
              URI.create("https://accounts.google.com/"),
              IGNORE_ALL, aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION);

        assertTrue(google1.hashCode() == google2.hashCode());
        assertTrue(google1.equals(google2));
    }

    @Test
    public void shouldNotEqualAnotherWithDifferentName() throws Exception {
        IdentityProvider google1 = new IdentityProvider("GOOGLE-1",
              URI.create("https://accounts.google.com/"),
              IGNORE_ALL, aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION);
        IdentityProvider google2 = new IdentityProvider("GOOGLE-2",
              URI.create("https://accounts.google.com/"),
              IGNORE_ALL, aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION);

        assertFalse(google1.equals(google2));
    }

    @Test
    public void shouldNotEqualAnotherWithDifferentUrl() throws Exception {
        IdentityProvider google = new IdentityProvider("MYIP",
              URI.create("https://accounts.google.com/"),
              IGNORE_ALL, aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION);
        IdentityProvider keycloak = new IdentityProvider("MYIP",
              URI.create("https://keycloak.desy.de/"),
              IGNORE_ALL, aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION);

        assertFalse(google.equals(keycloak));
    }

    @Test
    public void shouldNotEqualAnotherType() throws Exception {
        IdentityProvider google = new IdentityProvider("MYIP",
              URI.create("https://accounts.google.com/"),
              IGNORE_ALL, aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION);

        assertFalse(google.equals("MYIP"));
        assertFalse(google.equals("https://accounts.google.com/"));
        assertFalse(google.equals(URI.create("https://accounts.google.com/")));
    }

    @Test
    public void shouldReturnStringWithNameAndUrlWhenToStringCalled() throws Exception {
        IdentityProvider google = new IdentityProvider("GOOGLE",
              URI.create("https://accounts.google.com/"),
              IGNORE_ALL, aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION);

        assertThat(google.toString(), containsString("GOOGLE"));
        assertThat(google.toString(), containsString("https://accounts.google.com/"));
    }

    @Test
    public void shouldParseProviderWithTrailingSlash() throws Exception {
        IdentityProvider google = new IdentityProvider("GOOGLE",
              URI.create("https://accounts.google.com/"),
              IGNORE_ALL, aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION);

        assertThat(google.getName(), is(equalTo("GOOGLE")));
        assertThat(google.getIssuerEndpoint().toString(),
              is(equalTo("https://accounts.google.com/")));
        assertThat(google.getConfigurationEndpoint().toString(),
              is(equalTo("https://accounts.google.com/.well-known/openid-configuration")));
        assertThat(google.getProfile(), is(sameInstance(IGNORE_ALL)));
    }

    @Test
    public void shouldParseProviderWithoutTrailingSlash() throws Exception {
        IdentityProvider google = new IdentityProvider("GOOGLE",
              URI.create("https://accounts.google.com"),
              IGNORE_ALL, aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION);

        assertThat(google.getName(), is(equalTo("GOOGLE")));
        assertThat(google.getIssuerEndpoint().toString(),
              is(equalTo("https://accounts.google.com")));
        assertThat(google.getConfigurationEndpoint().toString(),
              is(equalTo("https://accounts.google.com/.well-known/openid-configuration")));
        assertThat(google.getProfile(), is(sameInstance(IGNORE_ALL)));
    }

    @Test
    public void shouldParseProviderWithPathWithoutTrailingSlash() throws Exception {
        IdentityProvider unity = new IdentityProvider("UNITY",
              URI.create("https://unity.helmholtz-data-federation.de/oauth2"), IGNORE_ALL,
              aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION);

        assertThat(unity.getName(), is(equalTo("UNITY")));
        assertThat(unity.getIssuerEndpoint().toString(),
              is(equalTo("https://unity.helmholtz-data-federation.de/oauth2")));
        assertThat(unity.getConfigurationEndpoint().toString(), is(equalTo(
              "https://unity.helmholtz-data-federation.de/oauth2/.well-known/openid-configuration")));
        assertThat(unity.getProfile(), is(sameInstance(IGNORE_ALL)));
    }

    @Test
    public void shouldParseProviderWithPathWithTrailingSlash() throws Exception {
        IdentityProvider unity = new IdentityProvider("UNITY",
              URI.create("https://unity.helmholtz-data-federation.de/oauth2/"), IGNORE_ALL,
              aClient().build(), Duration.ofSeconds(2), NO_SUPPRESSION);

        assertThat(unity.getName(), is(equalTo("UNITY")));
        assertThat(unity.getIssuerEndpoint().toString(),
              is(equalTo("https://unity.helmholtz-data-federation.de/oauth2/")));
        assertThat(unity.getConfigurationEndpoint().toString(), is(equalTo(
              "https://unity.helmholtz-data-federation.de/oauth2/.well-known/openid-configuration")));
        assertThat(unity.getProfile(), is(sameInstance(IGNORE_ALL)));
    }

    @Test
    public void shouldSupportDiscoveryDocument() throws Exception {
        given(anIdentityProvider("UNITY").withIssuer("https://login.helmholtz.de/oauth2/")
              .withClient(aClient()
                    .onGet("https://login.helmholtz.de/oauth2/.well-known/openid-configuration")
                    .responds().withEntity(
                          "{\"authorization_endpoint\":\"https:\\/\\/login.helmholtz.de\\/oauth2-as\\/oauth2-authz\","
                                + "\"token_endpoint\":\"https:\\/\\/login.helmholtz.de\\/oauth2\\/token\","
                                + "\"introspection_endpoint\":\"https:\\/\\/login.helmholtz.de\\/oauth2\\/introspect\","
                                + "\"revocation_endpoint\":\"https:\\/\\/login.helmholtz.de\\/oauth2\\/revoke\","
                                + "\"issuer\":\"https:\\/\\/login.helmholtz.de\\/oauth2\","
                                + "\"jwks_uri\":\"https:\\/\\/login.helmholtz.de\\/oauth2\\/jwk\","
                                + "\"scopes_supported\":[\"credentials\",\"openid\",\"profile\",\"eduperson_scoped_affiliation\","
                                + "\"eduperson_unique_id\",\"sn\",\"eduperson_assurance\",\"display_name\","
                                + "\"email\",\"eduperson_entitlement\",\"eduperson_principal_name\",\"single-logout\"],"
                                + "\"response_types_supported\":[\"code\",\"token\",\"id_token\",\"code id_token\","
                                + "\"id_token token\",\"code token\",\"code id_token token\"],"
                                + "\"response_modes_supported\":[\"query\",\"fragment\"],"
                                + "\"grant_types_supported\":[\"authorization_code\",\"implicit\"],"
                                + "\"code_challenge_methods_supported\":[\"plain\",\"S256\"],"
                                + "\"request_uri_parameter_supported\":true,\"subject_types_supported\":[\"public\"],"
                                + "\"userinfo_endpoint\":\"https:\\/\\/login.helmholtz.de\\/oauth2\\/userinfo\","
                                + "\"id_token_signing_alg_values_supported\":[\"RS256\",\"ES256\"]}")));

        var discovery = identityProvider.discoveryDocument();

        verify(client).execute(any(HttpGet.class));

        assertThat(discovery.getNodeType(), is(equalTo(JsonNodeType.OBJECT)));
        var userinfo = discovery.get("userinfo_endpoint");
        assertThat(userinfo, is(not(nullValue())));
        assertThat(userinfo.getNodeType(), is(equalTo(JsonNodeType.STRING)));
        assertThat(userinfo.asText(), is(equalTo("https://login.helmholtz.de/oauth2/userinfo")));
        var jwks = discovery.get("jwks_uri");
        assertThat(jwks, is(not(nullValue())));
        assertThat(jwks.getNodeType(), is(equalTo(JsonNodeType.STRING)));
        assertThat(jwks.asText(), is(equalTo("https://login.helmholtz.de/oauth2/jwk")));
    }

    @Test
    public void shouldNotContainSuppressionByDefault() throws Exception {
        given(anIdentityProvider("OP")
            .withIssuer("https://oidc.example.org/")
            .withClient(aClient()));

        assertThat(identityProvider.isSuppressed("example-1"), is(equalTo(false)));
        assertThat(identityProvider.isSuppressed("example-2"), is(equalTo(false)));
    }

    @Test
    public void shouldContainSingleSuppressionKeyword() throws Exception {
    given(
        anIdentityProvider("OP")
            .withIssuer("https://oidc.example.org/")
            .withClient(aClient())
            .withSuppress("example-1"));

        assertThat(identityProvider.isSuppressed("example-1"), is(equalTo(true)));
        assertThat(identityProvider.isSuppressed("example-2"), is(equalTo(false)));
    }

    @Test
    public void shouldContainTwoSuppressionKeywords() throws Exception {
    given(
        anIdentityProvider("OP")
            .withIssuer("https://oidc.example.org/")
            .withClient(aClient())
            .withSuppress("example-1", "example-2"));

        assertThat(identityProvider.isSuppressed("example-1"), is(equalTo(true)));
        assertThat(identityProvider.isSuppressed("example-2"), is(equalTo(true)));
    }

    @Test
    public void shouldReturnCachedDocumentOnSuccess() throws Exception {
        given(anIdentityProvider("UNITY")
              .withIssuer("https://op.example.org/")
              .withClient(aClient().onGet("https://op.example.org/.well-known/openid-configuration")
                    .responds().withEntity(
                          "{\"userinfo_endpoint\": \"https:\\/\\/op.example.org\\/userinfo\"}"))
              .withWarmedCache());

        var discovery = identityProvider.discoveryDocument();

        verify(client).execute(any(HttpGet.class));

        assertThat(discovery.getNodeType(), is(equalTo(JsonNodeType.OBJECT)));
        var userinfo = discovery.get("userinfo_endpoint");
        assertThat(userinfo, is(not(nullValue())));
        assertThat(userinfo.getNodeType(), is(equalTo(JsonNodeType.STRING)));
        assertThat(userinfo.asText(), is(equalTo("https://op.example.org/userinfo")));
    }

    @Test
    public void shouldReturnMissingDocumentOnError() throws Exception {
        given(anIdentityProvider("UNITY").withIssuer("https://op.example.org/").withClient(aClient()
              .onGet("https://op.example.org/.well-known/openid-configuration")
              .responds().withStatusCode(500).withoutEntity()));

        var discovery = identityProvider.discoveryDocument();

        verify(client).execute(any(HttpGet.class));

        assertThat(discovery.getNodeType(), is(equalTo(JsonNodeType.MISSING)));
    }

    @Test
    public void shouldUsedCacheFailedResponseOnError() throws Exception {
        given(anIdentityProvider("UNITY")
              .withIssuer("https://op.example.org/")
              .withClient(aClient().onGet("https://op.example.org/.well-known/openid-configuration")
                    .responds().withStatusCode(500).withoutEntity())
              .withWarmedCache());

        var discovery = identityProvider.discoveryDocument();

        verify(client).execute(any(HttpGet.class));

        assertThat(discovery.getNodeType(), is(equalTo(JsonNodeType.MISSING)));
    }

    @Test
    public void shouldObtainUpdatedInformationOnceCacheExpires() throws Exception {
        given(anIdentityProvider("UNITY")
              .withIssuer("https://op.example.org/")
              .withClient(aClient()
                    .onGet("https://op.example.org/.well-known/openid-configuration").responds()
                    .withEntity(
                          "{\"userinfo_endpoint\": \"https:\\/\\/op.example.org\\/old-userinfo\"}")
                    .onGet("https://op.example.org/.well-known/openid-configuration").responds()
                    .withEntity(
                          "{\"userinfo_endpoint\": \"https:\\/\\/op.example.org\\/new-userinfo\"}"))
              .withCacheDuration(0, SECONDS)
              .withWarmedCache());

        Thread.sleep(100); // Assuming better than 100 ms granularity in clock.

        var discovery = identityProvider.discoveryDocument();

        verify(client, times(2)).execute(any(HttpGet.class));

        assertThat(discovery.getNodeType(), is(equalTo(JsonNodeType.OBJECT)));
        var userinfo = discovery.get("userinfo_endpoint");
        assertThat(userinfo, is(not(nullValue())));
        assertThat(userinfo.getNodeType(), is(equalTo(JsonNodeType.STRING)));
        assertThat(userinfo.asText(), is(equalTo("https://op.example.org/new-userinfo")));
    }

    private void given(IdentityProviderBuilder builder) {
        builder.build();
    }

    private IdentityProviderBuilder anIdentityProvider(String name) {
        return new IdentityProviderBuilder(name);
    }

    /**
     * Fluent class to build a (real) IdentityProvider object.
     */
    private class IdentityProviderBuilder {

        private final String name;
        private final ImmutableList.Builder<String> suppression = ImmutableList.<String>builder();
        private URI issuer;
        private boolean warmUpCache;
        private Duration cacheDuration = Duration.ofHours(1);

        IdentityProviderBuilder(String name) {
            this.name = name;
        }

        IdentityProviderBuilder withIssuer(String url) {
            issuer = URI.create(url);
            return this;
        }

        IdentityProviderBuilder withCacheDuration(int delay, ChronoUnit units) {
            cacheDuration = Duration.of(delay, units);
            return this;
        }

        IdentityProviderBuilder withClient(MockHttpClientBuilder builder) {
            client = builder.build();
            return this;
        }

        IdentityProviderBuilder withWarmedCache() {
            warmUpCache = true;
            return this;
        }

        IdentityProviderBuilder withSuppress(String... keywords) {
            suppression.addAll(Arrays.asList(keywords));
            return this;
        }

        void build() {
            requireNonNull(issuer);
            requireNonNull(client);
            identityProvider = new IdentityProvider(name, issuer, IGNORE_ALL, client,
                  cacheDuration, suppression.build());
            if (warmUpCache) {
                identityProvider.discoveryDocument();
            }
        }
    }
}
