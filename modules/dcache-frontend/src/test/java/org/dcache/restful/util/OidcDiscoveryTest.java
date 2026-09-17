/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2026 Deutsches Elektronen-Synchrotron
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
package org.dcache.restful.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OidcDiscoveryTest {

    private HttpServer server;
    private URI issuer;
    private OidcDiscovery discovery;
    private final AtomicInteger discoveryHits = new AtomicInteger();
    private String discoveryBody =
          "{\"authorization_endpoint\":\"https://op.example/authorize\","
                + "\"token_endpoint\":\"https://op.example/token\"}";

    @Before
    public void setup() throws Exception {
        discoveryHits.set(0);
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/.well-known/openid-configuration", exchange -> {
            discoveryHits.incrementAndGet();
            byte[] body = discoveryBody.getBytes(UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.createContext("/oauth2/.well-known/openid-configuration", exchange -> {
            discoveryHits.incrementAndGet();
            byte[] body = discoveryBody.getBytes(UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.start();
        issuer = URI.create("http://localhost:" + server.getAddress().getPort());
        discovery = new OidcDiscovery();
    }

    @After
    public void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    public void shouldBuildDiscoveryUrlWithoutTrailingSlash() {
        assertThat(OidcDiscovery.configurationEndpoint(URI.create("https://accounts.google.com"))
                    .toString(),
              is(equalTo("https://accounts.google.com/.well-known/openid-configuration")));
    }

    @Test
    public void shouldBuildDiscoveryUrlWithTrailingSlash() {
        assertThat(OidcDiscovery.configurationEndpoint(URI.create("https://accounts.google.com/"))
                    .toString(),
              is(equalTo("https://accounts.google.com/.well-known/openid-configuration")));
    }

    @Test
    public void shouldBuildDiscoveryUrlWithPath() {
        assertThat(OidcDiscovery.configurationEndpoint(
                    URI.create("https://unity.example.org/oauth2")).toString(),
              is(equalTo("https://unity.example.org/oauth2/.well-known/openid-configuration")));
    }

    @Test
    public void shouldDiscoverAuthorizationAndTokenEndpoints() throws Exception {
        assertThat(discovery.authorizationEndpoint(issuer),
              is(equalTo("https://op.example/authorize")));
        assertThat(discovery.tokenEndpoint(issuer), is(equalTo("https://op.example/token")));
        assertThat(discoveryHits.get(), is(equalTo(1)));
    }

    @Test
    public void shouldDiscoverIssuerWithPath() throws Exception {
        URI pathIssuer = URI.create(issuer.toString() + "/oauth2");
        assertThat(discovery.authorizationEndpoint(pathIssuer),
              is(equalTo("https://op.example/authorize")));
    }

    @Test
    public void shouldPreferConfiguredTokenUrl() throws Exception {
        assertThat(discovery.resolveTokenEndpoint(issuer.toString(), "https://explicit.example/token"),
              is(equalTo("https://explicit.example/token")));
        assertThat(discoveryHits.get(), is(equalTo(0)));
    }

    @Test
    public void shouldDiscoverTokenUrlWhenNotConfigured() throws Exception {
        assertThat(discovery.resolveTokenEndpoint(issuer.toString(), "  "),
              is(equalTo("https://op.example/token")));
    }

    @Test(expected = IOException.class)
    public void shouldFailWhenNeitherTokenUrlNorIssuerSet() throws Exception {
        discovery.resolveTokenEndpoint("", null);
    }

    @Test
    public void shouldFillEmptyAuthzListFromIssuer() throws Exception {
        OidcDiscoveryConfigDecorator decorator = new OidcDiscoveryConfigDecorator();
        decorator.setOidcDiscovery(discovery);
        decorator.setIssuer(issuer.toString());

        Map<String, String> enriched = decorator.enrich(
              Map.of("dcache-view.oidc-provider-name-list", "AS"));

        assertThat(enriched.get(OidcDiscoveryConfigDecorator.AUTHZ_ENDPOINT_LIST),
              is(equalTo("https://op.example/authorize")));
    }

    @Test
    public void shouldKeepExplicitAuthzList() throws Exception {
        OidcDiscoveryConfigDecorator decorator = new OidcDiscoveryConfigDecorator();
        decorator.setOidcDiscovery(discovery);
        decorator.setIssuer(issuer.toString());

        Map<String, String> enriched = decorator.enrich(
              Map.of(OidcDiscoveryConfigDecorator.AUTHZ_ENDPOINT_LIST,
                    "https://explicit.example/authorize"));

        assertThat(enriched.get(OidcDiscoveryConfigDecorator.AUTHZ_ENDPOINT_LIST),
              is(equalTo("https://explicit.example/authorize")));
        assertThat(discoveryHits.get(), is(equalTo(0)));
    }
}
