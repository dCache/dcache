/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 Deutsches Elektronen-Synchrotron
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.base.Splitter;
import static java.util.Objects.requireNonNull;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * An OpenID-Connect Identity Provider.  An identity provider is a service that the admin has chosen
 * to trust in authenticating users via the OpenID-Connect protocol.  This class holds the
 * configuration information about a provider.
 * <p>
 * Each OIDC identity provider is assigned a name by the admin, which is used typically when
 * referring to the service in log messages.
 */
public class IdentityProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(IdentityProvider.class);

    private static final Duration CACHE_DURATION_WHEN_UNSUCCESSFUL = Duration.ofSeconds(10);

    private final ObjectMapper mapper = new ObjectMapper();
    private final String name;
    private final URI issuer;
    private final URI configuration;
    private final Profile profile;
    private final HttpClient client;
    private final Duration cacheDurationWhenSuccessful;
    private final List<String> suppress;

    private Instant nextDiscoveryFetch = Instant.now();
    private JsonNode discoveryDocument = MissingNode.getInstance();

    public IdentityProvider(String name, URI endpoint, Profile profile, HttpClient client,
            Duration discoveryCacheDuration, List<String> suppress) {
        checkArgument(!name.isEmpty(), "Empty name not allowed");
        this.name = name;
        this.issuer = requireNonNull(endpoint);
        checkArgument(endpoint.isAbsolute(), "URL is not absolute");
        this.profile = requireNonNull(profile);
        this.client = requireNonNull(client);
        configuration = issuer.resolve(
              withTrailingSlash(issuer.getPath()) + ".well-known/openid-configuration");
        cacheDurationWhenSuccessful = requireNonNull(discoveryCacheDuration);
        this.suppress = requireNonNull(suppress);
    }

    private static String withTrailingSlash(String path) {
        return path.endsWith("/") ? path : (path + "/");
    }

    public String getName() {
        return name;
    }

    public URI getIssuerEndpoint() {
        return issuer;
    }

    public Profile getProfile() {
        return profile;
    }

    public boolean isSuppressed(String value) {
        return suppress.stream().anyMatch(v -> v.equals(value));
    }

    /**
     */
    @VisibleForTesting
    URI getConfigurationEndpoint() {
        return configuration;
    }

    /**
     * Return the JSON document that describes the OP.  This document is sometimes called the OIDC
     * discover endpoint.  This URL is defined in OpenID-Connect Discovery document in
     * <a href="https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderConfig">section
     * 4</a>.
     * If the document is obtained successfully then the result is cached for a configurable period.
     * If there was a problem then the document is cached for a hard-coded, shorter period.
     * <p>
     * @return the root node of the configuration document
     */
    public synchronized JsonNode discoveryDocument() {
        Instant now = Instant.now();
        if (now.isAfter(nextDiscoveryFetch)) {
            try {
                discoveryDocument = HttpClientUtils.readJson(client, configuration);
                nextDiscoveryFetch = now.plus(cacheDurationWhenSuccessful);
            } catch (IOException e) {
                LOGGER.warn("Failed to fetch discovery document for {}: {}", name, e.toString());
                discoveryDocument = MissingNode.getInstance();
                nextDiscoveryFetch = now.plus(CACHE_DURATION_WHEN_UNSUCCESSFUL);
            }
        }
        return discoveryDocument;
    }

    @Override
    public int hashCode() {
        return name.hashCode() ^ issuer.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (!(other instanceof IdentityProvider)) {
            return false;
        }

        IdentityProvider otherIP = (IdentityProvider) other;
        return name.equals(otherIP.name) && issuer.equals(otherIP.issuer);
    }

    @Override
    public String toString() {
        return name + "[" + issuer.toASCIIString() + "]";
    }
}
