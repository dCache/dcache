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

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.concurrent.ConcurrentHashMap;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fetches and caches an OpenID Provider's discovery document
 * ({@code /.well-known/openid-configuration}) as defined by
 * <a href="https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderConfig">OIDC
 * Discovery</a>.
 * <p>
 * Used by the frontend to obtain {@code authorization_endpoint} and {@code token_endpoint}
 * when those URLs are not configured explicitly.
 */
public class OidcDiscovery {

    private static final Logger LOGGER = LoggerFactory.getLogger(OidcDiscovery.class);

    private static final int CONNECT_TIMEOUT_MS = 10_000;
    private static final int READ_TIMEOUT_MS = 30_000;

    private final ConcurrentHashMap<URI, JSONObject> cache = new ConcurrentHashMap<>();

    /**
     * Build the discovery URL from an issuer, matching gPlazma's {@code IdentityProvider}
     * construction: {@code {issuer}/.well-known/openid-configuration}, including issuers
     * that have a path component.
     */
    public static URI configurationEndpoint(URI issuer) {
        String path = issuer.getPath();
        if (path == null) {
            path = "";
        }
        return issuer.resolve(withTrailingSlash(path) + ".well-known/openid-configuration");
    }

    private static String withTrailingSlash(String path) {
        return path.endsWith("/") ? path : (path + "/");
    }

    /**
     * Return {@code configuredTokenUrl} when it is non-blank; otherwise the
     * {@code token_endpoint} from the issuer's discovery document.
     */
    public String resolveTokenEndpoint(String issuer, String configuredTokenUrl)
          throws IOException {
        if (hasText(configuredTokenUrl)) {
            return configuredTokenUrl.trim();
        }
        if (!hasText(issuer)) {
            throw new IOException(
                  "frontend.authn.oidc.token-url is empty and frontend.authn.oidc.issuer is not set");
        }
        return tokenEndpoint(URI.create(issuer.trim()));
    }

    public String authorizationEndpoint(URI issuer) throws IOException {
        return requiredEndpoint(fetchDocument(issuer), "authorization_endpoint", issuer);
    }

    public String tokenEndpoint(URI issuer) throws IOException {
        return requiredEndpoint(fetchDocument(issuer), "token_endpoint", issuer);
    }

    public JSONObject fetchDocument(URI issuer) throws IOException {
        URI configuration = configurationEndpoint(issuer);
        JSONObject cached = cache.get(configuration);
        if (cached != null) {
            return cached;
        }
        JSONObject document = download(configuration);
        cache.put(configuration, document);
        LOGGER.info("Loaded OIDC discovery document for {}", issuer);
        return document;
    }

    private JSONObject download(URI configuration) throws IOException {
        URL url = configuration.toURL();
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
        conn.setReadTimeout(READ_TIMEOUT_MS);
        conn.setRequestProperty("Accept", "application/json");
        try {
            int status = conn.getResponseCode();
            if (status != HttpURLConnection.HTTP_OK) {
                throw new IOException("OIDC discovery failed for " + configuration
                      + ": HTTP " + status);
            }
            try (InputStream in = conn.getInputStream()) {
                String body = new String(in.readAllBytes(), UTF_8);
                return new JSONObject(body);
            }
        } finally {
            conn.disconnect();
        }
    }

    private static String requiredEndpoint(JSONObject document, String field, URI issuer)
          throws IOException {
        if (!document.has(field) || document.isNull(field)) {
            throw new IOException("OIDC discovery document for " + issuer + " has no " + field);
        }
        String value = document.getString(field);
        if (!hasText(value)) {
            throw new IOException("OIDC discovery document for " + issuer + " has empty " + field);
        }
        return value;
    }

    static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
