/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 - 2020 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.scitoken;

import static com.google.common.base.Preconditions.checkArgument;
import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableSet;
import diskCacheV111.util.FsPath;
import java.math.BigInteger;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.Principal;
import java.security.PublicKey;
import java.security.spec.KeySpec;
import java.security.spec.RSAPublicKeySpec;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.http.client.HttpClient;
import org.dcache.auth.OAuthProviderPrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.util.JsonWebToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a SciToken issuer, a service with associated metadata.  This typically represents a
 * VO.
 */
public class Issuer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Issuer.class);

    private final String id;
    private final String endpoint;
    private final Set<Principal> identity;
    private final FsPath prefix;
    private final Queue<String> previousJtis;

    private final JsonNode configuration;
    private final JsonNode jwks;


    private Supplier<Map<String, PublicKey>> keys = MemoizeMapWithExpiry.memorize(this::parseJwks)
          .whenEmptyFor(Duration.ofMinutes(1))
          .whenNonEmptyFor(Duration.ofMinutes(10))
          .build();

    public Issuer(HttpClient client, String id, String endpoint, FsPath prefix,
          Set<Principal> identity, int tokenHistory) {
        this.id = id;
        this.endpoint = checkURI(endpoint);
        this.prefix = prefix;

        StringBuilder sb = new StringBuilder(endpoint.length() + 43);
        sb.append(endpoint);
        if (!endpoint.endsWith("/")) {
            sb.append('/');
        }
        sb.append(".well-known/openid-configuration");
        String configEndpoint = sb.toString();

        this.identity = ImmutableSet.<Principal>builder()
              .addAll(identity)
              .add(new OAuthProviderPrincipal(id))
              .build();

        this.configuration = new HttpJsonNode(client, configEndpoint,
              Duration.ofHours(1), Duration.ofSeconds(10));
        this.jwks = new HttpJsonNode(client, this::parseConfigurationForJwksUri,
              Duration.ofSeconds(1), Duration.ofSeconds(1));

        previousJtis = tokenHistory > 0 ? EvictingQueue.create(tokenHistory) : null;
    }

    private String checkURI(String endpoint) {
        URI issuer = URI.create(endpoint);
        checkArgument(issuer.getScheme() != null, "Bad issuer \"%s\": missing schema", endpoint);
        checkArgument(issuer.getScheme().equals("http") || issuer.getScheme().equals("https"),
              "Bad issuer \"%s\": schema must be either 'http' or 'https'", endpoint);
        checkArgument(issuer.getHost() != null, "Bad issuer \"%s\": missing host");
        return endpoint;
    }

    public Set<Principal> getPrincipals() {
        return identity;
    }

    public String getId() {
        return id;
    }

    public FsPath getPrefix() {
        return prefix;
    }

    public String getEndpoint() {
        return endpoint;
    }

    private Optional<String> parseConfigurationForJwksUri() {
        JsonNode jwksNode = configuration.get("jwks_uri");

        if (jwksNode == null) {
            LOGGER.warn("configuration does not have jwks_uri");
            return Optional.empty();
        }

        if (!jwksNode.isTextual()) {
            LOGGER.warn("configuration has non-textual jwks_uri");
            return Optional.empty();
        }

        String url = jwksNode.asText();

        if (url.isEmpty()) {
            LOGGER.warn("configuration has empty jwks_uri");
            return Optional.empty();
        }

        return Optional.of(url);
    }

    private Map<String, PublicKey> parseJwks() {
        JsonNode keys = jwks.get("keys");
        if (keys == null) {
            LOGGER.warn("missing keys");
            return Collections.emptyMap();
        }
        if (!keys.isArray()) {
            LOGGER.warn("keys not an array");
            return Collections.emptyMap();
        }

        Map<String, PublicKey> publicKeys = new HashMap<>();
        for (JsonNode key : keys) {
            try {
                String kid = getString(key, "kid");
                publicKeys.put(kid, buildPublicKey(key));
            } catch (BadKeyDescriptionException e) {
                LOGGER.warn("Bad public key: {}", e.getMessage());
            }
        }
        return publicKeys;
    }

    private PublicKey buildPublicKey(JsonNode details) throws BadKeyDescriptionException {
        String kty = getString(details, "kty");
        switch (kty) {
            case "RSA":
                return buildRSAPublicKey(details);
            default:
                throw new BadKeyDescriptionException("Unknown key type " + kty);
        }
    }

    private String getString(JsonNode details, String key) throws BadKeyDescriptionException {
        JsonNode target = details.get(key);
        if (target == null) {
            throw new BadKeyDescriptionException("Missing attribute " + key);
        }
        if (!target.isTextual()) {
            throw new BadKeyDescriptionException("Attribute not textual " + key);
        }
        return target.asText();
    }

    private PublicKey buildRSAPublicKey(JsonNode details) throws BadKeyDescriptionException {
        try {
            byte[] e = Base64.getUrlDecoder().decode(getString(details, "e"));
            byte[] n = Base64.getUrlDecoder().decode(getString(details, "n"));
            KeySpec keySpec = new RSAPublicKeySpec(new BigInteger(1, n), new BigInteger(1, e));
            return KeyFactory.getInstance("RSA", "BC").generatePublic(keySpec);
        } catch (GeneralSecurityException e) {
            throw new BadKeyDescriptionException("Unable to build RSA public key: " + e.toString());
        }
    }

    public void checkIssued(JsonWebToken token) throws AuthenticationException {
        Map<String, PublicKey> keyMap = keys.get();

        String kid = token.getKeyIdentifier();
        if (kid != null) {
            PublicKey publicKey = keyMap.get(kid);
            checkAuthentication(publicKey != null, "Unknown kid \"" + kid + "\"");
            checkAuthentication(token.isSignedBy(publicKey), "Invalid signature");
        } else {
            checkAuthentication(keyMap.values().stream().anyMatch(token::isSignedBy),
                  "Invalid signature");
        }

        if (previousJtis != null) {
            Optional<String> jtiClaim = token.getPayloadString("jti");
            if (jtiClaim.isPresent()) {
                String jti = jtiClaim.get();
                boolean isReplayAttack = previousJtis.contains(jti);
                previousJtis.add(jti);
                checkAuthentication(!isReplayAttack, "token reuse");
            }
        }
    }
}
