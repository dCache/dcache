/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 - 2022 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.oidc.jwt;

import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.common.collect.EvictingQueue;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
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
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.http.client.HttpClient;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.oidc.HttpClientUtils;
import org.dcache.gplazma.oidc.IdentityProvider;
import org.dcache.gplazma.util.JsonWebToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * Represents a JWT issuer.  This is an IdentityProvider along with their signing material.
 */
public class Issuer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Issuer.class);

    private final Queue<String> previousJtis;
    private final IdentityProvider provider;
    private final HttpClient client;
    private final boolean offlineSuppressed;

    private final Supplier<Map<String, PublicKey>> keys = MemoizeMapWithExpiry.memorize(this::readJwksDocument)
          .whenEmptyFor(Duration.ofMinutes(1))
          .whenNonEmptyFor(Duration.ofMinutes(10))
          .build();

    public Issuer(HttpClient client, IdentityProvider provider, int tokenHistory) {
        this.provider = requireNonNull(provider);
        this.client = requireNonNull(client);
        previousJtis = tokenHistory > 0 ? EvictingQueue.create(tokenHistory) : null;
        offlineSuppressed = provider.isSuppressed("offline");
        if (offlineSuppressed) {
            LOGGER.warn("Offline verification of JWT access tokens issued by OP {} has been "
                + "suppressed.  This may cause various problems, including dCache being slow to "
                + "process requests with such tokens and dCache generating high load for the OP.",
                provider.getName());
        }
    }

    public boolean isOfflineSuppressed() {
        return offlineSuppressed;
    }

    public IdentityProvider getIdentityProvider() {
        return provider;
    }

    public String getEndpoint() {
        return provider.getIssuerEndpoint().toASCIIString();
    }

    private Optional<URI> jwksEndpoint() {
        JsonNode configuration = provider.discoveryDocument();
        if (configuration.getNodeType() == JsonNodeType.MISSING) {
            return Optional.empty();
        }

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

        try {
            return Optional.of(new URI(url));
        } catch (URISyntaxException e) {
            LOGGER.warn("Bad jwks_uri URI \"{}\": {}", url, e.toString());
            return Optional.empty();
        }
    }

    private Optional<JsonNode> fetchJson(URI uri) {
        try {
            JsonNode document = HttpClientUtils.readJson(client, uri);
            return Optional.of(document);
        } catch (IOException e) {
            LOGGER.warn("Failed to fetch {}: {}", uri, e.toString());
            return Optional.empty();
        }
    }

    private Optional<JsonNode> extractElement(JsonNode object, String key) {
        if (object.getNodeType() != JsonNodeType.OBJECT) {
            LOGGER.warn("Json node has wrong type: {} != OBJECT", object.getNodeType());
            return Optional.empty();
        }

        var element = object.get(key);

        if (element == null) {
            LOGGER.warn("JSON object is missing key \"{}\"", key);
            return Optional.empty();
        }

        return Optional.of(element);
    }

    private Optional<JsonNode> asArray(JsonNode node) {
        if (node.getNodeType() != JsonNodeType.ARRAY) {
            LOGGER.warn("Json node has wrong type: {} != ARRAY", node.getNodeType());
            return Optional.empty();
        }
        return Optional.of(node);
    }

    private Map<String, PublicKey> readJwksDocument() {
        return jwksEndpoint()
                .flatMap(this::fetchJson)
                .flatMap(j -> this.extractElement(j, "keys"))
                .flatMap(this::asArray)
                .map(this::parseJwksKeys)
                .orElse(Collections.emptyMap());
    }

    private Map<String, PublicKey> parseJwksKeys(JsonNode keys) {
        Map<String, PublicKey> publicKeys = new HashMap<>();
        for (JsonNode key : keys) {
            try {
                String kid = getOptionalString(key, "kid").orElseGet(() -> UUID.randomUUID().toString());
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

    private Optional<String> getOptionalString(JsonNode details, String key) throws BadKeyDescriptionException {
        JsonNode value = details.get(key);
        if (value != null && !value.isTextual()) {
            throw new BadKeyDescriptionException("Attribute not textual " + key);
        }
        return Optional.ofNullable(value).map(JsonNode::asText);
    }

    private PublicKey buildRSAPublicKey(JsonNode details) throws BadKeyDescriptionException {
        try {
            byte[] e = Base64.getUrlDecoder().decode(getString(details, "e"));
            byte[] n = Base64.getUrlDecoder().decode(getString(details, "n"));
            KeySpec keySpec = new RSAPublicKeySpec(new BigInteger(1, n), new BigInteger(1, e));
            return KeyFactory.getInstance("RSA").generatePublic(keySpec);
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
