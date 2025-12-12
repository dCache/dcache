/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 - 2024 Deutsches Elektronen-Synchrotron
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
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECPoint;
import java.security.spec.ECParameterSpec;
import java.security.spec.ECPublicKeySpec;
import java.security.spec.ECGenParameterSpec;
import java.security.AlgorithmParameters;
import java.util.Base64;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.http.client.HttpClient;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.oidc.helpers.ReasonBearingMissingNode;
import org.dcache.gplazma.oidc.HttpClientUtils;
import org.dcache.gplazma.oidc.IdentityProvider;
import org.dcache.gplazma.util.JsonWebToken;
import org.dcache.util.Result;
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

    // Recommendation for six hours comes from this document:
    //     https://doi.org/10.5281/zenodo.3460258
    // REVISIT: the cache duration depends only whether the JWKS document may
    // be fetched, is a JSON object with "keys" pointing to an array.  If these
    // steps are successful and one (or more) of the array items are malformed
    // then we will cache the result for <tt>withSuccessFor</tt> duration.
    // This behaviour may be suboptimal.
    private final Supplier<Result<Map<String, Result<PublicKey,String>>,String>> keys =
            MemoizeResultWithExpiry.memorize(this::readJwksDocument)
                    .whenFailureFor(Duration.ofMinutes(1))
                    .whenSuccessFor(Duration.ofHours(6))
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

    private Result<URI,String> jwksEndpoint() {
        JsonNode configuration = provider.discoveryDocument();
        if (configuration.getNodeType() == JsonNodeType.MISSING) {
            String reason = configuration instanceof ReasonBearingMissingNode
                    ? ((ReasonBearingMissingNode) configuration).getReason()
                    : "unknown problem";
            return Result.failure(reason);
        }

        JsonNode jwksNode = configuration.get("jwks_uri");

        if (jwksNode == null) {
            LOGGER.warn("configuration does not have jwks_uri");
            return Result.failure("discovery document missing jwks_uri");
        }

        if (!jwksNode.isTextual()) {
            LOGGER.warn("configuration has non-textual jwks_uri");
            return Result.failure("discovery document has non-textual jwks_uri");
        }

        String url = jwksNode.asText();

        if (url.isEmpty()) {
            LOGGER.warn("configuration has empty jwks_uri");
            return Result.failure("discovery document has empty jwks_uri");
        }

        try {
            return Result.success(new URI(url));
        } catch (URISyntaxException e) {
            LOGGER.warn("Bad jwks_uri URI \"{}\": {}", url, e.toString());
            return Result.failure("Bad jwks_uri URI " + url + ": " + e.toString());
        }
    }

    private Result<JsonNode,String> fetchJson(URI uri) {
        try {
            JsonNode document = HttpClientUtils.readJson(client, uri);
            return Result.success(document);
        } catch (IOException e) {
            LOGGER.warn("Failed to fetch {}: {}", uri, e.toString());
            return Result.failure("Failed to fetch " + uri + ": " + e.toString());
        }
    }

    private Result<JsonNode,String> extractElement(JsonNode object, String key) {
        if (object.getNodeType() != JsonNodeType.OBJECT) {
            LOGGER.warn("Json node has wrong type: {} != OBJECT", object.getNodeType());
            return Result.failure("Json node has wrong type: " + object.getNodeType() + " != OBJECT");
        }

        var element = object.get(key);

        if (element == null) {
            LOGGER.warn("JSON object is missing key \"{}\"", key);
            return Result.failure("JSON object is missing key \"" + key + "\"");
        }

        return Result.success(element);
    }

    private Result<JsonNode,String> asArray(JsonNode node) {
        if (node.getNodeType() != JsonNodeType.ARRAY) {
            LOGGER.warn("Json node has wrong type: {} != ARRAY", node.getNodeType());
            return Result.failure("Json node has wrong type: " + node.getNodeType() + " != ARRAY");
        }
        return Result.success(node);
    }

    private Result<Map<String, Result<PublicKey,String>>,String> readJwksDocument() {
        return jwksEndpoint()
                .flatMap(this::fetchJson)
                .flatMap(j -> this.extractElement(j, "keys"))
                .flatMap(this::asArray)
                .map(this::parseJwksKeys);
    }

    private Map<String, Result<PublicKey,String>> parseJwksKeys(JsonNode keys) {
        Map<String, Result<PublicKey,String>> publicKeys = new HashMap<>();
        for (JsonNode key : keys) {
            if (!key.isObject()) {
                LOGGER.debug("Ignoring JWKS \"keys\" array item that is not a JSON object.");
                continue;
            }
            String kid;
            try {
                kid = getOptionalString(key, "kid").orElseGet(() -> UUID.randomUUID().toString());
            } catch (BadKeyDescriptionException e) {
                LOGGER.warn("Issuer returned a JWKS document with a bad public"
                        + " key identifier (\"kid\").  Using a random"
                        + " identifier as a work-around, but this could still"
                        + " lead to tokens being reject.");
                kid = UUID.randomUUID().toString();
            }
            try {
                Result<PublicKey,String> r = Result.success(buildPublicKey(key));
                publicKeys.put(kid, r);
            } catch (BadKeyDescriptionException e) {
                Result<PublicKey,String> r = Result.failure(e.getMessage());
                publicKeys.put(kid, r);
                LOGGER.debug("Bad public key: {}", e.getMessage());
            }
        }
        return publicKeys;
    }

    private PublicKey buildPublicKey(JsonNode details) throws BadKeyDescriptionException {
        String kty = getString(details, "kty");
        switch (kty) {
            case "RSA":
                return buildRSAPublicKey(details);
            case "EC":
                return buildECPublicKey(details);
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

    private ECParameterSpec getECParameterSpec(String crv) throws GeneralSecurityException {
        String name;

        switch (crv) {
            case "P-256":
                name = "secp256r1";
                break;
            case "P-384":
                name = "secp384r1";
                break;
            case "P-521":
                name = "secp521r1";
                break;
            default:
                throw new GeneralSecurityException("Unsupported curve: " + crv);
        }
        AlgorithmParameters parameters = AlgorithmParameters.getInstance("EC");
        parameters.init(new ECGenParameterSpec(name));
        return parameters.getParameterSpec(ECParameterSpec.class);
    }   

    private PublicKey buildECPublicKey(JsonNode details) throws BadKeyDescriptionException {
        try {
            String crv = getString(details, "crv");
            byte[] x = Base64.getUrlDecoder().decode(getString(details, "x"));
            byte[] y = Base64.getUrlDecoder().decode(getString(details, "y"));
            ECParameterSpec params = getECParameterSpec(crv);
            ECPoint point = new ECPoint(new BigInteger(1, x), new BigInteger(1, y));
            ECPublicKeySpec pubSpec = new ECPublicKeySpec(point, params);
            return KeyFactory.getInstance("EC").generatePublic(pubSpec);
        } catch (GeneralSecurityException e) {
            throw new BadKeyDescriptionException("Unable to build EC public key: " + e.toString());
        }
    }

    public void checkIssued(JsonWebToken token) throws AuthenticationException {
        Map<String, Result<PublicKey,String>> keyMap = keys.get()
                .orElseThrow(msg -> new AuthenticationException(
                        "Problem fetching public keys: " + msg));

        String kid = token.getKeyIdentifier();
        if (kid != null) {
            var publicKeyResult = keyMap.get(kid);
            checkAuthentication(publicKeyResult != null, "issuer has no public"
                    + " key for this token (key id \"" + kid + "\")");
            PublicKey publicKey = publicKeyResult
                    .orElseThrow(msg -> new AuthenticationException("issuer has"
                            + " malformed public key for this token: " + msg));
            checkAuthentication(token.isSignedBy(publicKey), "token not signed by issuer");
        } else {
            if (!keyMap.values().stream()
                    .map(Result::getSuccess)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .anyMatch(token::isSignedBy)) {
                StringBuilder sb = new StringBuilder("issuer has no public key for this token");
                String failures = keyMap.entrySet().stream()
                        .filter(e -> e.getValue().isFailure())
                        .map(e -> e.getKey() + "[" + e.getValue().getFailure().get() + "]")
                        .collect(Collectors.joining(", "));
                if (!failures.isEmpty()) {
                    sb.append(": ").append(failures);
                }
                throw new AuthenticationException(sb.toString());
            }
        }

        if (previousJtis != null) {
            Optional<String> jtiClaim = token.getPayloadString("jti");
            if (jtiClaim.isPresent()) {
                String jti = jtiClaim.get();
                boolean isReplayAttack = previousJtis.contains(jti);
                previousJtis.add(jti);
                checkAuthentication(!isReplayAttack, "token is being reused; possible replay attack");
            }
        }
    }
}
