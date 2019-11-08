/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.util;

import com.google.common.base.Splitter;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.Signature;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A JsonWebToken is a bearer token with three parts: a header, a payload and
 * a signature.  This class provides access to well-known header claims, the
 * ability to verify the signature, and a flexible mechanism to extract
 * information from the payload.
 */
public class JsonWebToken
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonWebToken.class);

    private final ObjectMapper mapper = new ObjectMapper();

    // Header values
    private final String typ;
    private final String alg;
    private final String kid;

    private final JsonNode payload;

    private final byte[] unsignedToken;
    private final byte[] signature;

    public static boolean isCompatibleFormat(String token)
    {
        List<String> elements = Splitter.on('.').limit(3).splitToList(token);
        return elements.size() == 3 && elements.stream().allMatch(JsonWebToken::isBase64Encoded);
    }

    private static boolean isBase64Encoded(String data)
    {
        try {
            Base64.getUrlDecoder().decode(data);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private JsonNode decodeToJson(String encoded) throws IOException
    {
        String data = new String(decodeToBytes(encoded), StandardCharsets.UTF_8);
        return mapper.readValue(data, JsonNode.class);
    }

    private static byte[] decodeToBytes(String data)
    {
        return Base64.getUrlDecoder().decode(data);
    }

    public JsonWebToken(String token) throws IOException
    {
        int lastDot = token.lastIndexOf('.');
        checkArgument(lastDot > 0, "Missing '.' in JWT");
        unsignedToken = token.substring(0, lastDot).getBytes(StandardCharsets.US_ASCII);

        List<String> elements = Splitter.on('.').limit(3).splitToList(token);
        checkArgument(elements.size() == 3, "Wrong number of '.' in token");

        JsonNode header = decodeToJson(elements.get(0));
        alg = header.get("alg").getTextValue();
        typ = getOptionalString(header, "typ");
        kid = getOptionalString(header, "kid");

        payload = decodeToJson(elements.get(1));
        signature = decodeToBytes(elements.get(2));
    }

    private String getOptionalString(JsonNode object, String key)
    {
        JsonNode node = object.get(key);
        return node == null ? null : node.getTextValue();
    }

    @Nullable
    public String getKeyIdentifier()
    {
        return kid;
    }

    public boolean isSignedBy(PublicKey key)
    {
        try {
            Signature signature = getSignature();
            signature.initVerify(key);
            signature.update(unsignedToken);
            return signature.verify(this.signature);
        } catch (GeneralSecurityException e) {
            LOGGER.warn("Problem verifying signature: {}", e.toString());
            return false;
        }
    }

    private Signature getSignature() throws GeneralSecurityException
    {
        switch (alg) {
        case "RS256":
            return Signature.getInstance("SHA256withRSA", "BC");
        default:
            throw new NoSuchAlgorithmException("Unknown JWT alg " + alg);
        }
    }

    public Optional<Instant> getPayloadInstant(String key)
    {
        return Optional.ofNullable(payload.get(key))
                .filter(JsonNode::isIntegralNumber)
                .map(JsonNode::asLong)
                .map(Instant::ofEpochSecond);
    }

    public Optional<String> getPayloadString(String key)
    {
        return Optional.ofNullable(payload.get(key))
                .filter(JsonNode::isTextual)
                .map(JsonNode::getTextValue);
    }

    /**
     * A node is either absent, a JSON String, or a JSON Array with JSON String
     * elements. If absent then an empty List is returned.  If present and a
     * JSON String then a List containing only that value is returned.  If
     * present and a JSON Array then a List containing all textual elements is
     * returned; all non-textual elements are ignored.  If the value is not a
     * JSON String or a JSON array then an empty List is returned.
     * @param key the ID of the element to return
     * @return a List of Strings, which may be empty.
     */
    public List<String> getPayloadStringOrArray(String key)
    {
        return Optional.ofNullable(payload.get(key))
                .filter(n -> n.isArray() || n.isTextual())
                .map(JsonWebToken::toStringList)
                .orElse(Collections.emptyList());
    }

    private static List<String> toStringList(JsonNode node)
    {
        if (node.isArray()) {
            return StreamSupport.stream(node.spliterator(), false)
                    .filter(JsonNode::isTextual) // Non text array elements are simply ignored
                    .map(JsonNode::getTextValue)
                    .collect(Collectors.toList());
        } else  if (node.isTextual()) {
            return Collections.singletonList(node.getTextValue());
        } else {
            throw new RuntimeException("Unable to convert node " + node
                    + " to List<String>");
        }
    }
}
