/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A class for creating JWT tokens.  To improve performance somewhat, the same public/private
 * key-pair is used for all JWTs created from the same JwtFactor instance.
 */
public class JwtFactory {
    private static final int RSA_KEY_SIZE = 2048; // a somewhat arbitrary choice.
    private static final Base64.Encoder BASE64_ENCODER = Base64.getUrlEncoder().withoutPadding();

    /**
     * Fluent class for building a JWT.
     */
    public class Builder {
        private final ObjectNode headerObject = mapper.createObjectNode();
        private final ObjectNode payloadObject = mapper.createObjectNode();
        private Optional<String> payload = Optional.empty();

        public Builder withHeaderClaim(String key, String value) {
            headerObject.put(key, value);
            return this;
        }

        public Builder withPayloadClaim(String key, String value) {
            checkState(payload.isEmpty());
            payloadObject.put(key, value);
            return this;
        }

        public Builder withPayloadClaim(String key, String... values) {
            checkState(payload.isEmpty());
            var node = payloadObject.putArray(key);
            Arrays.stream(values).forEach(node::add);
            return this;
        }

        public Builder withPayloadClaim(String key, Instant when) {
            checkState(payload.isEmpty());
            payloadObject.put(key, when.getEpochSecond());
            return this;
        }

        public Builder withPayload(String payload) {
            checkState(this.payload.isEmpty());
            this.payload = Optional.of(payload);
            return this;
        }

        public String build() {
            String headerText = headerObject.put("alg", "RS256").toString();
            String payloadText = payload.orElse(payloadObject.toString());
            String headerAndPayload = encode(headerText) + "." + encode(payloadText);
            String signature = sign(headerAndPayload);
            return headerAndPayload + "." + signature;
        }
    }

    private final ObjectMapper mapper = new ObjectMapper();
    private final PublicKey publicKey;
    private final PrivateKey privateKey;

    public JwtFactory() {
        try {
            KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
            keyGen.initialize(RSA_KEY_SIZE);
            KeyPair pair = keyGen.generateKeyPair();
            publicKey = pair.getPublic();
            privateKey = pair.getPrivate();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("RSA not supported: " + e);
        }
    }

    private static String encode(BigInteger value) {
        byte[] data = value.toByteArray();
        return encode(data);
    }

    private static String encode(String value) {
        return encode(value.getBytes(UTF_8));
    }

    private static String encode(byte[] value) {
        byte[] encodedData = BASE64_ENCODER.encode(value);
        return new String(encodedData);
    }

    public PublicKey publicKey() {
        return publicKey;
    }

    public ObjectNode describePublicKey() {
        if (publicKey instanceof RSAPublicKey) {
            return describeRsaPublicKey((RSAPublicKey) publicKey);
        }
        throw new UnsupportedOperationException("Cannot describe public key of type "
                + publicKey.getClass().getCanonicalName());
    }

    private ObjectNode describeRsaPublicKey(RSAPublicKey key) {
        ObjectNode description = mapper.createObjectNode();
        description.put("kty", "RSA");
        description.put("e", encode(key.getPublicExponent()));
        description.put("n", encode(key.getModulus()));
        return description;
    }

    public String sign(String target) {
        try {
            Signature signature = Signature.getInstance("SHA256withRSA");
            signature.initSign(privateKey);
            signature.update(target.getBytes(UTF_8));
            return encode(signature.sign());
        } catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException e) {
            throw new RuntimeException("Problem creating signature: " + e, e);
        }
    }

    public Builder aJwt() {
        return new Builder();
    }
}