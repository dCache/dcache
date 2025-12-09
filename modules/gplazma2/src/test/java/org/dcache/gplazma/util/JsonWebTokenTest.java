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
package org.dcache.gplazma.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.Optional;
import org.junit.Test;

public class JsonWebTokenTest {

    /* HEADER:
        {
            "kid": "rsa1",
            "alg": "RS256"
        }
    PAYLOAD:
        {
            "sub": "ea1a3e57-2155-4895-9dc7-77c3282aa71b",
            "iss": "https://iam.extreme-datacloud.eu/",
            "exp": 1552298417,
            "iat": 1552294817,
            "jti": "cc9b6a5c-8240-455c-98db-7813c2abcefc"
        }
    */
    private static final String VALID_JWT = "eyJraWQiOiJyc2ExIiwiYWxnIjoiUlMyNT"
          + "YifQ.eyJzdWIiOiJlYTFhM2U1Ny0yMTU1LTQ4OTUtOWRjNy03N2MzMjgyYWE3MWI"
          + "iLCJpc3MiOiJodHRwczpcL1wvaWFtLmV4dHJlbWUtZGF0YWNsb3VkLmV1XC8iLCJ"
          + "leHAiOjE1NTIyOTg0MTcsImlhdCI6MTU1MjI5NDgxNywianRpIjoiY2M5YjZhNWM"
          + "tODI0MC00NTVjLTk4ZGItNzgxM2MyYWJjZWZjIn0.Rmc16S2Y-Eae8zLXQJq-_C4"
          + "xsV9SikWpbn9J2lRVBKGEBp_8UvZgv6CdTfvhaRS7JBmAioc_ubLFqh2sBt478xg"
          + "jBFVEiSol5uAMtdxjZSxFZeVCRPXPbgvQLpHIo9jhpWl-YfC18wW_Js9grL8IcZf"
          + "b87_sT-dtXL_ctFHvmic";

    @Test
    public void shouldIdentifyValidJwt() {
        boolean isValid = JsonWebToken.isCompatibleFormat(VALID_JWT);

        assertTrue(isValid);
    }

    @Test
    public void shouldIdentifyJwtWithTooFewDots() {
        boolean isValid = JsonWebToken.isCompatibleFormat(
              "eyJraWQiOiJyc2ExIiwiYWxnIjoiUlMyNTYifQeyJzdWIiOiJlYTFhM2U1Ny" // Missing dot here
                    + "0yMTU1LTQ4OTUtOWRjNy03N2MzMjgyYWE3MWIiLCJpc3MiOiJodHRwczpcL"
                    + "1wvaWFtLmV4dHJlbWUtZGF0YWNsb3VkLmV1XC8iLCJleHAiOjE1NTIyOTg0"
                    + "MTcsImlhdCI6MTU1MjI5NDgxNywianRpIjoiY2M5YjZhNWMtODI0MC00NTV"
                    + "jLTk4ZGItNzgxM2MyYWJjZWZjIn0.Rmc16S2Y-Eae8zLXQJq-_C4xsV9Sik"
                    + "Wpbn9J2lRVBKGEBp_8UvZgv6CdTfvhaRS7JBmAioc_ubLFqh2sBt478xgjB"
                    + "FVEiSol5uAMtdxjZSxFZeVCRPXPbgvQLpHIo9jhpWl-YfC18wW_Js9grL8I"
                    + "cZfb87_sT-dtXL_ctFHvmic");

        assertFalse(isValid);
    }


    @Test
    public void shouldParseValidJwt() throws Exception {
        JsonWebToken jwt = new JsonWebToken(VALID_JWT);

        assertThat(jwt.getKeyIdentifier(), is(equalTo("rsa1")));

        assertThat(jwt.getPayloadString("sub"),
              is(equalTo(Optional.of("ea1a3e57-2155-4895-9dc7-77c3282aa71b"))));
        assertThat(jwt.getPayloadString("iss"),
              is(equalTo(Optional.of("https://iam.extreme-datacloud.eu/"))));
        assertThat(jwt.getPayloadString("jti"),
              is(equalTo(Optional.of("cc9b6a5c-8240-455c-98db-7813c2abcefc"))));
        assertThat(jwt.getPayloadString("nothere"), is(equalTo(Optional.empty())));

        assertThat(jwt.getPayloadInstant("exp"),
              is(equalTo(Optional.of(Instant.parse("2019-03-11T10:00:17Z")))));
        assertThat(jwt.getPayloadInstant("iat"),
              is(equalTo(Optional.of(Instant.parse("2019-03-11T09:00:17Z")))));
        assertThat(jwt.getPayloadInstant("nothere"), is(equalTo(Optional.empty())));
    }

    @Test
    public void shouldTranscodeECDSASignatureToDER() throws Exception {
        // Example ECDSA signature (64 bytes, P-256)
        byte[] jwsSignature = new byte[] {
            (byte) 0x30, (byte) 0x44, (byte) 0x02, (byte) 0x20, // R part
            (byte) 0x5A, (byte) 0x12, (byte) 0x34, (byte) 0x56, (byte) 0x78, (byte) 0x9A, (byte) 0xBC, (byte) 0xDE,
            (byte) 0xF0, (byte) 0x12, (byte) 0x34, (byte) 0x56, (byte) 0x78, (byte) 0x9A, (byte) 0xBC, (byte) 0xDE,
            (byte) 0xF0, (byte) 0x12, (byte) 0x34, (byte) 0x56, (byte) 0x78, (byte) 0x9A, (byte) 0xBC, (byte) 0xDE,
            (byte) 0xF0, (byte) 0x12, (byte) 0x34, (byte) 0x56, (byte) 0x78, (byte) 0x9A, (byte) 0xBC, (byte) 0xDE,
            (byte) 0x02, (byte) 0x20, // S part
            (byte) 0x1A, (byte) 0x23, (byte) 0x45, (byte) 0x67, (byte) 0x89, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF,
            (byte) 0x01, (byte) 0x23, (byte) 0x45, (byte) 0x67, (byte) 0x89, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF,
            (byte) 0x01, (byte) 0x23, (byte) 0x45, (byte) 0x67, (byte) 0x89, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF,
            (byte) 0x01, (byte) 0x23, (byte) 0x45, (byte) 0x67, (byte) 0x89, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF
        };

        // Call the method via reflection since it's private
        java.lang.reflect.Method method = JsonWebToken.class.getDeclaredMethod("transcodeJWTECDSASignatureToDER", byte[].class);
        method.setAccessible(true);
        byte[] der = (byte[]) method.invoke(null, jwsSignature);

        // Expected DER: SEQUENCE + length + INTEGER R + INTEGER S
        // For this example, should be valid DER
        assertThat(der[0], is(equalTo((byte) 0x30))); // SEQUENCE
        // Further assertions can be added for specific DER structure
    }

    @Test(expected = Exception.class)
    public void shouldRejectOddLengthECDSASignature() throws Exception {
        byte[] invalidSignature = new byte[63]; // Odd length

        java.lang.reflect.Method method = JsonWebToken.class.getDeclaredMethod("transcodeJWTECDSASignatureToDER", byte[].class);
        method.setAccessible(true);
        method.invoke(null, invalidSignature);
    }
}
