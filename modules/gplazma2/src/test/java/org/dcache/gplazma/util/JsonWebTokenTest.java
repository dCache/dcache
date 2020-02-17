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

import org.junit.Test;

import java.time.Instant;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class JsonWebTokenTest
{
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
    public void shouldIdentifyValidJwt()
    {
        boolean isValid = JsonWebToken.isCompatibleFormat(VALID_JWT);

        assertTrue(isValid);
    }

    @Test
    public void shouldIdentifyJwtWithTooFewDots()
    {
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
    public void shouldParseValidJwt() throws Exception
    {
        JsonWebToken jwt = new JsonWebToken(VALID_JWT);

        assertThat(jwt.getKeyIdentifier(), is(equalTo("rsa1")));

        assertThat(jwt.getPayloadString("sub"), is(equalTo(Optional.of("ea1a3e57-2155-4895-9dc7-77c3282aa71b"))));
        assertThat(jwt.getPayloadString("iss"), is(equalTo(Optional.of("https://iam.extreme-datacloud.eu/"))));
        assertThat(jwt.getPayloadString("jti"), is(equalTo(Optional.of("cc9b6a5c-8240-455c-98db-7813c2abcefc"))));
        assertThat(jwt.getPayloadString("nothere"), is(equalTo(Optional.empty())));

        assertThat(jwt.getPayloadInstant("exp"), is(equalTo(Optional.of(Instant.parse("2019-03-11T10:00:17Z")))));
        assertThat(jwt.getPayloadInstant("iat"), is(equalTo(Optional.of(Instant.parse("2019-03-11T09:00:17Z")))));
        assertThat(jwt.getPayloadInstant("nothere"), is(equalTo(Optional.empty())));
    }
}
