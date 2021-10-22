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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.dcache.gplazma.util.JsonWebToken;
import org.junit.Before;
import org.junit.Test;

import static com.github.npathai.hamcrestopt.OptionalMatchers.isPresentAnd;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertTrue;

public class JwtFactoryTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private JwtFactory factory;

    @Before
    public void setup() {
        factory = null;
    }

    @Test
    public void shouldGenerateValidTokenWithoutPayloadClaims() throws Exception {
        given(aJwtFactory());

        String jwt = factory.aJwt().build();

        assertTrue(JsonWebToken.isCompatibleFormat(jwt));
        JsonWebToken token = new JsonWebToken(jwt);
        assertThat(token.getKeyIdentifier(), is(nullValue()));
        assertThat(token.getPayloadMap(), is(anEmptyMap()));
        assertTrue(token.isSignedBy(factory.publicKey()));
    }

    @Test
    public void shouldGenerateValidTokenWithAPayloadStringClaim() throws Exception {
        given(aJwtFactory());

        String jwt = factory.aJwt().withPayloadClaim("sub", "my-identity").build();

        assertTrue(JsonWebToken.isCompatibleFormat(jwt));
        JsonWebToken token = new JsonWebToken(jwt);
        assertThat(token.getKeyIdentifier(), is(nullValue()));
        assertThat(token.getPayloadString("sub"), isPresentAnd(equalTo("my-identity")));
        assertThat(token.getPayloadMap(), aMapWithSize(1));
        assertThat(token.getPayloadMap(), hasEntry("sub", jsonString("my-identity")));
        assertTrue(token.isSignedBy(factory.publicKey()));
    }

    @Test
    public void shouldGenerateValidTokenWithAPayloadInstantClaim() throws Exception {
        Instant expiry = Instant.now().plus(5, MINUTES).truncatedTo(SECONDS); // We work with seconds granularity.
        given(aJwtFactory());

        String jwt = factory.aJwt()
                .withPayloadClaim("sub", "my-identity")
                .withPayloadClaim("exp", expiry)
                .build();

        assertTrue(JsonWebToken.isCompatibleFormat(jwt));
        JsonWebToken token = new JsonWebToken(jwt);
        assertThat(token.getKeyIdentifier(), is(nullValue()));
        assertThat(token.getPayloadString("sub"), isPresentAnd(equalTo("my-identity")));
        assertThat(token.getPayloadInstant("exp"), isPresentAnd(equalTo(expiry)));
        assertThat(token.getPayloadMap(), aMapWithSize(2));
        assertThat(token.getPayloadMap(), hasEntry("sub", jsonString("my-identity")));
        assertTrue(token.isSignedBy(factory.publicKey()));
    }

    @Test
    public void shouldGenerateValidTokenWithAPayloadArrayClaim() throws Exception {
        given(aJwtFactory());

        String jwt = factory.aJwt()
                .withPayloadClaim("sub", "my-identity")
                .withPayloadClaim("groups", "group-1", "group-2")
                .build();

        assertTrue(JsonWebToken.isCompatibleFormat(jwt));
        JsonWebToken token = new JsonWebToken(jwt);
        assertThat(token.getKeyIdentifier(), is(nullValue()));
        assertThat(token.getPayloadString("sub"), isPresentAnd(equalTo("my-identity")));
        assertThat(token.getPayloadStringOrArray("groups"), contains("group-1", "group-2"));
        assertThat(token.getPayloadMap(), aMapWithSize(2));
        assertThat(token.getPayloadMap(), hasEntry("sub", jsonString("my-identity")));
        assertThat(token.getPayloadMap(), hasEntry("groups", jsonArray("group-1", "group-2")));
        assertTrue(token.isSignedBy(factory.publicKey()));
    }

    @Test
    public void shouldGenerateValidTokenWithKidAndAPayloadClaim() throws Exception {
        given(aJwtFactory());

        String jwt = factory.aJwt()
                .withHeaderClaim("kid", "KEY-1")
                .withPayloadClaim("sub", "my-identity")
                .build();

        assertTrue(JsonWebToken.isCompatibleFormat(jwt));
        JsonWebToken token = new JsonWebToken(jwt);
        assertThat(token.getKeyIdentifier(), is(equalTo("KEY-1")));
        assertThat(token.getPayloadString("sub"), isPresentAnd(equalTo("my-identity")));
        assertThat(token.getPayloadMap(), aMapWithSize(1));
        assertThat(token.getPayloadMap(), hasEntry("sub", jsonString("my-identity")));
        assertTrue(token.isSignedBy(factory.publicKey()));
    }

    @Test(expected=IOException.class)
    public void shouldGenerateInvalidToken() throws Exception {
        given(aJwtFactory());

        String jwt = factory.aJwt().withPayload("BOGUS PAYLOAD VALUE").build();

        assertTrue(JsonWebToken.isCompatibleFormat(jwt));
        new JsonWebToken(jwt);
    }

    private void given(JwtFactoryBuilder builder) {
        factory = builder.build();
    }

    private JwtFactoryBuilder aJwtFactory() {
        return new JwtFactoryBuilder();
    }

    private JsonNode jsonString(String value) throws JsonProcessingException {
        return mapper.readTree("\"" + value + "\"");
    }

    private JsonNode jsonArray(String... value) throws JsonProcessingException {
        String json = Arrays.stream(value).collect(Collectors.joining("\", \"", "[\"", "\"]"));
        return mapper.readTree(json);
    }

    /**
     * Fluent class for building JwtFactory instances.
     */
    private static class JwtFactoryBuilder {
        public JwtFactory build() {
            return new JwtFactory();
        }
    }
}
