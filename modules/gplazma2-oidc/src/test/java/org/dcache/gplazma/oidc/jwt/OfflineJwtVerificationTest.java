/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2022 Deutsches Elektronen-Synchrotron
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.oidc.ExtractResult;
import org.dcache.gplazma.oidc.IdentityProvider;
import org.dcache.gplazma.oidc.JwtFactory;
import org.dcache.gplazma.oidc.MockIdentityProviderBuilder;
import org.dcache.gplazma.oidc.UnableToProcess;
import org.junit.Before;
import org.junit.Test;
import org.mockito.BDDMockito;

import static com.google.common.base.Preconditions.checkState;
import static java.time.temporal.ChronoUnit.MINUTES;
import static org.dcache.gplazma.oidc.MockIdentityProviderBuilder.anIp;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;

public class OfflineJwtVerificationTest {

    private final JwtFactory jwtFactory = new JwtFactory();
    private final ObjectMapper mapper = new ObjectMapper();

    private OfflineJwtVerification verification;
    private String jwt;
    private IdentityProvider identityProvider;

    @Before
    public void setup() {
        verification = null;
        jwt = null;
        identityProvider = null;
    }

    @Test(expected=UnableToProcess.class)
    public void shouldNotProcessNonJwt() throws Exception {
        given(anOfflineJwtVerification().withEmptyAudienceTargetProperty());

        verification.extract("token-that-is-not-a-jwt");
    }

    @Test(expected=UnableToProcess.class)
    public void shouldNotProcessValidJwtWhenOfflineSuppressed() throws Exception {
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/"));
        given(anOfflineJwtVerification()
            .withEmptyAudienceTargetProperty()
            .withIssuer(anIssuer().withIp(identityProvider).withOfflineSuppressed()));
        given(aJwt()
            .withPayloadClaim("iss", "https://oidc.example.org/")
            .withPayloadClaim("sub", "paul"));

        verification.extract(jwt);
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectTokenWithoutIss() throws Exception {
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/"));
        given(anOfflineJwtVerification()
            .withEmptyAudienceTargetProperty()
            .withIssuer(anIssuer().withIp(identityProvider)));
        given(aJwt()
            .withPayloadClaim("sub", "paul"));

        verification.extract(jwt);
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectTokenFromUntrustedOp() throws Exception {
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/"));
        given(anOfflineJwtVerification()
            .withEmptyAudienceTargetProperty()
            .withIssuer(anIssuer().withIp(identityProvider)));
        given(aJwt()
            .withPayloadClaim("iss", "https://another-oidc.example.com/")
            .withPayloadClaim("sub", "paul"));

        verification.extract(jwt);
    }

    @Test
    public void shouldAcceptNonExpiringTokenFromTrustedOp() throws Exception {
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/"));
        given(anOfflineJwtVerification()
            .withEmptyAudienceTargetProperty()
            .withIssuer(anIssuer().withIp(identityProvider)));
        given(aJwt()
            .withPayloadClaim("iss", "https://oidc.example.org/")
            .withPayloadClaim("sub", "paul"));

        ExtractResult result = verification.extract(jwt);

        assertThat(result.idp(), is(sameInstance(identityProvider)));
        assertThat(result.claims(), hasEntry("iss", jsonString("https://oidc.example.org/")));
        assertThat(result.claims(), hasEntry("sub", jsonString("paul")));
    }

    @Test
    public void shouldAcceptNonExpiredTokenFromTrustedOp() throws Exception {
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/"));
        given(anOfflineJwtVerification()
            .withEmptyAudienceTargetProperty()
            .withIssuer(anIssuer().withIp(identityProvider)));
        given(aJwt()
            .withPayloadClaim("iss", "https://oidc.example.org/")
            .withPayloadClaim("exp", Instant.now().plus(5, MINUTES))
            .withPayloadClaim("sub", "paul"));

        ExtractResult result = verification.extract(jwt);

        assertThat(result.idp(), is(sameInstance(identityProvider)));
        assertThat(result.claims(), hasEntry("iss", jsonString("https://oidc.example.org/")));
        assertThat(result.claims(), hasEntry("sub", jsonString("paul")));
        assertThat(result.claims(), hasKey("exp"));
    }

    @Test
    public void shouldAcceptNonExpiredUnembargoedTokenFromTrustedOp() throws Exception {
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/"));
        given(anOfflineJwtVerification()
            .withEmptyAudienceTargetProperty()
            .withIssuer(anIssuer().withIp(identityProvider)));
        given(aJwt()
            .withPayloadClaim("iss", "https://oidc.example.org/")
            .withPayloadClaim("nbf", Instant.now().minus(5, MINUTES))
            .withPayloadClaim("exp", Instant.now().plus(5, MINUTES))
            .withPayloadClaim("sub", "paul"));

        ExtractResult result = verification.extract(jwt);

        assertThat(result.idp(), is(sameInstance(identityProvider)));
        assertThat(result.claims(), hasEntry("iss", jsonString("https://oidc.example.org/")));
        assertThat(result.claims(), hasEntry("sub", jsonString("paul")));
        assertThat(result.claims(), hasKey("nbf"));
        assertThat(result.claims(), hasKey("exp"));
    }

    @Test
    public void shouldAcceptUnembargoedTokenFromTrustedOp() throws Exception {
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/"));
        given(anOfflineJwtVerification()
            .withEmptyAudienceTargetProperty()
            .withIssuer(anIssuer().withIp(identityProvider)));
        given(aJwt()
            .withPayloadClaim("iss", "https://oidc.example.org/")
            .withPayloadClaim("nbf", Instant.now().minus(5, MINUTES))
            .withPayloadClaim("sub", "paul"));

        ExtractResult result = verification.extract(jwt);

        assertThat(result.idp(), is(sameInstance(identityProvider)));
        assertThat(result.claims(), hasEntry("iss", jsonString("https://oidc.example.org/")));
        assertThat(result.claims(), hasEntry("sub", jsonString("paul")));
        assertThat(result.claims(), hasKey("nbf"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectExpiredTokenFromTrustedOp() throws Exception {
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/"));
        given(anOfflineJwtVerification()
            .withEmptyAudienceTargetProperty()
            .withIssuer(anIssuer().withIp(identityProvider)));
        given(aJwt()
            .withPayloadClaim("iss", "https://oidc.example.org/")
            .withPayloadClaim("exp", Instant.now().minus(5, MINUTES))
            .withPayloadClaim("sub", "paul"));

        verification.extract(jwt);
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectExpiredUnembargoedTokenFromTrustedOp() throws Exception {
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/"));
        given(anOfflineJwtVerification()
            .withEmptyAudienceTargetProperty()
            .withIssuer(anIssuer().withIp(identityProvider)));
        given(aJwt()
            .withPayloadClaim("iss", "https://oidc.example.org/")
            .withPayloadClaim("nbf", Instant.now().minus(10, MINUTES))
            .withPayloadClaim("exp", Instant.now().minus(5, MINUTES))
            .withPayloadClaim("sub", "paul"));

        verification.extract(jwt);
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectEmbargoedTokenFromTrustedOp() throws Exception {
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/"));
        given(anOfflineJwtVerification()
            .withEmptyAudienceTargetProperty()
            .withIssuer(anIssuer().withIp(identityProvider)));
        given(aJwt()
            .withPayloadClaim("iss", "https://oidc.example.org/")
            .withPayloadClaim("nbf", Instant.now().plus(5, MINUTES))
            .withPayloadClaim("sub", "paul"));

        verification.extract(jwt);
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectUnexpiredEmbargoedTokenFromTrustedOp() throws Exception {
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/"));
        given(anOfflineJwtVerification()
            .withEmptyAudienceTargetProperty()
            .withIssuer(anIssuer().withIp(identityProvider)));
        given(aJwt()
            .withPayloadClaim("iss", "https://oidc.example.org/")
            .withPayloadClaim("nbf", Instant.now().plus(5, MINUTES))
            .withPayloadClaim("exp", Instant.now().plus(10, MINUTES))
            .withPayloadClaim("sub", "paul"));

        verification.extract(jwt);
    }

    private JsonNode jsonString(String json) throws JsonProcessingException {
        return mapper.readTree("\"" + json + "\"");
    }

    private void given(MockIdentityProviderBuilder builder) {
        identityProvider = builder.build();
    }

    private void given(OfflineJwtVerificationBuilder builder) {
        verification = builder.build();
    }

    private void given(JwtFactory.Builder builder) {
        jwt = builder.build();
    }

    private MockIssuerBuilder anIssuer() {
        return new MockIssuerBuilder();
    }

    private JwtFactory.Builder aJwt() {
        return jwtFactory.aJwt();
    }

    private OfflineJwtVerificationBuilder anOfflineJwtVerification() {
        return new OfflineJwtVerificationBuilder();
    }

    /**
     * A fluent class to build a mock Issuer.
     */
    private static class MockIssuerBuilder {
        private final Issuer issuer = mock(Issuer.class);
        private boolean hasIp;

        public MockIssuerBuilder withIp(MockIdentityProviderBuilder builder) {
            return withIp(builder.build());
        }

        public MockIssuerBuilder withIp(IdentityProvider ip) {
            checkState(!hasIp);
            var endpoint = ip.getIssuerEndpoint();
            BDDMockito.given(issuer.getIdentityProvider()).willReturn(ip);
            BDDMockito.given(issuer.getEndpoint()).willReturn(endpoint.toASCIIString());
            hasIp = true;
            return this;
        }

        public MockIssuerBuilder withOfflineSuppressed() {
            BDDMockito.given(issuer.isOfflineSuppressed()).willReturn(true);
            return this;
        }

        public Issuer build() {
            checkState(hasIp);
            return issuer;
        }
    }

    /**
     * A fluent class to build a (real) OfflineJwtVerification object.
     */
    private static class OfflineJwtVerificationBuilder {
        private final Properties properties = new Properties();
        private final List<Issuer> issuers = new ArrayList<>();

        public OfflineJwtVerificationBuilder withAudienceTargetProperty(String value) {
            properties.setProperty("gplazma.oidc.audience-targets", value);
            return this;
        }

        public OfflineJwtVerificationBuilder withEmptyAudienceTargetProperty() {
            properties.setProperty("gplazma.oidc.audience-targets", "");
            return this;
        }

        public OfflineJwtVerificationBuilder withIssuer(MockIssuerBuilder builder) {
            issuers.add(builder.build());
            return this;
        }

        public OfflineJwtVerification build() {
            return new OfflineJwtVerification(properties, issuers);
        }
    }
}