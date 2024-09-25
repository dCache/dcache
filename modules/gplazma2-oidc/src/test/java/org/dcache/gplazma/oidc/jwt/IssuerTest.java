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

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.security.PublicKey;
import java.util.Base64;
import java.util.Optional;
import org.apache.http.client.HttpClient;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.oidc.IdentityProvider;
import org.dcache.gplazma.oidc.JwtFactory;
import org.dcache.gplazma.oidc.MockHttpClientBuilder;
import org.dcache.gplazma.oidc.MockIdentityProviderBuilder;
import org.dcache.gplazma.util.JsonWebToken;
import org.junit.Before;
import org.junit.Test;
import org.mockito.BDDMockito;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.dcache.gplazma.oidc.MockHttpClientBuilder.aClient;
import static org.dcache.gplazma.oidc.MockIdentityProviderBuilder.anIp;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class IssuerTest {

    private final JwtFactory jwtFactory = new JwtFactory();
    private final JwtFactory anotherJwtFactory = new JwtFactory();

    private HttpClient client;
    private JsonWebToken jwt;
    private IdentityProvider identityProvider;
    private Issuer issuer;

    @Before
    public void setup() {
        client = null;
        jwt = null;
        identityProvider = null;
        issuer = null;
    }

    @Test
    public void shouldReturnIdentityProvider() throws Exception {
        given(aClient());
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/"));
        given(anIssuer().withoutHistory());

        IdentityProvider ip = issuer.getIdentityProvider();

        assertThat(ip, is(sameInstance(identityProvider)));
    }

    @Test
    public void shouldReturnEndpoint() throws Exception {
        given(aClient());
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/"));
        given(anIssuer().withoutHistory());

        String endpoint = issuer.getEndpoint();

        assertThat(endpoint, is(equalTo("https://oidc.example.org/")));
    }

    @Test
    public void shouldNotShowSuppressedByDefault() throws Exception {
        given(aClient());
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/"));
        given(anIssuer().withoutHistory());

        assertThat(issuer.isOfflineSuppressed(), is(equalTo(false)));
    }

    @Test
    public void shouldShowOfflineSuppressedWhenConfigured() throws Exception {
        given(aClient());
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/").withSuppress("offline"));
        given(anIssuer().withoutHistory());

        assertThat(issuer.isOfflineSuppressed(), is(equalTo(true)));
    }

    @Test
    public void shouldPropagateErrorWhenCannotFetchDiscoveryDocument() throws Exception {
        given(aClient().onGet("https://oidc.example.org/.well-known/openid-configuration").responds()
                .withStatusCode(SC_NOT_FOUND).withoutEntity());
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withMissingDiscoveryWithReason("fnord"));
        given(aMockJwt().withoutKid().withoutJti().signedBy(jwtFactory.publicKey()));
        given(anIssuer().withoutHistory());

        try {
            issuer.checkIssued(jwt);
            fail("checkIssued unexpectedly passed");
        } catch (AuthenticationException e) {
            String msg = e.getMessage();
            assertThat(msg, containsString("fnord"));
        }
    }

    // Perhaps this case (MissingDiscoveryWithoutReason) could be dropped in the future.
    @Test(expected=AuthenticationException.class)
    public void shouldPropagateDefaultErrorWhenCannotFetchDiscoveryDocument() throws Exception {
        given(aClient().onGet("https://oidc.example.org/.well-known/openid-configuration").responds()
                .withStatusCode(SC_NOT_FOUND).withoutEntity());
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withMissingDiscoveryWithoutReason());
        given(aMockJwt().withoutKid().withoutJti().signedBy(jwtFactory.publicKey()));
        given(anIssuer().withoutHistory());

        issuer.checkIssued(jwt);
    }

    @Test
    public void shouldAcceptJwtWithoutKidWhenJkwsNoKid() throws Exception {
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withEntity("{\"keys\": [" + jwtFactory.describePublicKey() + "]}"));
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{\"jwks_uri\": \"https://oidc.example.org/jwks\"}"));
        given(aMockJwt().withoutKid().withoutJti().signedBy(jwtFactory.publicKey()));
        given(anIssuer().withoutHistory());

        issuer.checkIssued(jwt);

        verify(jwt).isSignedBy(eq(jwtFactory.publicKey()));
    }

    @Test
    public void shouldAcceptJwtWithoutKid() throws Exception {
        ObjectNode description = jwtFactory.describePublicKey().put("kid", "key-1");
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withEntity("{\"keys\": [" + description + "]}"));
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{\"jwks_uri\": \"https://oidc.example.org/jwks\"}"));
        given(aMockJwt().withoutKid().withoutJti().signedBy(jwtFactory.publicKey()));
        given(anIssuer().withoutHistory());

        issuer.checkIssued(jwt);

        verify(jwt).isSignedBy(eq(jwtFactory.publicKey()));
    }

    @Test
    public void shouldAcceptJwtWithKid() throws Exception {
        ObjectNode description = jwtFactory.describePublicKey().put("kid", "key-1");
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withEntity("{\"keys\": [" + description + "]}"));
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{\"jwks_uri\": \"https://oidc.example.org/jwks\"}"));
        given(anIssuer().withoutHistory());
        given(aMockJwt().withKid("key-1").withoutJti().signedBy(jwtFactory.publicKey()));

        issuer.checkIssued(jwt);

        verify(jwt).isSignedBy(eq(jwtFactory.publicKey()));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectJwtWithUnknownKid() throws Exception {
        ObjectNode description = jwtFactory.describePublicKey().put("kid", "key-1");
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withEntity("{\"keys\": [" + description + "]}"));
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{\"jwks_uri\": \"https://oidc.example.org/jwks\"}"));
        given(anIssuer().withoutHistory());
        given(aMockJwt().withKid("unknown-key-1").withoutJti().signedBy(jwtFactory.publicKey()));

        issuer.checkIssued(jwt);
    }

    @Test
    public void shouldRejectJwtWithDiscoveryNoJwksUri() throws Exception {
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{}"));
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withStatusCode(SC_NOT_FOUND).withoutEntity());
        given(anIssuer().withoutHistory());
        given(aMockJwt().withKid("key-1").withoutJti().signedBy(jwtFactory.publicKey()));

        try {
            issuer.checkIssued(jwt);
            fail("checkIssued returned success");
        } catch (AuthenticationException e) {
            assertThat(e.getMessage(), containsString("jwks_uri"));
            assertThat(e.getMessage(), containsString("missing"));
        }
    }

    @Test
    public void shouldRejectJwtWithDiscoveryNonTextualJwksUri() throws Exception {
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{\"jwks_uri\": null}"));
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withStatusCode(SC_NOT_FOUND).withoutEntity());
        given(anIssuer().withoutHistory());
        given(aMockJwt().withKid("key-1").withoutJti().signedBy(jwtFactory.publicKey()));

        try {
            issuer.checkIssued(jwt);
            fail("checkIssued returned success");
        } catch (AuthenticationException e) {
            assertThat(e.getMessage(), containsString("jwks_uri"));
            assertThat(e.getMessage(), containsString("non-textual"));
        }
    }

    @Test
    public void shouldRejectJwtWithDiscoveryEmptyJwksUri() throws Exception {
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{\"jwks_uri\": \"\"}"));
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withStatusCode(SC_NOT_FOUND).withoutEntity());
        given(anIssuer().withoutHistory());
        given(aMockJwt().withKid("key-1").withoutJti().signedBy(jwtFactory.publicKey()));

        try {
            issuer.checkIssued(jwt);
            fail("checkIssued returned success");
        } catch (AuthenticationException e) {
            assertThat(e.getMessage(), containsString("jwks_uri"));
            assertThat(e.getMessage(), containsString("empty"));
        }
    }

    @Test
    public void shouldRejectJwtWithDiscoveryBadUriJwksUri() throws Exception {
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{\"jwks_uri\": \":bad URI\"}"));
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withStatusCode(SC_NOT_FOUND).withoutEntity());
        given(anIssuer().withoutHistory());
        given(aMockJwt().withKid("key-1").withoutJti().signedBy(jwtFactory.publicKey()));

        try {
            issuer.checkIssued(jwt);
            fail("checkIssued returned success");
        } catch (AuthenticationException e) {
            assertThat(e.getMessage(), containsString("jwks_uri"));
            assertThat(e.getMessage(), containsString("URI"));
        }
    }

    @Test
    public void shouldRejectJwtWithJwksNotValidJson() throws Exception {
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{\"jwks_uri\": \"https://oidc.example.org/jwks\"}"));
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withEntity("NOT JSON"));
        given(anIssuer().withoutHistory());
        given(aMockJwt().withKid("key-1").withoutJti().signedBy(jwtFactory.publicKey()));

        try {
            issuer.checkIssued(jwt);
            fail("checkIssued returned success");
        } catch (AuthenticationException e) {
            assertThat(e.getMessage(), containsString("public keys"));
            assertThat(e.getMessage(), containsString("https://oidc.example.org/jwks"));
            assertThat(e.getMessage(), containsString("Unrecognized token"));
        }
    }

    @Test
    public void shouldRejectJwtWithJwksNotObject() throws Exception {
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{\"jwks_uri\": \"https://oidc.example.org/jwks\"}"));
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withEntity("[]"));
        given(anIssuer().withoutHistory());
        given(aMockJwt().withKid("key-1").withoutJti().signedBy(jwtFactory.publicKey()));

        try {
            issuer.checkIssued(jwt);
            fail("checkIssued returned success");
        } catch (AuthenticationException e) {
            assertThat(e.getMessage(), containsString("public keys"));
            assertThat(e.getMessage(), containsString("wrong type"));
        }
    }

    @Test
    public void shouldRejectJwtWithJwksMissingKeys() throws Exception {
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{\"jwks_uri\": \"https://oidc.example.org/jwks\"}"));
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withEntity("{}"));
        given(anIssuer().withoutHistory());
        given(aMockJwt().withKid("key-1").withoutJti().signedBy(jwtFactory.publicKey()));

        try {
            issuer.checkIssued(jwt);
            fail("checkIssued returned success");
        } catch (AuthenticationException e) {
            assertThat(e.getMessage(), containsString("public keys"));
            assertThat(e.getMessage(), containsString("missing"));
            assertThat(e.getMessage(), containsString("\"keys\""));
        }
    }

    @Test
    public void shouldRejectJwtWithJwksKeysNotArray() throws Exception {
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{\"jwks_uri\": \"https://oidc.example.org/jwks\"}"));
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withEntity("{\"keys\": null}"));
        given(anIssuer().withoutHistory());
        given(aMockJwt().withKid("key-1").withoutJti().signedBy(jwtFactory.publicKey()));

        try {
            issuer.checkIssued(jwt);
            fail("checkIssued returned success");
        } catch (AuthenticationException e) {
            assertThat(e.getMessage(), containsString("public keys"));
            assertThat(e.getMessage(), containsString("wrong type"));
        }
    }

    @Test
    public void shouldRejectJwtWithKeySignedWithMalformedKeysArray() throws Exception {
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{\"jwks_uri\": \"https://oidc.example.org/jwks\"}"));
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withEntity("{\"keys\": [ \"a random string\"]}"));
        given(anIssuer().withoutHistory());
        given(aMockJwt().withKid("key-1").withoutJti().signedBy(jwtFactory.publicKey()));

        try {
            issuer.checkIssued(jwt);
            fail("checkIssued returned success");
        } catch (AuthenticationException e) {
            assertThat(e.getMessage(), containsString("no public key"));
        }
    }

    @Test
    public void shouldRejectJwtWithKeySignedWithKeyOfUnknownKeyType() throws Exception {
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{\"jwks_uri\": \"https://oidc.example.org/jwks\"}"));
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withEntity("{\"keys\": [{"
                        + "\"kid\": \"key-1\","
                        + "\"kty\": \"UNKNOWN_KEY_TYPE\"}]}"));
        given(anIssuer().withoutHistory());
        given(aMockJwt().withKid("key-1").withoutJti().signedBy(jwtFactory.publicKey()));

        try {
            issuer.checkIssued(jwt);
            fail("checkIssued returned success");
        } catch (AuthenticationException e) {
            assertThat(e.getMessage(), containsString("key type"));
            assertThat(e.getMessage(), containsString("UNKNOWN_KEY_TYPE"));
        }
    }

    @Test
    public void shouldRejectJwtWithKeySignedWithKeyWithMissingKeyType() throws Exception {
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{\"jwks_uri\": \"https://oidc.example.org/jwks\"}"));
        var description = jwtFactory.describePublicKey().put("kid", "key-1").without("kty");
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withEntity("{\"keys\": [" + description + "]}"));
        given(anIssuer().withoutHistory());
        given(aMockJwt().withKid("key-1").withoutJti().signedBy(jwtFactory.publicKey()));

        try {
            issuer.checkIssued(jwt);
            fail("checkIssued returned success");
        } catch (AuthenticationException e) {
            assertThat(e.getMessage(), containsStringIgnoringCase("attribute"));
            assertThat(e.getMessage(), containsString("kty")); //REVISIT shouldn't kty be in quotes?
        }
    }

    @Test
    public void shouldRejectJwtWithKeySignedWithKeyWithNonTexturalKeyType() throws Exception {
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{\"jwks_uri\": \"https://oidc.example.org/jwks\"}"));
        var description = jwtFactory.describePublicKey()
                .put("kid", "key-1")
                .put("kty", 42);
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withEntity("{\"keys\": [" + description + "]}"));
        given(anIssuer().withoutHistory());
        given(aMockJwt().withKid("key-1").withoutJti().signedBy(jwtFactory.publicKey()));

        try {
            issuer.checkIssued(jwt);
            fail("checkIssued returned success");
        } catch (AuthenticationException e) {
            assertThat(e.getMessage(), containsStringIgnoringCase("attribute"));
            assertThat(e.getMessage(), containsString("textual"));
            assertThat(e.getMessage(), containsString("kty")); //REVISIT shouldn't kty be in quotes?
        }
    }

    @Test
    public void shouldRejectJwtWithNoKidNotSignedByPublicKey() throws Exception {
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{\"jwks_uri\": \"https://oidc.example.org/jwks\"}"));
        var description = jwtFactory.describePublicKey();
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withEntity("{\"keys\": [" + description + "]}"));
        given(anIssuer().withoutHistory());
        given(aMockJwt().withoutKid().withoutJti().signedBy(anotherJwtFactory.publicKey()));

        try {
            issuer.checkIssued(jwt);
            fail("checkIssued returned success");
        } catch (AuthenticationException e) {
            assertThat(e.getMessage(), containsString("public key"));
        }
    }

    @Test
    public void shouldRejectJwtWithNoKidNotSignedByPublicKeyThatHasAProblem() throws Exception {
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{\"jwks_uri\": \"https://oidc.example.org/jwks\"}"));
        var description = jwtFactory.describePublicKey().put("kty", "UNKNOWN_ALG");
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withEntity("{\"keys\": [" + description + "]}"));
        given(anIssuer().withoutHistory());
        given(aMockJwt().withoutKid().withoutJti().signedBy(anotherJwtFactory.publicKey()));

        try {
            issuer.checkIssued(jwt);
            fail("checkIssued returned success");
        } catch (AuthenticationException e) {
            assertThat(e.getMessage(), containsStringIgnoringCase("issuer"));
            assertThat(e.getMessage(), containsString("public key"));
            assertThat(e.getMessage(), containsString("UNKNOWN_ALG"));
        }
    }

    private ObjectNode mangle(ObjectNode description) {
        String encodedN = description.get("n").asText();
        byte[] n = Base64.getUrlDecoder().decode(encodedN);

        // Mangle the modulus
        byte[] mangledN = new byte[2];
        System.arraycopy( n, 0, mangledN, 0, 2);
        String encodedMangledN = Base64.getUrlEncoder().encodeToString(mangledN);

        return description.deepCopy().put("n", encodedMangledN);
    }

    @Test
    public void shouldRejectJwtWithNoKidNotSignedByPublicKeyThatHasACryptoProblem() throws Exception {
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{\"jwks_uri\": \"https://oidc.example.org/jwks\"}"));
        var description = mangle(jwtFactory.describePublicKey())
                .put("kid", "key-1");
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withEntity("{\"keys\": [" + description + "]}"));
        given(anIssuer().withoutHistory());
        given(aMockJwt().withKid("key-1").withoutJti().signedBy(anotherJwtFactory.publicKey()));

        try {
            issuer.checkIssued(jwt);
            fail("checkIssued returned success");
        } catch (AuthenticationException e) {
            assertThat(e.getMessage(), containsString("public key"));
            assertThat(e.getMessage(), containsString("RSA"));
            assertThat(e.getMessage(), containsString("512 bits long"));
        }
    }

    @Test
    public void shouldAcceptJwtFromKey1FromOpWithMultipleKeys() throws Exception {
        ObjectNode description1 = jwtFactory.describePublicKey().put("kid", "key-1");
        ObjectNode description2 = anotherJwtFactory.describePublicKey().put("kid", "key-2");
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withEntity("{\"keys\": [" + description1 + "," + description2 + "]}"));
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{\"jwks_uri\": \"https://oidc.example.org/jwks\"}"));
        given(anIssuer().withoutHistory());
        given(aMockJwt().withKid("key-1").withoutJti().signedBy(jwtFactory.publicKey()));

        issuer.checkIssued(jwt);

        verify(jwt).isSignedBy(eq(jwtFactory.publicKey()));
        verify(jwt, never()).isSignedBy(eq(anotherJwtFactory.publicKey()));
    }

    @Test
    public void shouldRejectJwtWithKidSignedByKeyWithInvalidKid() throws Exception {
        // Description has invalid 'kid' value: it should be a string, not a number.
        ObjectNode description = jwtFactory.describePublicKey().put("kid", 42);
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withEntity("{\"keys\": [" + description + "]}"));
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{\"jwks_uri\": \"https://oidc.example.org/jwks\"}"));
        given(anIssuer().withoutHistory());
        given(aMockJwt().withKid("42").withoutJti().signedBy(jwtFactory.publicKey()));

        // NB. The current strategy rejects this JWT; however, this isn't a
        // requirement.  Some future patch might implement a fall-back strategy
        // that would allow this case to succeed.
        //
        // This test exists to exercise the corresponding code-path (ensuring
        // dCache handles this case robustly) and to ensure the corresponding
        // error message contains the expected content.

        try {
            issuer.checkIssued(jwt);
            fail("checkIssued returned success");
        } catch (AuthenticationException e) {
            assertThat(e.getMessage(), containsString("issuer"));
            assertThat(e.getMessage(), containsString("public key"));
            assertThat(e.getMessage(), containsString("\"42\""));
        }
    }

    @Test
    public void shouldAcceptJwtWithoutKidSignedByKeyWithInvalidKid() throws Exception {
        // Description has invalid 'kid' value: it should be a string, not a number.
        ObjectNode description = jwtFactory.describePublicKey().put("kid", 42);
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withEntity("{\"keys\": [" + description + "]}"));
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{\"jwks_uri\": \"https://oidc.example.org/jwks\"}"));
        given(anIssuer().withoutHistory());
        given(aMockJwt().withoutKid().withoutJti().signedBy(jwtFactory.publicKey()));

        issuer.checkIssued(jwt);
    }

    @Test
    public void shouldAcceptJwtFromKey2FromOpWithMultipleKeys() throws Exception {
        ObjectNode description1 = jwtFactory.describePublicKey().put("kid", "key-1");
        ObjectNode description2 = anotherJwtFactory.describePublicKey().put("kid", "key-2");
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withEntity("{\"keys\": [" + description1 + "," + description2 + "]}"));
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{\"jwks_uri\": \"https://oidc.example.org/jwks\"}"));
        given(anIssuer().withoutHistory());
        given(aMockJwt().withKid("key-2").withoutJti().signedBy(anotherJwtFactory.publicKey()));

        issuer.checkIssued(jwt);

        verify(jwt, never()).isSignedBy(eq(jwtFactory.publicKey()));
        verify(jwt).isSignedBy(eq(anotherJwtFactory.publicKey()));
    }

    @Test
    public void shouldAcceptJwtFromKey1FromOpWithMultipleKeysNoKid() throws Exception {
        ObjectNode description1 = jwtFactory.describePublicKey();
        ObjectNode description2 = anotherJwtFactory.describePublicKey();
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withEntity("{\"keys\": [" + description1 + "," + description2 + "]}"));
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{\"jwks_uri\": \"https://oidc.example.org/jwks\"}"));
        given(anIssuer().withoutHistory());
        given(aMockJwt().withoutKid().withoutJti().signedBy(jwtFactory.publicKey()));

        issuer.checkIssued(jwt);

        verify(jwt).isSignedBy(eq(jwtFactory.publicKey()));

        // NB we don't verify whether JsonWebToken.isSignedBy(anotherJwtFactory) is called as we
        // don't want to force a particular order on trying out the public keys.
    }

    @Test
    public void shouldAcceptJwtFromKey2FromOpWithMultipleKeysNoKid() throws Exception {
        ObjectNode description1 = jwtFactory.describePublicKey();
        ObjectNode description2 = anotherJwtFactory.describePublicKey();
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withEntity("{\"keys\": [" + description1 + "," + description2 + "]}"));
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{\"jwks_uri\": \"https://oidc.example.org/jwks\"}"));
        given(anIssuer().withoutHistory());
        given(aMockJwt().withoutKid().withoutJti().signedBy(anotherJwtFactory.publicKey()));

        issuer.checkIssued(jwt);

        verify(jwt).isSignedBy(eq(anotherJwtFactory.publicKey()));

        // NB we don't verify whether JsonWebToken.isSignedBy(jwtFactory) is called as we
        // don't want to force a particular order on trying out the public keys.
    }

    @Test
    public void shouldAcceptJwtWithoutJtiMultipleTimesWithoutReplayProtection() throws Exception {
        ObjectNode description = jwtFactory.describePublicKey();
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withEntity("{\"keys\": [" + description + "]}"));
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{\"jwks_uri\": \"https://oidc.example.org/jwks\"}"));
        given(anIssuer().withoutHistory());
        given(aMockJwt().withoutKid().withoutJti().signedBy(jwtFactory.publicKey()));

        issuer.checkIssued(jwt);
        issuer.checkIssued(jwt);
    }

    @Test
    public void shouldAcceptJwtWithoutJtiMultipleTimesWithReplayProtection() throws Exception {
        ObjectNode description = jwtFactory.describePublicKey();
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withEntity("{\"keys\": [" + description + "]}"));
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{\"jwks_uri\": \"https://oidc.example.org/jwks\"}"));
        given(anIssuer().withHistory(10));
        given(aMockJwt().withoutKid().withoutJti().signedBy(jwtFactory.publicKey()));

        issuer.checkIssued(jwt);
        issuer.checkIssued(jwt);
    }

    @Test
    public void shouldAcceptJwtWithJtiMultipleTimesWithoutReplayProtection() throws Exception {
        ObjectNode description = jwtFactory.describePublicKey();
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withEntity("{\"keys\": [" + description + "]}"));
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{\"jwks_uri\": \"https://oidc.example.org/jwks\"}"));
        given(anIssuer().withoutHistory());
        given(aMockJwt().withoutKid().withJti("token-42").signedBy(jwtFactory.publicKey()));

        issuer.checkIssued(jwt);
        issuer.checkIssued(jwt);
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectJwtWithJtiMultipleTimesWithReplayProtection() throws Exception {
        ObjectNode description = jwtFactory.describePublicKey();
        given(aClient().onGet("https://oidc.example.org/jwks").responds()
                .withEntity("{\"keys\": [" + description + "]}"));
        given(anIp("EXAMPLE").withEndpoint("https://oidc.example.org/")
                .withDiscovery("{\"jwks_uri\": \"https://oidc.example.org/jwks\"}"));
        given(anIssuer().withHistory(10));
        given(aMockJwt().withoutKid().withJti("token-42").signedBy(jwtFactory.publicKey()));

        issuer.checkIssued(jwt);
        issuer.checkIssued(jwt);
    }

    public void given(MockHttpClientBuilder builder) {
        client = builder.build();
    }

    public void given(MockIdentityProviderBuilder builder) {
        identityProvider = builder.build();
    }

    public void given(JsonWebTokenBuilder builder) {
        jwt = builder.build();
    }

    public void given(IssuerBuilder builder) {
        issuer = builder.build();
    }

    public JsonWebTokenBuilder aMockJwt() {
        return new JsonWebTokenBuilder();
    }

    public IssuerBuilder anIssuer() {
        return new IssuerBuilder();
    }

    /**
     * A fluent class that builds a mock JsonWebToken object.
     */
    private static class JsonWebTokenBuilder {
        private static final JsonWebToken token = mock(JsonWebToken.class);
        private boolean hasJti;
        private boolean hasKid;

        public JsonWebTokenBuilder withKid(String kid) {
            checkState(!hasKid);
            BDDMockito.given(token.getKeyIdentifier()).willReturn(kid);
            hasKid = true;
            return this;
        }

        public JsonWebTokenBuilder withoutKid() {
            checkState(!hasKid);
            BDDMockito.given(token.getKeyIdentifier()).willReturn(null);
            hasKid = true;
            return this;
        }

        public JsonWebTokenBuilder withJti(String id) {
            checkState(!hasJti);
            BDDMockito.given(token.getPayloadString(eq("jti"))).willReturn(Optional.ofNullable(id));
            hasJti = true;
            return this;
        }

        public JsonWebTokenBuilder withoutJti() {
            checkState(!hasJti);
            BDDMockito.given(token.getPayloadString(eq("jti"))).willReturn(Optional.empty());
            hasJti = true;
            return this;
        }

        public JsonWebTokenBuilder signedBy(PublicKey key) {
            BDDMockito.given(token.isSignedBy(eq(key))).willReturn(true);
            return this;
        }

        public JsonWebToken build() {
            checkState(hasJti);
            checkState(hasKid);
            return token;
        }
    }

    /**
     * A fluent class that builds a Issuer.
     */
    private class IssuerBuilder {
        int history;
        boolean hasHistory;

        public IssuerBuilder withHistory(int n) {
            checkState(!hasHistory);
            history = n;
            hasHistory = true;
            return this;
        }

        public IssuerBuilder withoutHistory() {
            checkState(!hasHistory);
            history = 0;
            hasHistory = true;
            return this;
        }

        public Issuer build() {
            checkState(hasHistory);
            checkState(client != null);
            checkState(identityProvider != null);
            return new Issuer(client, identityProvider, history);
        }
    }
}