/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2020 Deutsches Elektronen-Synchrotron
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

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.Signature;
import java.security.SignatureException;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import diskCacheV111.util.FsPath;

import org.dcache.auth.BearerTokenCredential;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.JwtJtiPrincipal;
import org.dcache.auth.JwtSubPrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.util.PrincipalSetMaker;

import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.Objects.requireNonNull;
import static org.dcache.auth.attributes.Activity.*;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class SciTokenPluginTest
{
    private static final PrivateKey PRIVATE_KEY;
    private static final PublicKey PUBLIC_KEY;
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Base64.Encoder BASE64_ENCODER = Base64.getUrlEncoder().withoutPadding();

    private SciTokenPlugin plugin;
    private Set<Principal> identifiedPrincipals;
    private Set<Restriction> restrictions;
    private HttpClient client;
    private Map<String,IssuerInfo> signers;
    private Restriction resultingRestriction;

    static {
        try {
            Security.addProvider(new BouncyCastleProvider());
            KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA", "BC");
            SecureRandom random = SecureRandom.getInstance("SHA1PRNG", "SUN");
            keyGen.initialize(512, random);
            KeyPair pair = keyGen.generateKeyPair();
            PRIVATE_KEY = pair.getPrivate();
            PUBLIC_KEY = pair.getPublic();
        } catch (NoSuchAlgorithmException | NoSuchProviderException e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void setup()
    {
        plugin = null;
        identifiedPrincipals = new HashSet<>();
        restrictions = new HashSet<>();
        signers = new HashMap<>();
        resultingRestriction = null;
        client = mock(HttpClient.class);
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectMissingID()
    {
        Properties properties = new Properties();
        properties.setProperty("gplazma.scitoken.issuer!", "https://example.org/ /prefix/path uid:1000 gid:1000");
        SciTokenPlugin ignored = new SciTokenPlugin(properties, client);
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectMissingURL()
    {
        Properties properties = new Properties();
        properties.setProperty("gplazma.scitoken.issuer!EXAMPLE", "");
        SciTokenPlugin ignored = new SciTokenPlugin(properties, client);
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectBadURLMissingSchema()
    {
        Properties properties = new Properties();
        properties.setProperty("gplazma.scitoken.issuer!EXAMPLE", "not-a-url /prefix/path uid:1000 gid:1000");
        SciTokenPlugin ignored = new SciTokenPlugin(properties, client);
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectBadURLWrongSchema()
    {
        Properties properties = new Properties();
        properties.setProperty("gplazma.scitoken.issuer!EXAMPLE", "ftp://example.org/ /prefix/path uid:1000 gid:1000");
        SciTokenPlugin ignored = new SciTokenPlugin(properties, client);
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectBadURLMissingHostname()
    {
        Properties properties = new Properties();
        properties.setProperty("gplazma.scitoken.issuer!EXAMPLE", "http:/// /prefix/path uid:1000 gid:1000");
        SciTokenPlugin ignored = new SciTokenPlugin(properties, client);
    }

    @Test
    public void shouldAcceptHttpsURLSchema()
    {
        Properties properties = new Properties();
        properties.setProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000");
        properties.setProperty("gplazma.scitoken.token-history", "0");
        properties.setProperty("gplazma.scitoken.audience-targets", "");

        SciTokenPlugin ignored = new SciTokenPlugin(properties, client);
    }

    @Test(expected=AuthenticationException.class)
    public void shouldFailIfNoBearerToken() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));

        whenAuthenticatingWith(this.principals().withDn("/C=DE/O=GermanGrid/OU=DESY/CN=Alexander Paul Millar"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldFailWhenPresentedWithTwoJwtTokens() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        whenAuthenticatingWith(
                aJwtToken()
                    .withRandomJti()
                    .withRandomSub()
                    .withClaim("scope", "read:/")
                    .issuedBy("OP1").usingKey("key1"),
                aJwtToken()
                        .withRandomJti()
                        .withRandomSub()
                        .withClaim("scope", "write:/")
                        .issuedBy("OP1").usingKey("key1"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectJwtWithoutSubClaim() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        whenAuthenticatingWith(aJwtToken()
                .withRandomJti()
                .withRandomSub()
                .issuedBy("OP1").usingKey("key1"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectJwtWithoutIssClaim() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        whenAuthenticatingWith(aJwtToken()
                .withRandomJti()
                .withRandomSub()
                .withClaim("scope", "read:/")
                .issuedBy("OP1").usingKey("key1")
                .omittingClaim("iss"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectJwtWithUnknownIssClaim() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        whenAuthenticatingWith(aJwtToken()
                .withRandomJti()
                .withRandomSub()
                .withClaim("scope", "read:/")
                .issuedBy("OP1").usingKey("key1")
                .withClaim("iss", "https://bad-actor.example.com/"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectExpiredJwt() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        whenAuthenticatingWith(aJwtToken()
                .withRandomJti()
                .withRandomSub()
                .withClaim("scope", "read:/")
                .withClaim("exp", Instant.now().minus(10, MINUTES))
                .issuedBy("OP1").usingKey("key1"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectBadlyFormattedOpenidConfiguration() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer()
                .withOpenidConfigurationContents("BADLY FORMATTED JSON")
                .withURL("https://example.org/")
                .withKey("key1", rsa256Keys()));

        // FIXME suppress logging
        whenAuthenticatingWith(aJwtToken()
                .withRandomJti()
                .withRandomSub()
                .withClaim("scope", "read:/")
                .withClaim("exp", Instant.now().plus(10, MINUTES))
                .issuedBy("OP1").usingKey("key1"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectBadlyFormattedJwt() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer()
                .withJwtContents("BADLY FORMATTED JSON")
                .withURL("https://example.org/")
                .withKey("key1", rsa256Keys()));

        // FIXME suppress logging
        whenAuthenticatingWith(aJwtToken()
                .withRandomJti()
                .withRandomSub()
                .withClaim("scope", "read:/")
                .withClaim("exp", Instant.now().plus(10, MINUTES))
                .issuedBy("OP1").usingKey("key1"));
    }

    @Test
    public void shouldAcceptNonExpiredJwt() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        String sub = UUID.randomUUID().toString();
        String jti = UUID.randomUUID().toString();
        whenAuthenticatingWith(aJwtToken()
                .withClaim("jti", jti)
                .withClaim("sub", sub)
                .withClaim("scope", "read:/")
                .withClaim("exp", Instant.now().plus(10, MINUTES))
                .issuedBy("OP1").usingKey("key1"));

        assertThat(identifiedPrincipals, hasItems(new UidPrincipal(1000),
                new GidPrincipal(1000, true), new JwtSubPrincipal("EXAMPLE", sub),
                new JwtJtiPrincipal("EXAMPLE", jti)));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectNotYetValidJwt() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        whenAuthenticatingWith(aJwtToken()
                .withRandomJti()
                .withRandomSub()
                .withClaim("scope", "read:/")
                .withClaim("nbf", Instant.now().plus(10, MINUTES))
                .issuedBy("OP1").usingKey("key1"));
    }

    @Test
    public void shouldAcceptAlreadyValidJwt() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        String sub = UUID.randomUUID().toString();
        String jti = UUID.randomUUID().toString();
        whenAuthenticatingWith(aJwtToken()
                .withClaim("jti", jti)
                .withClaim("sub", sub)
                .withClaim("scope", "read:/")
                .withClaim("nbf", Instant.now().minus(10, MINUTES))
                .issuedBy("OP1").usingKey("key1"));

        assertThat(identifiedPrincipals, hasItems(new UidPrincipal(1000),
                new GidPrincipal(1000, true), new JwtSubPrincipal("EXAMPLE", sub),
                new JwtJtiPrincipal("EXAMPLE", jti)));
    }

    @Test
    public void shouldAcceptJwtWithoutAudience() throws Exception
    {
        given(aSciTokenPlugin()
                .withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000")
                .withProperty("gplazma.scitoken.audience-targets", "this-service"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        String sub = UUID.randomUUID().toString();
        String jti = UUID.randomUUID().toString();
        whenAuthenticatingWith(aJwtToken()
                .withClaim("jti", jti)
                .withClaim("sub", sub)
                .withClaim("scope", "read:/")
                .issuedBy("OP1").usingKey("key1"));

        assertThat(identifiedPrincipals, hasItems(new UidPrincipal(1000),
                new GidPrincipal(1000, true), new JwtSubPrincipal("EXAMPLE", sub),
                new JwtJtiPrincipal("EXAMPLE", jti)));
    }


    @Test(expected=AuthenticationException.class)
    public void shouldRejectJwtWithSpecificAudience() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        whenAuthenticatingWith(aJwtToken()
                .withRandomJti()
                .withRandomSub()
                .withClaim("scope", "read:/")
                .withClaim("aud", "some-other-service")
                .issuedBy("OP1").usingKey("key1"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectJwtWithWrongAudience() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000")
                        .withProperty("gplazma.scitoken.audience-targets", "this-service"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        whenAuthenticatingWith(aJwtToken()
                .withRandomJti()
                .withRandomSub()
                .withClaim("scope", "read:/")
                .withClaim("aud", "some-other-service")
                .issuedBy("OP1").usingKey("key1"));
    }

    @Test
    public void shouldAcceptJwtWithMatchingSingleAudience() throws Exception
    {
        given(aSciTokenPlugin()
                .withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000")
                .withProperty("gplazma.scitoken.audience-targets", "this-service"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        String sub = UUID.randomUUID().toString();
        String jti = UUID.randomUUID().toString();
        whenAuthenticatingWith(aJwtToken()
                .withClaim("jti", jti)
                .withClaim("sub", sub)
                .withClaim("scope", "read:/")
                .withClaim("aud", "this-service")
                .issuedBy("OP1").usingKey("key1"));

        assertThat(identifiedPrincipals, hasItems(new UidPrincipal(1000),
                new GidPrincipal(1000, true), new JwtSubPrincipal("EXAMPLE", sub),
                new JwtJtiPrincipal("EXAMPLE", jti)));
    }

    @Test
    public void shouldAcceptJwtWithMatchingSingleAudienceOfMany() throws Exception
    {
        given(aSciTokenPlugin()
                .withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000")
                .withProperty("gplazma.scitoken.audience-targets", "another-name this-service that-service"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        String sub = UUID.randomUUID().toString();
        String jti = UUID.randomUUID().toString();
        whenAuthenticatingWith(aJwtToken()
                .withClaim("jti", jti)
                .withClaim("sub", sub)
                .withClaim("scope", "read:/")
                .withClaim("aud", "this-service")
                .issuedBy("OP1").usingKey("key1"));

        assertThat(identifiedPrincipals, hasItems(new UidPrincipal(1000),
                new GidPrincipal(1000, true), new JwtSubPrincipal("EXAMPLE", sub),
                new JwtJtiPrincipal("EXAMPLE", jti)));
    }

    @Test
    public void shouldAcceptJwtWithMultipleAudienceOneOfWhichMatches() throws Exception
    {
        given(aSciTokenPlugin()
                .withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000")
                .withProperty("gplazma.scitoken.audience-targets", "this-service"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        String sub = UUID.randomUUID().toString();
        String jti = UUID.randomUUID().toString();
        whenAuthenticatingWith(aJwtToken()
                .withClaim("jti", jti)
                .withClaim("sub", sub)
                .withClaim("scope", "read:/")
                .withArrayClaim("aud", "that-service", "this-service", "the-other-service")
                .issuedBy("OP1").usingKey("key1"));

        assertThat(identifiedPrincipals, hasItems(new UidPrincipal(1000),
                new GidPrincipal(1000, true), new JwtSubPrincipal("EXAMPLE", sub),
                new JwtJtiPrincipal("EXAMPLE", jti)));

    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectJwtWithoutSciTokenSubClaim() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        whenAuthenticatingWith(aJwtToken()
                .withRandomJti()
                .withRandomSub()
                .withClaim("scope", "email openid")
                .issuedBy("OP1").usingKey("key1"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectJwtWithNeitherSubNorJtiClaim() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        whenAuthenticatingWith(aJwtToken()
                .withClaim("scope", "read:/")
                .issuedBy("OP1").usingKey("key1"));
    }

    @Test
    public void shouldAcceptJwtWithSubClaimAndWithoutJtiClaim() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        String sub = UUID.randomUUID().toString();
        whenAuthenticatingWith(aJwtToken()
                .withClaim("sub", sub)
                .withClaim("scope", "read:/")
                .issuedBy("OP1").usingKey("key1"));

        assertThat(identifiedPrincipals, hasItems(new UidPrincipal(1000),
                new GidPrincipal(1000, true), new JwtSubPrincipal("EXAMPLE", sub)));
    }

    @Test
    public void shouldAcceptJwtWithJtiClaimAndWithoutSubClaim() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        String jti = UUID.randomUUID().toString();
        whenAuthenticatingWith(aJwtToken()
                .withClaim("jti", jti)
                .withClaim("scope", "read:/")
                .issuedBy("OP1").usingKey("key1"));

        assertThat(identifiedPrincipals, hasItems(new UidPrincipal(1000),
                new GidPrincipal(1000, true), new JwtJtiPrincipal("EXAMPLE", jti)));
    }

    @Test
    public void shouldAllowOPFinalSlashOmission() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/foo /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer()
                .withURL("https://example.org/foo")
                .withConfigurationPath("/foo/.well-known/openid-configuration")
                .withJsonWebKeyPath("/foo/jwk")
                .withKey("key1", rsa256Keys()));

        String sub = UUID.randomUUID().toString();
        String jti = UUID.randomUUID().toString();
        whenAuthenticatingWith(aJwtToken()
                .withClaim("jti", jti)
                .withClaim("sub", sub)
                .withClaim("scope", "read:/")
                .issuedBy("OP1").usingKey("key1"));

        assertThat(identifiedPrincipals, hasItems(new UidPrincipal(1000),
                new GidPrincipal(1000, true), new JwtSubPrincipal("EXAMPLE", sub),
                new JwtJtiPrincipal("EXAMPLE", jti)));
        assertFalse(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/path/file.dat")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix/path")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.ROOT));
        assertTrue(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/path/new-file.dat")));
        assertTrue(resultingRestriction.isRestricted(DELETE, FsPath.create("/prefix/path/file.dat")));
        assertTrue(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/different-file.dat")));
    }

    /* Tests targeting SciToken claims */

    @Test
    public void shouldAllowReadOnlyAccess() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        String sub = UUID.randomUUID().toString();
        String jti = UUID.randomUUID().toString();
        whenAuthenticatingWith(aJwtToken()
                .withClaim("jti", jti)
                .withClaim("sub", sub)
                .withClaim("scope", "read:/")
                .issuedBy("OP1").usingKey("key1"));

        assertThat(identifiedPrincipals, hasItems(new UidPrincipal(1000),
                new GidPrincipal(1000, true), new JwtSubPrincipal("EXAMPLE", sub),
                new JwtJtiPrincipal("EXAMPLE", jti)));
        assertFalse(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/path/file.dat")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix/path")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.ROOT));
        assertTrue(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/path/new-file.dat")));
        assertTrue(resultingRestriction.isRestricted(DELETE, FsPath.create("/prefix/path/file.dat")));
        assertTrue(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/different-file.dat")));
    }

    @Test
    public void shouldAllowReadOnlyAccessForSubPath() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        String sub = UUID.randomUUID().toString();
        String jti = UUID.randomUUID().toString();
        whenAuthenticatingWith(aJwtToken()
                .withClaim("jti", jti)
                .withClaim("sub", sub)
                .withClaim("scope", "read:/scope-path")
                .issuedBy("OP1").usingKey("key1"));

        assertThat(identifiedPrincipals, hasItems(new UidPrincipal(1000),
                new GidPrincipal(1000, true), new JwtSubPrincipal("EXAMPLE", sub),
                new JwtJtiPrincipal("EXAMPLE", jti)));
        assertFalse(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/scope-path/file.dat")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix/scope-path")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.ROOT));
        assertTrue(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/scope-path/new-file.dat")));
        assertTrue(resultingRestriction.isRestricted(DELETE, FsPath.create("/prefix/scope-path/file.dat")));
        assertTrue(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/non-scope-path/different-file.dat")));
        assertTrue(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/different-file.dat")));
    }

    @Test
    public void shouldAllowReadOnlyAccessForTwoSubPath() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        String sub = UUID.randomUUID().toString();
        String jti = UUID.randomUUID().toString();
        whenAuthenticatingWith(aJwtToken()
                .withClaim("jti", jti)
                .withClaim("sub", sub)
                .withClaim("scope", "read:/scope-path1 read:/scope-path2")
                .issuedBy("OP1").usingKey("key1"));

        assertThat(identifiedPrincipals, hasItems(new UidPrincipal(1000),
                new GidPrincipal(1000, true), new JwtSubPrincipal("EXAMPLE", sub),
                new JwtJtiPrincipal("EXAMPLE", jti)));
        assertFalse(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/scope-path1/file.dat")));
        assertFalse(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/scope-path2/file.dat")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix/scope-path1")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix/scope-path2")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.ROOT));
        assertTrue(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/scope-path1/new-file.dat")));
        assertTrue(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/scope-path2/new-file.dat")));
        assertTrue(resultingRestriction.isRestricted(DELETE, FsPath.create("/prefix/scope-path1/file.dat")));
        assertTrue(resultingRestriction.isRestricted(DELETE, FsPath.create("/prefix/scope-path2/file.dat")));
        assertTrue(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/non-scope-path/different-file.dat")));
        assertTrue(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/different-file.dat")));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectOnlyScopeHasRelativePath() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        whenAuthenticatingWith(aJwtToken()
                .withRandomJti()
                .withRandomSub()
                .withClaim("scope", "read:foo")
                .issuedBy("OP1").usingKey("key1"));
    }

    @Test
    public void shouldIgnoreReadScopeWithoutAbsolutePath() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        String sub = UUID.randomUUID().toString();
        String jti = UUID.randomUUID().toString();
        whenAuthenticatingWith(aJwtToken()
                .withClaim("jti", jti)
                .withClaim("sub", sub)
                .withClaim("scope", "read:foo read:/")
                .issuedBy("OP1").usingKey("key1"));

        assertThat(identifiedPrincipals, hasItems(new UidPrincipal(1000),
                new GidPrincipal(1000, true), new JwtSubPrincipal("EXAMPLE", sub),
                new JwtJtiPrincipal("EXAMPLE", jti)));
        assertFalse(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/path/file.dat")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix/path")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.ROOT));
        assertTrue(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/path/new-file.dat")));
        assertTrue(resultingRestriction.isRestricted(DELETE, FsPath.create("/prefix/path/file.dat")));
        assertTrue(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/different-file.dat")));
    }

    @Test
    public void shouldAllowReadOnlyAccessWithNoPath() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        String sub = UUID.randomUUID().toString();
        String jti = UUID.randomUUID().toString();
        whenAuthenticatingWith(aJwtToken()
                .withClaim("jti", jti)
                .withClaim("sub", sub)
                .withClaim("scope", "read")
                .issuedBy("OP1").usingKey("key1"));

        assertThat(identifiedPrincipals, hasItems(new UidPrincipal(1000),
                new GidPrincipal(1000, true), new JwtSubPrincipal("EXAMPLE", sub),
                new JwtJtiPrincipal("EXAMPLE", jti)));
        assertFalse(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/path/file.dat")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix/path")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.ROOT));
        assertTrue(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/path/new-file.dat")));
        assertTrue(resultingRestriction.isRestricted(DELETE, FsPath.create("/prefix/path/file.dat")));
        assertTrue(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/different-file.dat")));
    }

    @Test
    public void shouldAllowReadOnlyAccessWithPrefixScope() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        String sub = UUID.randomUUID().toString();
        String jti = UUID.randomUUID().toString();
        whenAuthenticatingWith(aJwtToken()
                .withClaim("jti", jti)
                .withClaim("sub", sub)
                .withClaim("scope", "https://scitokens.org/v1/authz/read")
                .issuedBy("OP1").usingKey("key1"));

        assertThat(identifiedPrincipals, hasItems(new UidPrincipal(1000),
                new GidPrincipal(1000, true), new JwtSubPrincipal("EXAMPLE", sub),
                new JwtJtiPrincipal("EXAMPLE", jti)));
        assertFalse(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/path/file.dat")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix/path")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.ROOT));
        assertTrue(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/path/new-file.dat")));
        assertTrue(resultingRestriction.isRestricted(DELETE, FsPath.create("/prefix/path/file.dat")));
        assertTrue(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/different-file.dat")));
    }

    @Test
    public void shouldDenyAllAccessForQueueExecuteScope() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        String sub = UUID.randomUUID().toString();
        String jti = UUID.randomUUID().toString();
        whenAuthenticatingWith(aJwtToken()
                .withClaim("jti", jti)
                .withClaim("sub", sub)
                .withClaim("scope", "queue execute")
                .issuedBy("OP1").usingKey("key1"));

        assertThat(identifiedPrincipals, hasItems(new UidPrincipal(1000),
                new GidPrincipal(1000, true), new JwtSubPrincipal("EXAMPLE", sub),
                new JwtJtiPrincipal("EXAMPLE", jti)));
        assertTrue(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/path/file.dat")));
        assertTrue(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix/path")));
        assertTrue(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix")));
        assertTrue(resultingRestriction.isRestricted(LIST, FsPath.ROOT));
        assertTrue(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/path/new-file.dat")));
        assertTrue(resultingRestriction.isRestricted(DELETE, FsPath.create("/prefix/path/file.dat")));
        assertTrue(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/different-file.dat")));
    }

    @Test
    public void shouldAllowFirstJwtUsage() throws Exception
    {
        given(aSciTokenPlugin()
                .withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000 -tokenHistory=10"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        String sub = UUID.randomUUID().toString();
        String jti = UUID.randomUUID().toString();
        whenAuthenticatingWith(aJwtToken()
                .withClaim("jti", jti)
                .withClaim("sub", sub)
                .withClaim("scope", "queue execute")
                .issuedBy("OP1").usingKey("key1"));

        assertThat(identifiedPrincipals, hasItems(new UidPrincipal(1000),
                new GidPrincipal(1000, true), new JwtSubPrincipal("EXAMPLE", sub),
                new JwtJtiPrincipal("EXAMPLE", jti)));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectSecondJwtUsage() throws Exception
    {
        given(aSciTokenPlugin()
                .withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000 -tokenHistory=10"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        String sub = UUID.randomUUID().toString();
        String jti = UUID.randomUUID().toString();
        String jwt = aJwtToken()
                .withClaim("jti", jti)
                .withClaim("sub", sub)
                .withClaim("scope", "queue execute")
                .issuedBy("OP1").usingKey("key1").build();
        givenAlreadyAuthenticatedWith(jwt);

        whenAuthenticatingWith(jwt);
    }

    @Test
    public void shouldAllowWriteOnlyAccess() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        String sub = UUID.randomUUID().toString();
        String jti = UUID.randomUUID().toString();
        whenAuthenticatingWith(aJwtToken()
                .withClaim("jti", jti)
                .withClaim("sub", sub)
                .withClaim("scope", "write:/")
                .issuedBy("OP1").usingKey("key1"));

        assertThat(identifiedPrincipals, hasItems(new UidPrincipal(1000),
                new GidPrincipal(1000, true), new JwtSubPrincipal("EXAMPLE", sub),
                new JwtJtiPrincipal("EXAMPLE", jti)));
        assertFalse(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/path/file.dat")));
        assertFalse(resultingRestriction.isRestricted(DELETE, FsPath.create("/prefix/path/file.dat")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix/path")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.ROOT));
        assertTrue(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/path/file.dat")));
        assertTrue(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/file.dat")));
        assertTrue(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/file.dat")));
    }

    @Test
    public void shouldAllowReadWriteOnlyAccess() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        String sub = UUID.randomUUID().toString();
        String jti = UUID.randomUUID().toString();
        whenAuthenticatingWith(aJwtToken()
                .withClaim("jti", jti)
                .withClaim("sub", sub)
                .withClaim("scope", "write:/ read:/")
                .issuedBy("OP1").usingKey("key1"));

        assertThat(identifiedPrincipals, hasItems(new UidPrincipal(1000),
                new GidPrincipal(1000, true), new JwtSubPrincipal("EXAMPLE", sub),
                new JwtJtiPrincipal("EXAMPLE", jti)));
        assertFalse(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/path/new-file.dat")));
        assertFalse(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/path/file.dat")));
        assertFalse(resultingRestriction.isRestricted(DELETE, FsPath.create("/prefix/path/file.dat")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix/path")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.ROOT));
        assertTrue(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/different-file.dat")));
    }

    @Test
    public void shouldAllowGeneralReadTargetedWrite() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        String sub = UUID.randomUUID().toString();
        String jti = UUID.randomUUID().toString();
        whenAuthenticatingWith(aJwtToken()
                .withClaim("jti", jti)
                .withClaim("sub", sub)
                .withClaim("scope", "write:/home/paul read:/")
                .issuedBy("OP1").usingKey("key1"));

        assertThat(identifiedPrincipals, hasItems(new UidPrincipal(1000),
                new GidPrincipal(1000, true), new JwtSubPrincipal("EXAMPLE", sub),
                new JwtJtiPrincipal("EXAMPLE", jti)));
        assertFalse(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/path/home/paul/new-file.dat")));
        assertFalse(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/path/home/paul/file.dat")));
        assertFalse(resultingRestriction.isRestricted(DELETE, FsPath.create("/prefix/path/home/paul/file.dat")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix/path/home/paul")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix/path/home")));
        assertFalse(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/path/file.dat")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix/path")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.ROOT));
        assertTrue(resultingRestriction.isRestricted(DELETE, FsPath.create("/prefix/path/home/file.dat")));
        assertTrue(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/path/home/new-file.dat")));
        assertTrue(resultingRestriction.isRestricted(DELETE, FsPath.create("/prefix/path/file.dat")));
        assertTrue(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/path/new-file.dat")));
        assertTrue(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/file.dat")));
        assertTrue(resultingRestriction.isRestricted(DELETE, FsPath.create("/prefix/file.dat")));
        assertTrue(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/new-file.dat")));
        assertTrue(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/file.dat")));
        assertTrue(resultingRestriction.isRestricted(DELETE, FsPath.create("/file.dat")));
        assertTrue(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/new-file.dat")));
    }

    /* Tests for the WLCG Common profile. */

    @Test
    public void shouldAllowReadOnlyAccessWithWlcgClaim() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        String sub = UUID.randomUUID().toString();
        String jti = UUID.randomUUID().toString();
        whenAuthenticatingWith(aJwtToken()
                .withClaim("jti", jti)
                .withClaim("sub", sub)
                .withClaim("scope", "storage.read:/")
                .issuedBy("OP1").usingKey("key1"));

        assertThat(identifiedPrincipals, hasItems(new UidPrincipal(1000),
                new GidPrincipal(1000, true), new JwtSubPrincipal("EXAMPLE", sub),
                new JwtJtiPrincipal("EXAMPLE", jti)));
        assertFalse(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/path/file.dat")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix/path")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.ROOT));
        assertTrue(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/path/new-file.dat")));
        assertTrue(resultingRestriction.isRestricted(DELETE, FsPath.create("/prefix/path/file.dat")));
        assertTrue(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/different-file.dat")));
    }

    @Test
    public void shouldAllowReadOnlyAccessWithWlcgClaimWithNonRootPath() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        String sub = UUID.randomUUID().toString();
        String jti = UUID.randomUUID().toString();
        whenAuthenticatingWith(aJwtToken()
                .withClaim("jti", jti)
                .withClaim("sub", sub)
                .withClaim("scope", "storage.read:/data/general")
                .issuedBy("OP1").usingKey("key1"));

        assertThat(identifiedPrincipals, hasItems(new UidPrincipal(1000),
                new GidPrincipal(1000, true), new JwtSubPrincipal("EXAMPLE", sub),
                new JwtJtiPrincipal("EXAMPLE", jti)));
        assertFalse(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/path/data/general/file.dat")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix/path/data/general")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix/path/data")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix/path")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.ROOT));
        assertTrue(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/path/data/file.dat")));
        assertTrue(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/path/file.dat")));
        assertTrue(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/file.dat")));
        assertTrue(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/file.dat")));
        assertTrue(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/path/data/general/file.dat")));
        assertTrue(resultingRestriction.isRestricted(DELETE, FsPath.create("/prefix/path/data/general/file.dat")));
    }

    @Test
    public void shouldAllowModifyOnlyAccessWithWlcgClaim() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        String sub = UUID.randomUUID().toString();
        String jti = UUID.randomUUID().toString();
        whenAuthenticatingWith(aJwtToken()
                .withClaim("jti", jti)
                .withClaim("sub", sub)
                .withClaim("scope", "storage.modify:/")
                .issuedBy("OP1").usingKey("key1"));

        assertThat(identifiedPrincipals, hasItems(new UidPrincipal(1000),
                new GidPrincipal(1000, true), new JwtSubPrincipal("EXAMPLE", sub),
                new JwtJtiPrincipal("EXAMPLE", jti)));
        assertFalse(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/path/file.dat")));
        assertFalse(resultingRestriction.isRestricted(MANAGE, FsPath.create("/prefix/path/file.dat")));
        assertFalse(resultingRestriction.isRestricted(DELETE, FsPath.create("/prefix/path/file.dat")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix/path")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.ROOT));
        assertTrue(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/path/file.dat")));
        assertTrue(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/file.dat")));
        assertTrue(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/file.dat")));
        assertTrue(resultingRestriction.isRestricted(MANAGE, FsPath.create("/prefix/file.dat")));
        assertTrue(resultingRestriction.isRestricted(DELETE, FsPath.create("/file.dat")));
    }

    @Test
    public void shouldAllowModifyOnlyAccessWithWlcgClaimWithNonRootPath() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        String sub = UUID.randomUUID().toString();
        String jti = UUID.randomUUID().toString();
        whenAuthenticatingWith(aJwtToken()
                .withClaim("jti", jti)
                .withClaim("sub", sub)
                .withClaim("scope", "storage.modify:/data/general")
                .issuedBy("OP1").usingKey("key1"));

        assertThat(identifiedPrincipals, hasItems(new UidPrincipal(1000),
                new GidPrincipal(1000, true), new JwtSubPrincipal("EXAMPLE", sub),
                new JwtJtiPrincipal("EXAMPLE", jti)));
        assertFalse(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/path/data/general/file.dat")));
        assertFalse(resultingRestriction.isRestricted(DELETE, FsPath.create("/prefix/path/data/general/file.dat")));
        assertFalse(resultingRestriction.isRestricted(MANAGE, FsPath.create("/prefix/path/data/general/file.dat")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix/path/data/general")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix/path/data")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix/path")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.ROOT));
        assertTrue(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/path/data/general/file.dat")));
        assertTrue(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/path/data/file.dat")));
        assertTrue(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/path/file.dat")));
        assertTrue(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/file.dat")));
        assertTrue(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/file.dat")));
        assertTrue(resultingRestriction.isRestricted(DELETE, FsPath.create("/prefix/path/data/file.dat")));
        assertTrue(resultingRestriction.isRestricted(DELETE, FsPath.create("/prefix/path/file.dat")));
        assertTrue(resultingRestriction.isRestricted(DELETE, FsPath.create("/prefix/file.dat")));
        assertTrue(resultingRestriction.isRestricted(DELETE, FsPath.create("/file.dat")));
        assertTrue(resultingRestriction.isRestricted(MANAGE, FsPath.create("/prefix/path/data/file.dat")));
        assertTrue(resultingRestriction.isRestricted(MANAGE, FsPath.create("/prefix/path/file.dat")));
        assertTrue(resultingRestriction.isRestricted(MANAGE, FsPath.create("/prefix/file.dat")));
        assertTrue(resultingRestriction.isRestricted(MANAGE, FsPath.create("/file.dat")));
    }

    @Test
    public void shouldAllowCreateOnlyAccessWithWlcgClaim() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        String sub = UUID.randomUUID().toString();
        String jti = UUID.randomUUID().toString();
        whenAuthenticatingWith(aJwtToken()
                .withClaim("jti", jti)
                .withClaim("sub", sub)
                .withClaim("scope", "storage.create:/")
                .issuedBy("OP1").usingKey("key1"));

        assertThat(identifiedPrincipals, hasItems(new UidPrincipal(1000),
                new GidPrincipal(1000, true), new JwtSubPrincipal("EXAMPLE", sub),
                new JwtJtiPrincipal("EXAMPLE", jti)));
        assertFalse(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/path/file.dat")));
        assertFalse(resultingRestriction.isRestricted(MANAGE, FsPath.create("/prefix/path/file.dat")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix/path")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.ROOT));
        assertTrue(resultingRestriction.isRestricted(DELETE, FsPath.create("/prefix/path/file.dat")));
        assertTrue(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/path/file.dat")));
        assertTrue(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/file.dat")));
        assertTrue(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/file.dat")));
        assertTrue(resultingRestriction.isRestricted(MANAGE, FsPath.create("/prefix/file.dat")));
        assertTrue(resultingRestriction.isRestricted(MANAGE, FsPath.create("/file.dat")));
    }

    @Test
    public void shouldAllowCreateOnlyAccessWithWlcgClaimWithNonRootPath() throws Exception
    {
        given(aSciTokenPlugin().withProperty("gplazma.scitoken.issuer!EXAMPLE", "https://example.org/ /prefix/path uid:1000 gid:1000"));
        givenThat("OP1", isAnIssuer().withURL("https://example.org/").withKey("key1", rsa256Keys()));

        String sub = UUID.randomUUID().toString();
        String jti = UUID.randomUUID().toString();
        whenAuthenticatingWith(aJwtToken()
                .withClaim("jti", jti)
                .withClaim("sub", sub)
                .withClaim("scope", "storage.create:/data/general")
                .issuedBy("OP1").usingKey("key1"));

        assertThat(identifiedPrincipals, hasItems(new UidPrincipal(1000),
                new GidPrincipal(1000, true), new JwtSubPrincipal("EXAMPLE", sub),
                new JwtJtiPrincipal("EXAMPLE", jti)));
        assertFalse(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/path/data/general/file.dat")));
        assertFalse(resultingRestriction.isRestricted(MANAGE, FsPath.create("/prefix/path/data/general/file.dat")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix/path/data/general")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix/path/data")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix/path")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.create("/prefix")));
        assertFalse(resultingRestriction.isRestricted(LIST, FsPath.ROOT));
        assertTrue(resultingRestriction.isRestricted(DOWNLOAD, FsPath.create("/prefix/path/data/general/file.dat")));
        assertTrue(resultingRestriction.isRestricted(DELETE, FsPath.create("/prefix/path/data/general/file.dat")));
        assertTrue(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/path/data/file.dat")));
        assertTrue(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/path/file.dat")));
        assertTrue(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/prefix/file.dat")));
        assertTrue(resultingRestriction.isRestricted(UPLOAD, FsPath.create("/file.dat")));
        assertTrue(resultingRestriction.isRestricted(MANAGE, FsPath.create("/prefix/path/data/file.dat")));
        assertTrue(resultingRestriction.isRestricted(MANAGE, FsPath.create("/prefix/path/file.dat")));
        assertTrue(resultingRestriction.isRestricted(MANAGE, FsPath.create("/prefix/file.dat")));
        assertTrue(resultingRestriction.isRestricted(MANAGE, FsPath.create("/file.dat")));
    }

    private void whenAuthenticatingWith(PrincipalSetMaker maker) throws AuthenticationException
    {
        identifiedPrincipals.addAll(maker.build());
        plugin.authenticate(Collections.emptySet(), Collections.emptySet(),
                identifiedPrincipals, restrictions);
        resultingRestriction = Restrictions.concat(restrictions);
    }

    private void whenAuthenticatingWith(JwtBuilder... builder) throws AuthenticationException
    {
        Set<Object> privateCredentials = Arrays.stream(builder)
                .map(JwtBuilder::build)
                .map(BearerTokenCredential::new)
                .collect(Collectors.toSet());
        plugin.authenticate(Collections.emptySet(), privateCredentials,
                identifiedPrincipals, restrictions);
        resultingRestriction = Restrictions.concat(restrictions);
    }

    private void whenAuthenticatingWith(String jwtToken) throws AuthenticationException
    {
        plugin.authenticate(Collections.emptySet(),
                Collections.singleton(new BearerTokenCredential(jwtToken)),
                identifiedPrincipals, restrictions);
        resultingRestriction = Restrictions.concat(restrictions);
    }

    private SciTokenPluginBuilder aSciTokenPlugin()
    {
        return new SciTokenPluginBuilder();
    }

    private IssuerInfoBuilder isAnIssuer()
    {
        return new IssuerInfoBuilder();
    }

    private JwtBuilder aJwtToken()
    {
        return new JwtBuilder();
    }

    private PrincipalSetMaker principals()
    {
        return new PrincipalSetMaker();
    }

    private void given(SciTokenPluginBuilder builder)
    {
        plugin = builder.build();
    }

    private void givenAlreadyAuthenticatedWith(String jwtToken)
    {
        try {
            plugin.authenticate(Collections.emptySet(),
                    Collections.singleton(new BearerTokenCredential(jwtToken)),
                    new HashSet<>(), new HashSet<>());
        } catch (AuthenticationException e) {
            throw new RuntimeException("Expected authentication failed: " + e, e);
        }
    }


    private void givenThat(String id, IssuerInfoBuilder builder)
    {
        IssuerInfo issuer = builder.build();
        issuer.mockResponses(client);
        signers.put(id, issuer);
    }

    private class SciTokenPluginBuilder
    {
        private final Properties properties = new Properties();

        private SciTokenPluginBuilder()
        {
            // Add default values
            properties.setProperty("gplazma.scitoken.token-history", "0");
            properties.setProperty("gplazma.scitoken.audience-targets", "");
        }

        public SciTokenPluginBuilder withProperty(String key, String value)
        {
            properties.setProperty(key, value);
            return this;
        }

        public SciTokenPlugin build()
        {
            return new SciTokenPlugin(properties, client);
        }
    }

    private class IssuerInfoBuilder
    {
        private final Map<String,KeyMaterial> keys = new HashMap<>();
        private String url;
        private String jwkPath = "/jwk";
        private Optional<String> jwtContents = Optional.empty();
        private String configurationPath = "/.well-known/openid-configuration";
        private Optional<String> configurationContents = Optional.empty();

        public IssuerInfoBuilder withURL(String url)
        {
            this.url = requireNonNull(url);
            return this;
        }

        public IssuerInfoBuilder withKey(String id, KeyMaterial material)
        {
            keys.put(id, material);
            return this;
        }

        public IssuerInfoBuilder withConfigurationPath(String path)
        {
            configurationPath = path;
            return this;
        }

        public IssuerInfoBuilder withJsonWebKeyPath(String path)
        {
            jwkPath = path;
            return this;
        }

        public IssuerInfoBuilder withOpenidConfigurationContents(String data)
        {
            configurationContents = Optional.ofNullable(data);
            return this;
        }

        public IssuerInfoBuilder withJwtContents(String data)
        {
            jwtContents = Optional.ofNullable(data);
            return this;
        }

        public IssuerInfo build()
        {
            return new IssuerInfo(url, keys, configurationPath, configurationContents,
                    jwkPath, jwtContents);
        }
    }

    private KeyMaterial rsa256Keys()
    {
        return new KeyMaterial("RSA", "RS256", PUBLIC_KEY, PRIVATE_KEY);
    }

    private class KeyMaterial
    {
        private final String type;
        private final String alg;
        private final PublicKey publicKey;
        private final PrivateKey privateKey;

        public KeyMaterial(String type, String alg, PublicKey publicKey, PrivateKey privateKey)
        {
            this.type = type;
            this.alg = alg;
            this.publicKey = publicKey;
            this.privateKey = privateKey;
        }

        public String sign(String input)
        {
            byte[] data = input.getBytes(StandardCharsets.UTF_8);
            try {
                Signature sig = Signature.getInstance("SHA256withRSA", "BC");
                sig.initSign(privateKey);
                sig.update(data);
                byte[] signature = sig.sign();
                return BASE64_ENCODER.encodeToString(signature);
            } catch (NoSuchAlgorithmException | NoSuchProviderException
                    | SignatureException | InvalidKeyException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

        private String encode(BigInteger i)
        {
            return BASE64_ENCODER.encodeToString(i.toByteArray());
        }

        public void addToJwt(ObjectNode json)
        {
            json.put("kty", type);
            RSAPublicKey rsa = (RSAPublicKey) publicKey;
            json.put("e", encode(rsa.getPublicExponent()));
            json.put("n", encode(rsa.getModulus()));
        }
    }

    private class HttpGetMatcher implements ArgumentMatcher<HttpUriRequest>
    {
        private final URI uri;

        private HttpGetMatcher(URI uri)
        {
            this.uri = requireNonNull(uri);
        }

        @Override
        public boolean matches(HttpUriRequest t)
        {
            return t != null && t.getMethod() != null && t.getURI() != null
                    && t.getMethod().equals("GET") && t.getURI().equals(uri);
        }
    }

    private ResponseBuilder aResponse()
    {
        return new ResponseBuilder();
    }

    private class ResponseBuilder
    {
        private final HttpResponse response = mock(HttpResponse.class);
        private final Map<String,String> headers = new HashMap<>();

        public ResponseBuilder withEntity(String value)
        {
            byte[] entity = value.getBytes(StandardCharsets.UTF_8);

            try {
                HttpEntity httpEntity = mock(HttpEntity.class);
                when(httpEntity.getContent()).thenReturn(new ByteArrayInputStream(entity));
                when(httpEntity.getContentLength()).thenReturn((long)entity.length);
                doAnswer(i -> {
                        OutputStream stream = (OutputStream) i.getArgument(0);
                        stream.write(entity);
                        return null;
                    }).when(httpEntity).writeTo(any(OutputStream.class));

                when(response.getEntity()).thenReturn(httpEntity);
                return this;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public ResponseBuilder withHeader(String key, String value)
        {
            headers.put(key, value);
            return this;
        }

        public HttpResponse build()
        {
            for (Map.Entry<String,String> e : headers.entrySet()) {
                Header header = mock(Header.class);
                when(header.getName()).thenReturn(e.getKey());
                when(header.getValue()).thenReturn(e.getValue());
                when(response.getFirstHeader(eq(e.getKey()))).thenReturn(header);
                when(response.getLastHeader(eq(e.getKey()))).thenReturn(header);
            }
            return response;
        }
    }

    private class IssuerInfo
    {
        private final URI base;
        private final Map<String, KeyMaterial> keys;
        private final String keyPath;
        private final String configurationPath;
        private final Optional<String> openidConfig;
        private final Optional<String> jwtContents;

        public IssuerInfo(String base, Map<String, KeyMaterial> keys,
                String configurationPath, Optional<String> openidConfig,
                String jwkPath, Optional<String> jwtContents)
        {
            this.base = URI.create(base);
            this.keys = keys;
            this.keyPath = jwkPath;
            this.configurationPath = configurationPath;
            this.openidConfig = openidConfig;
            this.jwtContents = jwtContents;
        }

        public KeyMaterial getKey(String kid)
        {
            return keys.get(kid);
        }

        private String buildOpenidConfiguration()
        {
            ObjectNode json = MAPPER.createObjectNode();
            json.put("jwks_uri", base.resolve(keyPath).toASCIIString());
            return json.toString();
        }

        private String buildJwk()
        {
            ObjectNode json = MAPPER.createObjectNode();

            ArrayNode keysArray = json.putArray("keys");
            for (Map.Entry<String, KeyMaterial> key : keys.entrySet()) {
                ObjectNode keyInfo = keysArray.addObject();
                keyInfo.put("kid", key.getKey());
                key.getValue().addToJwt(keyInfo);
            }

            return json.toString();
        }

        private void mockReply(HttpClient client, String path, ResponseBuilder builder)
        {
            URI requestUri = base.resolve(path);
            try {
                HttpResponse response = builder.build();
                when(client.execute(argThat(new HttpGetMatcher(requestUri)))).thenReturn(response);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void mockResponses(HttpClient client)
        {
            String configEntity = openidConfig.orElseGet(this::buildOpenidConfiguration);
            mockReply(client, configurationPath, aResponse().withEntity(configEntity));

            String jwkEntity = jwtContents.orElseGet(this::buildJwk);
            mockReply(client, keyPath, aResponse().withEntity(jwkEntity));
        }
    }

    private class JwtBuilder
    {
        private final Map<String,Object> claims = new HashMap<>();
        private String payload;
        private IssuerInfo issuer;
        private String kid;

        public JwtBuilder withPayload(String payload)
        {
            this.payload = payload;
            return this;
        }

        public JwtBuilder withClaim(String key, String value)
        {
            claims.put(key, value);
            return this;
        }

        public JwtBuilder withArrayClaim(String key, String... values)
        {
            claims.put(key, Arrays.asList(values));
            return this;
        }

        public JwtBuilder withClaim(String key, Instant value)
        {
            claims.put(key, value.getEpochSecond());
            return this;
        }

        public JwtBuilder withRandomSub()
        {
            return withClaim("sub", UUID.randomUUID().toString());
        }

        public JwtBuilder withRandomJti()
        {
            return withClaim("jti", UUID.randomUUID().toString());
        }

        public JwtBuilder issuedBy(String id)
        {
            issuer = requireNonNull(signers.get(id));
            withClaim("iss", issuer.base.toASCIIString());
            return this;
        }

        public JwtBuilder usingKey(String id)
        {
            kid = requireNonNull(id);
            return this;
        }

        public JwtBuilder omittingClaim(String key)
        {
            claims.remove(key);
            return this;
        }

        private String base64Encoded(String json)
        {
            return BASE64_ENCODER.encodeToString(json.getBytes(StandardCharsets.UTF_8));
        }

        private String header()
        {
            ObjectNode json = MAPPER.createObjectNode();
            json.put("kid", kid);
            KeyMaterial keys = requireNonNull(issuer.getKey(kid), "Unknown kid");
            json.put("alg", keys.alg);
            return json.toString();
        }

        private String payload()
        {
            if (payload == null) {
                ObjectNode json = MAPPER.createObjectNode();
                for (Map.Entry<String,Object> e : claims.entrySet()) {
                    String key = e.getKey();
                    Object value = e.getValue();
                    if (value instanceof List) {
                        ArrayNode array = json.putArray(key);
                        ((List<String>)value).forEach(array::add);
                    } else if (value instanceof Long) {
                        json.put(key, (Long)value);
                    } else {
                        json.put(key, String.valueOf(value));
                    }
                }
                payload = json.toString();
            }
            return payload;
        }

        public String build()
        {
            requireNonNull(issuer, "Missing issuedBy() call");
            requireNonNull(kid, "Missing usingKey() call");
            String encodedHeader = base64Encoded(header());
            String encodedPayload = base64Encoded(payload());

            String valueToBeSigned = encodedHeader + "." + encodedPayload;
            String signature = issuer.getKey(kid).sign(valueToBeSigned);
            return valueToBeSigned + "." + signature;
        }
    }
}
