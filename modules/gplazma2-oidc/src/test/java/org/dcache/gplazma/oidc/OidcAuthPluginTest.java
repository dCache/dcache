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
package org.dcache.gplazma.oidc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import diskCacheV111.util.FsPath;
import java.net.URI;
import java.security.Principal;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.dcache.auth.BearerTokenCredential;
import org.dcache.auth.EmailAddressPrincipal;
import org.dcache.auth.EntitlementPrincipal;
import org.dcache.auth.ExemptFromNamespaceChecks;
import org.dcache.auth.FullNamePrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.LoAPrincipal;
import org.dcache.auth.OidcSubjectPrincipal;
import org.dcache.auth.OpenIdGroupPrincipal;
import org.dcache.auth.PasswordCredential;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.auth.attributes.Restriction;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.oidc.profiles.WlcgProfile;
import org.dcache.gplazma.oidc.profiles.OidcProfile;
import org.dcache.gplazma.oidc.profiles.ScitokensProfile;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.BDDMockito;

import static com.google.common.base.Preconditions.checkState;
import static java.time.temporal.ChronoUnit.MINUTES;
import static org.dcache.gplazma.oidc.MockHttpClientBuilder.aClient;
import static org.dcache.gplazma.oidc.MockIdentityProviderBuilder.anIp;
import static org.dcache.gplazma.oidc.MockProfileBuilder.aProfile;
import static org.dcache.gplazma.oidc.MockProfileResultBuilder.aProfileResult;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;


public class OidcAuthPluginTest {

    private final JwtFactory jwtFactory = new JwtFactory();

    private OidcAuthPlugin plugin;
    private Set<Principal> principals;
    private Set<Restriction> restrictions;
    private TokenProcessor processor;
    private String jwt;

    @Before
    public void setup() {
        plugin = null;
        principals = null;
        restrictions = null;
        processor = null;
        jwt = null;
    }

    @Test
    public void shouldReturnEmptySetFromBuildProvidersWithNoPrefixEntries() throws Exception {
        Properties properties = new Properties();

        var identityProviders = OidcAuthPlugin.buildProviders(properties, aClient().build(),
                Duration.ofSeconds(2));

        assertThat(identityProviders, is(empty()));
    }

    @Test
    public void shouldReturnSingleIdentityProviderFromBuildProvidersWithSinglePrefixEntries() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("gplazma.oidc.provider!EXAMPLE", "https://oidc.example.org/");

        var identityProviders = OidcAuthPlugin.buildProviders(properties, aClient().build(),
                Duration.ofSeconds(2));

        assertThat(identityProviders, hasSize(1));
        IdentityProvider provider = identityProviders.iterator().next();
        assertThat(provider.getName(), is(equalTo("EXAMPLE")));
        assertThat(provider.getIssuerEndpoint(), is(equalTo(URI.create("https://oidc.example.org/"))));
        assertThat(provider.getProfile(), is(instanceOf(OidcProfile.class)));
        OidcProfile oidcProfile = (OidcProfile)provider.getProfile();
        assertThat(oidcProfile.isPreferredUsernameClaimAccepted(), is(equalTo(false)));
        assertThat(oidcProfile.isGroupsClaimMappedToGroupName(), is(equalTo(false)));
    }

    @Test
    public void shouldReturnSingleIdentityProviderFromBuildProvidersWithSinglePrefixEntriesWithOidcProfile() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("gplazma.oidc.provider!EXAMPLE", "https://oidc.example.org/ -profile=oidc");

        var identityProviders = OidcAuthPlugin.buildProviders(properties, aClient().build(),
                Duration.ofSeconds(2));

        assertThat(identityProviders, hasSize(1));
        IdentityProvider provider = identityProviders.iterator().next();
        assertThat(provider.getName(), is(equalTo("EXAMPLE")));
        assertThat(provider.getIssuerEndpoint(), is(equalTo(URI.create("https://oidc.example.org/"))));
        assertThat(provider.getProfile(), is(instanceOf(OidcProfile.class)));
        OidcProfile oidcProfile = (OidcProfile)provider.getProfile();
        assertThat(oidcProfile.isPreferredUsernameClaimAccepted(), is(equalTo(false)));
        assertThat(oidcProfile.isGroupsClaimMappedToGroupName(), is(equalTo(false)));
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectIdentityProviderWithUnknownProfile() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("gplazma.oidc.provider!EXAMPLE",
                "https://oidc.example.org/ -profile=unknown-profile");

        OidcAuthPlugin.buildProviders(properties, aClient().build(), Duration.ofSeconds(2));
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldThrowExceptionByBuildProvidersWithMissingName() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("gplazma.oidc.provider!", "https://oidc.example.org/");

        OidcAuthPlugin.buildProviders(properties, aClient().build(), Duration.ofSeconds(2));
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldThrowExceptionByBuildProvidersWithMissingURI() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("gplazma.oidc.provider!", "");

        OidcAuthPlugin.buildProviders(properties, aClient().build(), Duration.ofSeconds(2));
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldThrowExceptionByBuildProvidersWithBadAcceptValue() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("gplazma.oidc.provider!", "https://oidc.example.org/ -accept=BAD-VALUE");

        OidcAuthPlugin.buildProviders(properties, aClient().build(), Duration.ofSeconds(2));
    }

    @Test
    public void shouldReturnSingleIdentityProviderAcceptUsernameFromBuildProvidersWithSinglePrefixEntries() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("gplazma.oidc.provider!EXAMPLE", "https://oidc.example.org/ -accept=username");

        var identityProviders = OidcAuthPlugin.buildProviders(properties, aClient().build(),
                Duration.ofSeconds(2));

        assertThat(identityProviders, hasSize(1));
        IdentityProvider provider = identityProviders.iterator().next();
        assertThat(provider.getName(), is(equalTo("EXAMPLE")));
        assertThat(provider.getIssuerEndpoint(), is(equalTo(URI.create("https://oidc.example.org/"))));
        assertThat(provider.getProfile(), is(instanceOf(OidcProfile.class)));
        OidcProfile oidcProfile = (OidcProfile)provider.getProfile();
        assertThat(oidcProfile.isPreferredUsernameClaimAccepted(), is(equalTo(true)));
        assertThat(oidcProfile.isGroupsClaimMappedToGroupName(), is(equalTo(false)));
    }

    @Test
    public void shouldReturnSingleIdentityProviderAcceptGroupsFromBuildProvidersWithSinglePrefixEntries() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("gplazma.oidc.provider!EXAMPLE", "https://oidc.example.org/ -accept=groups");

        var identityProviders = OidcAuthPlugin.buildProviders(properties, aClient().build(),
                Duration.ofSeconds(2));

        assertThat(identityProviders, hasSize(1));
        IdentityProvider provider = identityProviders.iterator().next();
        assertThat(provider.getName(), is(equalTo("EXAMPLE")));
        assertThat(provider.getIssuerEndpoint(), is(equalTo(URI.create("https://oidc.example.org/"))));
        assertThat(provider.getProfile(), is(instanceOf(OidcProfile.class)));
        OidcProfile oidcProfile = (OidcProfile)provider.getProfile();
        assertThat(oidcProfile.isPreferredUsernameClaimAccepted(), is(equalTo(false)));
        assertThat(oidcProfile.isGroupsClaimMappedToGroupName(), is(equalTo(true)));
    }

    @Test
    public void shouldReturnSingleIdentityProviderAcceptUsernameAndGroupsFromBuildProvidersWithSinglePrefixEntries() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("gplazma.oidc.provider!EXAMPLE", "https://oidc.example.org/ -accept=username,groups");

        var identityProviders = OidcAuthPlugin.buildProviders(properties, aClient().build(),
                Duration.ofSeconds(2));

        assertThat(identityProviders, hasSize(1));
        IdentityProvider provider = identityProviders.iterator().next();
        assertThat(provider.getName(), is(equalTo("EXAMPLE")));
        assertThat(provider.getIssuerEndpoint(), is(equalTo(URI.create("https://oidc.example.org/"))));
        assertThat(provider.getProfile(), is(instanceOf(OidcProfile.class)));
        OidcProfile oidcProfile = (OidcProfile)provider.getProfile();
        assertThat(oidcProfile.isPreferredUsernameClaimAccepted(), is(equalTo(true)));
        assertThat(oidcProfile.isGroupsClaimMappedToGroupName(), is(equalTo(true)));
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectScitokensProfileWithoutPrefix() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("gplazma.oidc.provider!EXAMPLE",
                "https://oidc.example.org/ -profile=scitokens");

        OidcAuthPlugin.buildProviders(properties, aClient().build(), Duration.ofSeconds(2));
    }

    @Test
    public void shouldAcceptScitokensProfileWithPrefix() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("gplazma.oidc.provider!EXAMPLE",
                "https://oidc.example.org/ -profile=scitokens -prefix=/target");

        var identityProviders = OidcAuthPlugin.buildProviders(properties, aClient().build(),
                Duration.ofSeconds(2));

        assertThat(identityProviders, hasSize(1));
        IdentityProvider provider = identityProviders.iterator().next();
        assertThat(provider.getProfile(), is(instanceOf(ScitokensProfile.class)));
        ScitokensProfile scitokensProfile = (ScitokensProfile)provider.getProfile();
        assertThat(scitokensProfile.getPrefix(), is(equalTo(FsPath.create("/target"))));
        assertThat(scitokensProfile.getAuthzIdentity(), is(empty()));
        assertThat(scitokensProfile.getNonAuthzIdentity(), is(empty()));
    }


    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectWlcgProfileWithoutPrefix() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("gplazma.oidc.provider!EXAMPLE",
                "https://oidc.example.org/ -profile=wlcg");

        OidcAuthPlugin.buildProviders(properties, aClient().build(), Duration.ofSeconds(2));
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectWlcgProfileWithBadAuthz() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("gplazma.oidc.provider!EXAMPLE",
                "https://oidc.example.org/ -profile=wlcg -prefix=/target -authz-id=bad-principal");

        OidcAuthPlugin.buildProviders(properties, aClient().build(), Duration.ofSeconds(2));
    }

    @Test
    public void shouldAcceptWlcgProfileWithPrefix() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("gplazma.oidc.provider!EXAMPLE",
                "https://oidc.example.org/ -profile=wlcg -prefix=/target");

        var identityProviders = OidcAuthPlugin.buildProviders(properties, aClient().build(),
                Duration.ofSeconds(2));

        assertThat(identityProviders, hasSize(1));
        IdentityProvider provider = identityProviders.iterator().next();
        assertThat(provider.getProfile(), is(instanceOf(WlcgProfile.class)));
        WlcgProfile authzWGProfile = (WlcgProfile)provider.getProfile();
        assertThat(authzWGProfile.getPrefix(), is(equalTo(FsPath.create("/target"))));
        assertThat(authzWGProfile.getAuthzIdentity(), is(empty()));
        assertThat(authzWGProfile.getNonAuthzIdentity(), is(empty()));
    }

    @Test
    public void shouldAcceptWlcgProfileWithAuthzPrincipal() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("gplazma.oidc.provider!EXAMPLE",
                "https://oidc.example.org/ -profile=wlcg -prefix=/target -authz-id=group:my-group");

        var identityProviders = OidcAuthPlugin.buildProviders(properties, aClient().build(),
                Duration.ofSeconds(2));

        assertThat(identityProviders, hasSize(1));
        IdentityProvider provider = identityProviders.iterator().next();
        assertThat(provider.getProfile(), is(instanceOf(WlcgProfile.class)));
        WlcgProfile authzWGProfile = (WlcgProfile)provider.getProfile();
        assertThat(authzWGProfile.getPrefix(), is(equalTo(FsPath.create("/target"))));
        assertThat(authzWGProfile.getAuthzIdentity(), contains(new GroupNamePrincipal("my-group")));
        assertThat(authzWGProfile.getNonAuthzIdentity(), is(empty()));
    }

    @Test
    public void shouldAcceptWlcgProfileWithNonAuthzPrincipal() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("gplazma.oidc.provider!EXAMPLE",
                "https://oidc.example.org/ -profile=wlcg -prefix=/target -non-authz-id=group:my-group");

        var identityProviders = OidcAuthPlugin.buildProviders(properties, aClient().build(),
                Duration.ofSeconds(2));

        assertThat(identityProviders, hasSize(1));
        IdentityProvider provider = identityProviders.iterator().next();
        assertThat(provider.getProfile(), is(instanceOf(WlcgProfile.class)));
        WlcgProfile authzWGProfile = (WlcgProfile)provider.getProfile();
        assertThat(authzWGProfile.getPrefix(), is(equalTo(FsPath.create("/target"))));
        assertThat(authzWGProfile.getAuthzIdentity(), is(empty()));
        assertThat(authzWGProfile.getNonAuthzIdentity(), contains(new GroupNamePrincipal("my-group")));
    }

    @Test
    public void shouldAcceptWlcgProfileWithAuthzAndNonAuthzPrincipal() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("gplazma.oidc.provider!EXAMPLE",
                "https://oidc.example.org/ -profile=wlcg -prefix=/target -authz-id=group:authz-group -non-authz-id=group:non-authz-group");

        var identityProviders = OidcAuthPlugin.buildProviders(properties, aClient().build(),
                Duration.ofSeconds(2));

        assertThat(identityProviders, hasSize(1));
        IdentityProvider provider = identityProviders.iterator().next();
        assertThat(provider.getProfile(), is(instanceOf(WlcgProfile.class)));
        WlcgProfile authzWGProfile = (WlcgProfile)provider.getProfile();
        assertThat(authzWGProfile.getPrefix(), is(equalTo(FsPath.create("/target"))));
        assertThat(authzWGProfile.getAuthzIdentity(), contains(new GroupNamePrincipal("authz-group")));
        assertThat(authzWGProfile.getNonAuthzIdentity(), contains(new GroupNamePrincipal("non-authz-group")));
    }

    @Test
    public void shouldCallShutdown() throws Exception {
        given(aPlugin().withTokenProcessor(aTokenProcessor().thatFailsTestIfCalled()));

        plugin.stop();

        verify(processor).shutdown();
    }

    @Test(expected=AuthenticationException.class)
    public void shouldFailWithoutBearerToken() throws Exception {
        given(aPlugin().withTokenProcessor(aTokenProcessor().thatFailsTestIfCalled()));

        when(invoked().withoutCredentials());
    }

    @Test(expected=AuthenticationException.class)
    public void shouldFailWithTwoBearerTokens() throws Exception {
        given(aPlugin().withTokenProcessor(aTokenProcessor().thatFailsTestIfCalled()));

        when(invoked().withBearerToken("FOO").withBearerToken("BAR"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldFailWithUsernamePasswordCredential() throws Exception {
        given(aPlugin().withTokenProcessor(aTokenProcessor().thatFailsTestIfCalled()));

        when(invoked().withUsernamePassword("fred", "password"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldFailIfTokenProcessorFailsWithUnableToProcess() throws Exception {
        given(aPlugin().withTokenProcessor(aTokenProcessor().thatThrows(new UnableToProcess("I've no idea what to do"))));

        when(invoked().withBearerToken("FOO"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldFailIfTokenProcessorFailsWithAuthenticationException() throws Exception {
        given(aPlugin().withTokenProcessor(aTokenProcessor().thatThrows(new AuthenticationException("bad token: FOO"))));

        when(invoked().withBearerToken("FOO"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldFailIfTokenProcessorReturnsNoClaims() throws Exception {
        given(aPlugin().withTokenProcessor(aTokenProcessor().thatReturns(aResult().withNoClaims())));

        when(invoked().withBearerToken("FOO"));
    }

    @Test
    public void shouldProvideSubPrincipalIfProfileReturnsSubPrincipal() throws Exception {
        var profile = aProfile().thatReturns(aProfileResult()
                .withPrincipals(Collections.singleton(new OidcSubjectPrincipal("sub-claim-value", "MY-OP"))))
                .build();
        var op = anIp("MY-OP").withProfile(profile).build();
        var claims = claims().withStringClaim("sub", "sub-claim-value").build();
        given(aPlugin().withTokenProcessor(aTokenProcessor()
                .thatReturns(aResult().from(op).containing(claims))));

        when(invoked().withBearerToken("FOO"));

        verify(processor).extract(eq("FOO"));
        verify(profile).processClaims(eq(op), eq(claims));
        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldAcceptSingleAudMatchesSingleAllowedValue() throws Exception {
        var profile = aProfile().thatReturns(aProfileResult()
                .withPrincipals(Collections.singleton(new OidcSubjectPrincipal("sub-claim-value", "MY-OP"))))
                .build();
        var op = anIp("MY-OP").withProfile(profile).build();
        var claims = claims()
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("aud", "dcache.example.org")
                .build();
        given(aPlugin()
            .withProperty("gplazma.oidc.audience-targets", "dcache.example.org")
            .withTokenProcessor(aTokenProcessor().thatReturns(aResult().from(op).containing(claims))));

        when(invoked().withBearerToken("FOO"));

        verify(processor).extract(eq("FOO"));
        verify(profile).processClaims(eq(op), eq(claims));
        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldAcceptSingleAudWithSpaceMatchesSingleAllowedValue() throws Exception {
        var profile = aProfile().thatReturns(aProfileResult()
                .withPrincipals(Collections.singleton(new OidcSubjectPrincipal("sub-claim-value", "MY-OP"))))
                .build();
        var op = anIp("MY-OP").withProfile(profile).build();
        var claims = claims()
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("aud", "my target")
                .build();
        given(aPlugin()
            .withProperty("gplazma.oidc.audience-targets", "\"my target\"")
            .withTokenProcessor(aTokenProcessor().thatReturns(aResult().from(op).containing(claims))));

        when(invoked().withBearerToken("FOO"));

        verify(processor).extract(eq("FOO"));
        verify(profile).processClaims(eq(op), eq(claims));
        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldAcceptWhenSingleAudMatchesOneFromMultipleAllowedValue() throws Exception {
        var profile = aProfile().thatReturns(aProfileResult()
                .withPrincipals(Collections.singleton(new OidcSubjectPrincipal("sub-claim-value", "MY-OP"))))
                .build();
        var op = anIp("MY-OP").withProfile(profile).build();
        var claims = claims()
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("aud", "dcache.example.org")
                .build();
        given(aPlugin()
            .withProperty("gplazma.oidc.audience-targets", "dcache.example.org wlcg-all-storage")
            .withTokenProcessor(aTokenProcessor().thatReturns(aResult().from(op).containing(claims))));

        when(invoked().withBearerToken("FOO"));

        verify(processor).extract(eq("FOO"));
        verify(profile).processClaims(eq(op), eq(claims));
        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectSingleAuthClaimWithNoConfiguredAudiences() throws Exception {
        given(aPlugin().withTokenProcessor(aTokenProcessor().thatReturns(aResult()
                .from(anIp("MY-OP").withProfile(aProfile().thatFailsTestIfCalled()))
                .containing(claims()
                        .withStringClaim("sub", "sub-claim-value")
                        .withStringClaim("aud", "another-service.example.org")))));

        when(invoked().withBearerToken("FOO"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectSingleAuthClaimWithDifferentConfiguredAudience() throws Exception {
        given(aPlugin()
            .withProperty("gplazma.oidc.audience-targets", "dcache.example.org")
            .withTokenProcessor(aTokenProcessor().thatReturns(aResult()
                .from(anIp("MY-OP").withProfile(aProfile().thatFailsTestIfCalled()))
                .containing(claims()
                        .withStringClaim("sub", "sub-claim-value")
                        .withStringClaim("aud", "another-service.example.org")))));

        when(invoked().withBearerToken("FOO"));
    }


    @Test
    public void shouldAcceptArrayAuthClaimWithMatchingConfiguredAudience() throws Exception {
        var profile = aProfile().thatReturns(aProfileResult()
                .withPrincipals(Collections.singleton(new OidcSubjectPrincipal("sub-claim-value", "MY-OP"))))
                .build();
        var op = anIp("MY-OP").withProfile(profile).build();
        var claims = claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("aud", "[\"one-service.example.org\", \"dcache.example.org\"]")
                .build();
        given(aPlugin()
            .withProperty("gplazma.oidc.audience-targets", "dcache.example.org")
            .withTokenProcessor(aTokenProcessor().thatReturns(aResult().from(op).containing(claims))));

        when(invoked().withBearerToken("FOO"));

        verify(processor).extract(eq("FOO"));
        verify(profile).processClaims(eq(op), eq(claims));
        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restrictions, is(empty()));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectArrayAuthClaimWithMatchingConfiguredAudience() throws Exception {
        given(aPlugin()
            .withProperty("gplazma.oidc.audience-targets", "dcache.example.org")
            .withTokenProcessor(aTokenProcessor().thatReturns(aResult()
                .from(anIp("MY-OP").withProfile(aProfile().thatFailsTestIfCalled()))
                .containing(claims()
                        .withStringClaim("sub", "sub-claim-value")
                        .with("aud", "[\"one-service.example.org\", \"another-service.example.org\"]")))));

        when(invoked().withBearerToken("FOO"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectExpiredJwtToken() throws Exception {
        given(aPlugin().withTokenProcessor(aTokenProcessor().thatFailsTestIfCalled()));
        given(aJwt().withPayloadClaim("exp", Instant.now().minus(5, MINUTES)));

        when(invoked().withBearerToken(jwt));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectEmbargoedJwtToken() throws Exception {
        given(aPlugin().withTokenProcessor(aTokenProcessor().thatFailsTestIfCalled()));
        given(aJwt().withPayloadClaim("nbf", Instant.now().plus(5, MINUTES)));

        when(invoked().withBearerToken(jwt));
    }

    @Test
    public void shouldAcceptNonExpiredJwtToken() throws Exception {
        var profile = aProfile().thatReturns(aProfileResult()
                .withPrincipals(Collections.singleton(new OidcSubjectPrincipal("sub-claim-value", "MY-OP"))))
                .build();
        var op = anIp("MY-OP").withProfile(profile).build();
        var claims = claims().withStringClaim("sub", "sub-claim-value").build();
        given(aPlugin().withTokenProcessor(aTokenProcessor().thatReturns(aResult().from(op).containing(claims))));
        given(aJwt().withPayloadClaim("exp", Instant.now().plus(5, MINUTES)));

        when(invoked().withBearerToken(jwt));

        verify(processor).extract(eq(jwt));
        verify(profile).processClaims(eq(op), eq(claims));
        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restrictions, is(empty()));
    }

    @Test
    public void shouldAcceptNonEmbargoedJwtToken() throws Exception {
        var profile = aProfile().thatReturns(aProfileResult()
                .withPrincipals(Collections.singleton(new OidcSubjectPrincipal("sub-claim-value", "MY-OP"))))
                .build();
        var op = anIp("MY-OP").withProfile(profile).build();
        var claims = claims().withStringClaim("sub", "sub-claim-value").build();
        given(aPlugin().withTokenProcessor(aTokenProcessor().thatReturns(aResult().from(op).containing(claims))));
        given(aJwt().withPayloadClaim("nbf", Instant.now().minus(5, MINUTES)));

        when(invoked().withBearerToken(jwt));

        verify(processor).extract(eq(jwt));
        verify(profile).processClaims(eq(op), eq(claims));
        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restrictions, is(empty()));
    }

    private void given(JwtFactory.Builder builder) {
        jwt = builder.build();
    }

    private void given(PluginBuilder builder) {
        plugin = builder.build();
    }

    private void when(AuthenticateInvocationBuilder builder) throws AuthenticationException {
        builder.invokeOn(plugin);
    }

    private PluginBuilder aPlugin() {
        return new PluginBuilder();
    }

    private JwtFactory.Builder aJwt() {
        return jwtFactory.aJwt();
    }

    private TokenProcessorBuilder aTokenProcessor() {
        return new TokenProcessorBuilder();
    }

    private ExtractResultBuilder aResult() {
        return new ExtractResultBuilder();
    }

    private ClaimMapBuilder claims() {
        return new ClaimMapBuilder();
    }

    private AuthenticateInvocationBuilder invoked() {
        return new AuthenticateInvocationBuilder();
    }

    /**
     * A fluent class for building a (real) OidcAuthPlugin.
     */
    private class PluginBuilder {
        private Properties properties = new Properties();

        public PluginBuilder() {
            // Use a reasonable default, just to keep tests a bit smaller.
            properties.setProperty("gplazma.oidc.audience-targets", "");
        }

        public PluginBuilder withTokenProcessor(TokenProcessorBuilder builder) {
            processor = builder.build();
            return this;
        }

        public PluginBuilder withProperty(String key, String value) {
            properties.setProperty(key, value);
            return this;
        }

        public OidcAuthPlugin build() {
            return new OidcAuthPlugin(properties, processor);
        }
    }

    /**
     * Fluent class to build an authentication plugin invocation.
     */
    private class AuthenticateInvocationBuilder {
        private final Set<Object> publicCredentials = new HashSet<>();
        private final Set<Object> privateCredentials = new HashSet<>();

        public AuthenticateInvocationBuilder withBearerToken(String token) {
            privateCredentials.add(new BearerTokenCredential(token));
            return this;
        }

        public AuthenticateInvocationBuilder withUsernamePassword(String username, String password) {
            privateCredentials.add(new PasswordCredential(username, password));
            return this;
        }

        public AuthenticateInvocationBuilder withoutCredentials() {
            privateCredentials.clear();
            return this;
        }

        public void invokeOn(OidcAuthPlugin plugin) throws AuthenticationException {
            principals = new HashSet<>();
            restrictions = new HashSet<>();
            plugin.authenticate(publicCredentials, privateCredentials, principals, restrictions);
        }
    }

    /**
     * A fluent class for building a TokenProcessor.  It provides the same response for all extract
     * requests.
     */
    private static class TokenProcessorBuilder {
        private final TokenProcessor processor = mock(TokenProcessor.class);
        private boolean responseAdded;

        public TokenProcessorBuilder thatThrows(AuthenticationException exception) {
            checkState(!responseAdded);
            try {
                BDDMockito.given(processor.extract(ArgumentMatchers.any())).willThrow(exception);
            } catch (AuthenticationException | UnableToProcess e) {
                throw new RuntimeException("Impossible exception caught", e);
            }
            responseAdded = true;
            return this;
        }

        public TokenProcessorBuilder thatThrows(UnableToProcess exception) {
            checkState(!responseAdded);
            try {
                BDDMockito.given(processor.extract(ArgumentMatchers.any())).willThrow(exception);
            } catch (AuthenticationException | UnableToProcess e) {
                throw new RuntimeException("Impossible exception caught", e);
            }
            responseAdded = true;
            return this;
        }

        public TokenProcessorBuilder thatReturns(ExtractResultBuilder builder) {
            checkState(!responseAdded);
            try {
                BDDMockito.given(processor.extract(ArgumentMatchers.any())).willReturn(builder.build());
            } catch (AuthenticationException | UnableToProcess e) {
                throw new RuntimeException("Impossible exception caught", e);
            }
            responseAdded = true;
            return this;
        }

        public TokenProcessorBuilder thatFailsTestIfCalled() {
            checkState(!responseAdded);
            try {
                BDDMockito.given(processor.extract(ArgumentMatchers.any())).willThrow(new AssertionError("TokenProcessor#assert called"));
            } catch (AuthenticationException | UnableToProcess e) {
                throw new RuntimeException("Impossible exception caught", e);
            }
            responseAdded = true;
            return this;
        }

        public TokenProcessor build() {
            checkState(responseAdded);
            return processor;
        }
    }

    /**
     * A fluent class for building a mocked ExtractResult.
     */
    private static class ExtractResultBuilder {
        private final ExtractResult result = mock(ExtractResult.class);

        public ExtractResultBuilder from(MockIdentityProviderBuilder builder) {
            var idp = builder.build();
            return from(idp);
        }

        public ExtractResultBuilder from(IdentityProvider idp) {
            BDDMockito.given(result.idp()).willReturn(idp);
            return this;
        }

        public ExtractResultBuilder containing(ClaimMapBuilder builder) {
            return containing(builder.build());
        }

        public ExtractResultBuilder containing(Map<String,JsonNode> claims) {
            BDDMockito.given(result.claims()).willReturn(claims);
            return this;
        }

        public ExtractResultBuilder withNoClaims() {
            // By default, Mockito returns an empty map.
            return this;
        }

        public ExtractResult build() {
            return result;
        }
    }

    /**
     * A fluent class for building a set of claims.
     */
    private static class ClaimMapBuilder {
        private final ObjectMapper mapper = new ObjectMapper();
        private final Map<String,JsonNode> claims = new HashMap<>();

        public ClaimMapBuilder withStringClaim(String key, String value) {
            return with(key, "\"" + value + "\"");
        }

        public ClaimMapBuilder with(String key, String jsonValue) {
            try {
                JsonNode json = mapper.readTree(jsonValue);
                claims.put(key, json);
                return this;
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        public Map<String,JsonNode> build() {
            return claims;
        }
    }
}
