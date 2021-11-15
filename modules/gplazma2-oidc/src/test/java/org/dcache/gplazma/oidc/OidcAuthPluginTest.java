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
import java.net.URI;
import java.security.Principal;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.dcache.auth.BearerTokenCredential;
import org.dcache.auth.EmailAddressPrincipal;
import org.dcache.auth.EntitlementPrincipal;
import org.dcache.auth.FullNamePrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.LoA;
import org.dcache.auth.LoAPrincipal;
import org.dcache.auth.OidcSubjectPrincipal;
import org.dcache.auth.OpenIdGroupPrincipal;
import org.dcache.auth.PasswordCredential;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.BDDMockito;

import static com.google.common.base.Preconditions.checkState;
import static java.time.temporal.ChronoUnit.MINUTES;
import static org.dcache.gplazma.oidc.MockIdentityProviderBuilder.anIp;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;


public class OidcAuthPluginTest {

    private final JwtFactory jwtFactory = new JwtFactory();

    private OidcAuthPlugin plugin;
    private Set<Principal> principals;
    private TokenProcessor processor;
    private String jwt;

    @Before
    public void setup() {
        plugin = null;
        principals = null;
        processor = null;
        jwt = null;
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldThrowExceptionIfHostnamesPropertyMissing() throws Exception {
        Properties properties = new Properties();

        OidcAuthPlugin.buildHosts(properties);
    }

    @Test
    public void shouldReturnEmptyIdentityProviderFromEmptyHostnamePropertyValue() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("gplazma.oidc.hostnames", "");

        var identityProvides = OidcAuthPlugin.buildHosts(properties);

        assertThat(identityProvides, is(empty()));
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldThrowExceptionIfHostnamePropertyValueContainsInvalidHost() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("gplazma.oidc.hostnames", "-oidc.example.org");

        OidcAuthPlugin.buildHosts(properties);
    }

    @Test
    public void shouldReturnIdentityProviderFromSingleHostnamePropertyValue() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("gplazma.oidc.hostnames", "oidc.example.org");

        var identityProviders = OidcAuthPlugin.buildHosts(properties);

        assertThat(identityProviders, hasSize(1));
        IdentityProvider provider = identityProviders.iterator().next();
        assertThat(provider.getName(), is(equalTo("oidc.example.org")));
        assertThat(provider.isUsernameAccepted(), is(equalTo(false)));
        assertThat(provider.areGroupsAccepted(), is(equalTo(false)));
        assertThat(provider.getIssuerEndpoint(), is(equalTo(URI.create("https://oidc.example.org/"))));
        assertThat(provider.getConfigurationEndpoint(), is(equalTo(URI.create("https://oidc.example.org/.well-known/openid-configuration"))));
    }

    @Test
    public void shouldReturnEmptySetFromBuildProvidersWithNoPrefixEntries() throws Exception {
        Properties properties = new Properties();

        var identityProviders = OidcAuthPlugin.buildProviders(properties);

        assertThat(identityProviders, is(empty()));
    }

    @Test
    public void shouldReturnSingleIdentityProviderFromBuildProvidersWithSinglePrefixEntries() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("gplazma.oidc.provider!EXAMPLE", "https://oidc.example.org/");

        var identityProviders = OidcAuthPlugin.buildProviders(properties);

        assertThat(identityProviders, hasSize(1));
        IdentityProvider provider = identityProviders.iterator().next();
        assertThat(provider.getName(), is(equalTo("EXAMPLE")));
        assertThat(provider.isUsernameAccepted(), is(equalTo(false)));
        assertThat(provider.areGroupsAccepted(), is(equalTo(false)));
        assertThat(provider.getIssuerEndpoint(), is(equalTo(URI.create("https://oidc.example.org/"))));
        assertThat(provider.getConfigurationEndpoint(), is(equalTo(URI.create("https://oidc.example.org/.well-known/openid-configuration"))));
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldThrowExceptionByBuildProvidersWithMissingName() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("gplazma.oidc.provider!", "https://oidc.example.org/");

        OidcAuthPlugin.buildProviders(properties);
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldThrowExceptionByBuildProvidersWithMissingURI() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("gplazma.oidc.provider!", "");

        OidcAuthPlugin.buildProviders(properties);
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldThrowExceptionByBuildProvidersWithBadAcceptValue() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("gplazma.oidc.provider!", "https://oidc.example.org/ -accept=BAD-VALUE");

        OidcAuthPlugin.buildProviders(properties);
    }

    @Test
    public void shouldReturnSingleIdentityProviderAcceptUsernameFromBuildProvidersWithSinglePrefixEntries() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("gplazma.oidc.provider!EXAMPLE", "https://oidc.example.org/ -accept=username");

        var identityProviders = OidcAuthPlugin.buildProviders(properties);

        assertThat(identityProviders, hasSize(1));
        IdentityProvider provider = identityProviders.iterator().next();
        assertThat(provider.getName(), is(equalTo("EXAMPLE")));
        assertThat(provider.isUsernameAccepted(), is(equalTo(true)));
        assertThat(provider.areGroupsAccepted(), is(equalTo(false)));
        assertThat(provider.getIssuerEndpoint(), is(equalTo(URI.create("https://oidc.example.org/"))));
        assertThat(provider.getConfigurationEndpoint(), is(equalTo(URI.create("https://oidc.example.org/.well-known/openid-configuration"))));
    }

    @Test
    public void shouldReturnSingleIdentityProviderAcceptGroupsFromBuildProvidersWithSinglePrefixEntries() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("gplazma.oidc.provider!EXAMPLE", "https://oidc.example.org/ -accept=groups");

        var identityProviders = OidcAuthPlugin.buildProviders(properties);

        assertThat(identityProviders, hasSize(1));
        IdentityProvider provider = identityProviders.iterator().next();
        assertThat(provider.getName(), is(equalTo("EXAMPLE")));
        assertThat(provider.isUsernameAccepted(), is(equalTo(false)));
        assertThat(provider.areGroupsAccepted(), is(equalTo(true)));
        assertThat(provider.getIssuerEndpoint(), is(equalTo(URI.create("https://oidc.example.org/"))));
        assertThat(provider.getConfigurationEndpoint(), is(equalTo(URI.create("https://oidc.example.org/.well-known/openid-configuration"))));
    }

    @Test
    public void shouldReturnSingleIdentityProviderAcceptUsernameAndGroupsFromBuildProvidersWithSinglePrefixEntries() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("gplazma.oidc.provider!EXAMPLE", "https://oidc.example.org/ -accept=username,groups");

        var identityProviders = OidcAuthPlugin.buildProviders(properties);

        assertThat(identityProviders, hasSize(1));
        IdentityProvider provider = identityProviders.iterator().next();
        assertThat(provider.getName(), is(equalTo("EXAMPLE")));
        assertThat(provider.isUsernameAccepted(), is(equalTo(true)));
        assertThat(provider.areGroupsAccepted(), is(equalTo(true)));
        assertThat(provider.getIssuerEndpoint(), is(equalTo(URI.create("https://oidc.example.org/"))));
        assertThat(provider.getConfigurationEndpoint(), is(equalTo(URI.create("https://oidc.example.org/.well-known/openid-configuration"))));
    }

    @Test
    public void shouldCallShutdown() throws Exception {
        givenAPluginWith(aTokenProcessor().thatFailsTestIfCalled());

        plugin.stop();

        verify(processor).shutdown();
    }

    @Test(expected=AuthenticationException.class)
    public void shouldFailWithoutBearerToken() throws Exception {
        givenAPluginWith(aTokenProcessor().thatFailsTestIfCalled());

        when(invoked().withoutCredentials());
    }

    @Test(expected=AuthenticationException.class)
    public void shouldFailWithTwoBearerTokens() throws Exception {
        givenAPluginWith(aTokenProcessor().thatFailsTestIfCalled());

        when(invoked().withBearerToken("FOO").withBearerToken("BAR"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldFailWithUsernamePasswordCredential() throws Exception {
        givenAPluginWith(aTokenProcessor().thatFailsTestIfCalled());

        when(invoked().withUsernamePassword("fred", "password"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldFailIfTokenProcessorFailsWithUnableToProcess() throws Exception {
        givenAPluginWith(aTokenProcessor().thatThrows(new UnableToProcess("I've no idea what to do")));

        when(invoked().withBearerToken("FOO"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldFailIfTokenProcessorFailsWithAuthenticationException() throws Exception {
        givenAPluginWith(aTokenProcessor().thatThrows(new AuthenticationException("bad token: FOO")));

        when(invoked().withBearerToken("FOO"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldFailIfTokenProcessorReturnsNoClaims() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().withNoClaims()));

        when(invoked().withBearerToken("FOO"));
    }

    @Test
    public void shouldProvideSubPrincipalIfTokenProcessorReturnsSubClaim() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value"))));

        when(invoked().withBearerToken("FOO"));

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
    public void shouldProvideGroupsPrincipalIfTokenProcessorReturnsGroupsClaim() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("groups", "[\"group-A\", \"group-B\"]"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, hasItem(new OpenIdGroupPrincipal("group-A")));
        assertThat(principals, hasItem(new OpenIdGroupPrincipal("group-B")));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldIgnoreGroupsIfGroupsClaimNotArray() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("groups", "group-A"))));

        when(invoked().withBearerToken("FOO"));

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
    public void shouldAddGroupsIfWlcgGroupsClaimPresent() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("wlcg.groups", "[\"group-A\", \"group-B\"]"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, hasItem(new OpenIdGroupPrincipal("group-A")));
        assertThat(principals, hasItem(new OpenIdGroupPrincipal("group-B")));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldIgnoreGroupsIfWlcgGroupsClainsInvalidValues() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("wlcg.groups", "[\"group-A\", null, true, 42]"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, hasItem(new OpenIdGroupPrincipal("group-A")));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldIgnoreGroupsIfWlcgGroupsNotArray() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("wlcg.groups", "group-A"))));

        when(invoked().withBearerToken("FOO"));

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
    public void shouldAddNameIfNameClaim() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("name", "Paul Millar"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, hasItem(new FullNamePrincipal("Paul Millar")));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldIgnoreNameIfNameClaimNotString() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("name", "[\"Paul\", \"Millar\"]"))));

        when(invoked().withBearerToken("FOO"));

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
    public void shouldIgnoreNameIfNameClaimEmpty() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("name", ""))));

        when(invoked().withBearerToken("FOO"));

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
    public void shouldAddConstructedNameGivenNameAndFamilyNameClaimsPresent() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("given_name", "Paul")
                .withStringClaim("family_name", "Millar"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, hasItem(new FullNamePrincipal("Paul Millar")));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldIgnoreGivenNameIfFamilyNameMissing() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("given_name", "Paul"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldIgnoreGivenNameIfFamilyNameEmpty() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("given_name", "Paul")
                .withStringClaim("family_name", ""))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldIgnoreFamilyNameIfGivenNameMissing() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("family_name", "Millar"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldIgnoreFamilyNameIfGivenNameEmpty() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("given_name", "")
                .withStringClaim("family_name", "Millar"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldProvideEmailIfEmailClaim() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("email", "paul.millar@desy.de"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, hasItem(new EmailAddressPrincipal("paul.millar@desy.de")));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldAddNoLoAPrincipalsIfAssuranceHasEmptyArray() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("eduperson_assurance", "[]"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldAddNoLoAPrincipalsIfAssuranceHasWrongType() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("eduperson_assurance", "https://refeds.org/assurance/ID/unique"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }


    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasRefedsIdUniqueAssuranceClaims() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("eduperson_assurance", "[\"https://refeds.org/assurance/ID/unique\"]"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.REFEDS_ID_UNIQUE)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasRefedsIdUniqueNoReassignAssuranceClaims() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("eduperson_assurance", "[\"https://refeds.org/assurance/ID/eppn-unique-no-reassign\"]"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.REFEDS_ID_EPPN_UNIQUE_NO_REASSIGN)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasRefedsIdUniqueReassignAfter1Y() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("eduperson_assurance", "[\"https://refeds.org/assurance/ID/eppn-unique-reassign-1y\"]"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.REFEDS_ID_EPPN_UNIQUE_REASSIGN_1Y)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasRefedsIapLow() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("eduperson_assurance", "[\"https://refeds.org/assurance/IAP/low\"]"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.REFEDS_IAP_LOW)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasRefedsIapMedium() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("eduperson_assurance", "[\"https://refeds.org/assurance/IAP/medium\"]"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.REFEDS_IAP_MEDIUM)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasRefedsIapHigh() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("eduperson_assurance", "[\"https://refeds.org/assurance/IAP/high\"]"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.REFEDS_IAP_HIGH)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldIgnoreLoAPrincipalIfAssuranceHasRefedsIapLocalEnterprise() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("eduperson_assurance", "[\"https://refeds.org/assurance/IAP/local-enterprise\"]"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasRefedsAtp1M() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("eduperson_assurance", "[\"https://refeds.org/assurance/ATP/ePA-1m\"]"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.REFEDS_ATP_1M)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasRefedsAtp1D() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("eduperson_assurance", "[\"https://refeds.org/assurance/ATP/ePA-1d\"]"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.REFEDS_ATP_1D)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasRefedsProfileCappuccino() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("eduperson_assurance", "[\"https://refeds.org/assurance/profile/cappuccino\"]"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.REFEDS_PROFILE_CAPPUCCINO)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasRefedsProfileEspresso() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("eduperson_assurance", "[\"https://refeds.org/assurance/profile/espresso\"]"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.REFEDS_PROFILE_ESPRESSO)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasIgtfAspen() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("eduperson_assurance", "[\"https://igtf.net/ap/authn-assurance/aspen\"]"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.IGTF_LOA_ASPEN)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasIgtfBirch() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("eduperson_assurance", "[\"https://igtf.net/ap/authn-assurance/birch\"]"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.IGTF_LOA_BIRCH)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasIgtfCedar() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("eduperson_assurance", "[\"https://igtf.net/ap/authn-assurance/cedar\"]"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.IGTF_LOA_CEDAR)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasIgtfDogwood() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("eduperson_assurance", "[\"https://igtf.net/ap/authn-assurance/dogwood\"]"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.IGTF_LOA_DOGWOOD)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasAarcAssam() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("eduperson_assurance", "[\"https://aarc-project.eu/policy/authn-assurance/assam\"]"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.AARC_PROFILE_ASSAM)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasEgiLow() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("eduperson_assurance", "[\"https://aai.egi.eu/LoA#Low\"]"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.EGI_LOW)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasEgiSubstantial() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("eduperson_assurance", "[\"https://aai.egi.eu/LoA#Substantial\"]"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.EGI_SUBSTANTIAL)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasEgiHigh() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("eduperson_assurance", "[\"https://aai.egi.eu/LoA#High\"]"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.EGI_HIGH)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasSeveralValues() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                // The following set of assurance claims consitute the Refeds Cappuccino profile.
                .with("eduperson_assurance", "[\"https://refeds.org/assurance\","
                        + " \"https://refeds.org/assurance/ID/unique\","
                        + " \"https://refeds.org/assurance/IAP/low\","
                        + " \"https://refeds.org/assurance/IAP/medium\","
                        + " \"https://refeds.org/assurance/ATP/ePA-1m\", "
                        + " \"https://refeds.org/assurance/profile/cappuccino\"]"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.REFEDS_ID_UNIQUE)));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.REFEDS_IAP_LOW)));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.REFEDS_IAP_MEDIUM)));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.REFEDS_ATP_1M)));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.REFEDS_PROFILE_CAPPUCCINO)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldProvideEntitlementPrincipalIfAssuranceHasSingleEntitlement() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("eduperson_entitlement", "foo:bar:baz"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, hasItem(new EntitlementPrincipal("foo:bar:baz")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldIgnoreEntitlementPrincipalIfAssuranceHasSingleInvalidString() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("eduperson_entitlement", ":not:a:valid:uri"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldIgnoreEntitlementPrincipalIfSingleAssuranceNotStringType() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("eduperson_entitlement", "42"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldIgnoreEntitlementPrincipalIfAssuranceObject() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("eduperson_entitlement", "{\"foo\": \"bar\"}"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldProvideEntitlementPrincipalIfAssuranceHasSingleArrayEntitlement() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("eduperson_entitlement", "[\"foo:bar:baz\"]"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, hasItem(new EntitlementPrincipal("foo:bar:baz")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldProvideEntitlementPrincipalIfAssuranceHasMultipleArrayEntitlement() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP")).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("eduperson_entitlement", "[\"foo:bar:baz\", \"a:b:c\"]"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, hasItem(new EntitlementPrincipal("foo:bar:baz")));
        assertThat(principals, hasItem(new EntitlementPrincipal("a:b:c")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldProvideUsernamePrincipalIfAcceptUsernameWithPreferredUsername() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP").withAcceptedUsername()).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("preferred_username", "paul"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, hasItem(new UserNamePrincipal("paul")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldIgnoreUsernameIfPreferredUsernameHasWrongType() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP").withAcceptedUsername()).containing(claims()
                .withStringClaim("sub", "sub-claim-value")
                .with("preferred_username", "42"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldNotHaveUsernameIfPreferredUsernameMissing() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP").withAcceptedUsername()).containing(claims()
                .withStringClaim("sub", "sub-claim-value"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
    }

    @Test
    public void shouldAddGroupIfAcceptGroups() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP").withAcceptedGroups())
                .containing(claims()
                        .withStringClaim("sub", "sub-claim-value")
                        .with("groups", "[\"group-A\", \"group-B\"]"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, hasItem(new GroupNamePrincipal("group-A")));
        assertThat(principals, hasItem(new GroupNamePrincipal("group-B")));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
    }

    @Test
    public void shouldAddGroupWithoutInitialSlashIfAcceptGroups() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP").withAcceptedGroups())
                .containing(claims()
                        .withStringClaim("sub", "sub-claim-value")
                        .with("groups", "[\"/group-A\", \"/group-B\"]"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, hasItem(new GroupNamePrincipal("group-A")));
        assertThat(principals, hasItem(new GroupNamePrincipal("group-B")));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
    }

    @Test
    public void shouldIgnoreGroupIfAcceptGroupsWithWrongType() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP").withAcceptedGroups())
            .containing(claims()
                    .withStringClaim("sub", "sub-claim-value")
                    .with("groups", "42"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
    }

    @Test
    public void shouldIgnoreGroupIfAcceptGroupsWithMissingGroupsClaim() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP").withAcceptedGroups())
                .containing(claims().withStringClaim("sub", "sub-claim-value"))));

        when(invoked().withBearerToken("FOO"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectExpiredJwtToken() throws Exception {
        givenAPluginWith(aTokenProcessor().thatFailsTestIfCalled());
        given(aJwt().withPayloadClaim("exp", Instant.now().minus(5, MINUTES)));

        when(invoked().withBearerToken(jwt));
    }

    @Test
    public void shouldAcceptNonExpiredJwtToken() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP").withAcceptedGroups())
                .containing(claims().withStringClaim("sub", "sub-claim-value"))));
        given(aJwt().withPayloadClaim("exp", Instant.now().plus(5, MINUTES)));

        when(invoked().withBearerToken(jwt));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
    }


    @Test(expected=AuthenticationException.class)
    public void shouldRejectEmbargoedJwtToken() throws Exception {
        givenAPluginWith(aTokenProcessor().thatFailsTestIfCalled());
        given(aJwt().withPayloadClaim("nbf", Instant.now().plus(5, MINUTES)));

        when(invoked().withBearerToken(jwt));
    }

    @Test
    public void shouldAcceptNonEmbargoedJwtToken() throws Exception {
        givenAPluginWith(aTokenProcessor().thatReturns(aResult().from(anIp("MY-OP").withAcceptedGroups())
                .containing(claims().withStringClaim("sub", "sub-claim-value"))));
        given(aJwt().withPayloadClaim("nbf", Instant.now().minus(5, MINUTES)));

        when(invoked().withBearerToken(jwt));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
    }

    private void givenAPluginWith(TokenProcessorBuilder builder) {
        processor = builder.build();
        plugin = new OidcAuthPlugin(processor);
    }

    private void given(JwtFactory.Builder builder) {
        jwt = builder.build();
    }

    private void when(AuthenticateInvocationBuilder builder) throws AuthenticationException {
        principals = builder.invokeOn(plugin);
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
     * Fluent class to build an authentication plugin invocation.
     */
    private static class AuthenticateInvocationBuilder {
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

        public Set<Principal> invokeOn(OidcAuthPlugin plugin) throws AuthenticationException {
            Set<Principal> identifiedPrincipals = new HashSet<>();
            plugin.authenticate(publicCredentials, privateCredentials, identifiedPrincipals);
            return identifiedPrincipals;
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
            BDDMockito.given(result.idp()).willReturn(builder.build());
            return this;
        }

        public ExtractResultBuilder containing(ClaimMapBuilder builder) {
            BDDMockito.given(result.claims()).willReturn(builder.build());
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
