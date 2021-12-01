/*
 * dCache - http://www.dcache.org/
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
package org.dcache.gplazma.oidc.profiles;

import java.security.Principal;
import java.util.Optional;
import java.util.Set;
import org.dcache.auth.EmailAddressPrincipal;
import org.dcache.auth.EntitlementPrincipal;
import org.dcache.auth.ExemptFromNamespaceChecks;
import org.dcache.auth.FullNamePrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.LoA;
import org.dcache.auth.LoAPrincipal;
import org.dcache.auth.OidcSubjectPrincipal;
import org.dcache.auth.OpenIdGroupPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.auth.attributes.Restriction;
import org.junit.Before;
import org.junit.Test;
import org.dcache.gplazma.oidc.ProfileResult;

import static com.github.npathai.hamcrestopt.OptionalMatchers.isEmpty;
import static org.dcache.gplazma.oidc.MockIdentityProviderBuilder.anIp;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class OidcProfileTest {

    private OidcProfile profile;
    private Set<Principal> principals;
    private Optional<Restriction> restriction;

    @Before
    public void setup() {
        profile = null;
        principals = null;
    }

    @Test
    public void shouldIntrospectCorrectlyWhenNotAcceptingPreferredUsernameAndNotAcceptingGroups()
            throws Exception {
        given(anOidcProfile());

        assertThat(profile.isPreferredUsernameClaimAccepted(), is(equalTo(false)));
        assertThat(profile.isGroupsClaimMappedToGroupName(), is(equalTo(false)));
    }

    @Test
    public void shouldIntrospectCorrectlyWhenAcceptingPreferredUsernameAndNotAcceptingGroups()
            throws Exception {
        given(anOidcProfile().thatAcceptsPreferredUsername());

        assertThat(profile.isPreferredUsernameClaimAccepted(), is(equalTo(true)));
        assertThat(profile.isGroupsClaimMappedToGroupName(), is(equalTo(false)));
    }

    @Test
    public void shouldIntrospectCorrectlyWhenAcceptingPreferredNotUsernameAndAcceptingGroups()
            throws Exception {
        given(anOidcProfile().thatAcceptsGroups());

        assertThat(profile.isPreferredUsernameClaimAccepted(), is(equalTo(false)));
        assertThat(profile.isGroupsClaimMappedToGroupName(), is(equalTo(true)));
    }

    @Test
    public void shouldIntrospectCorrectlyWhenAcceptingPreferredUsernameAndAcceptingGroups()
            throws Exception {
        given(anOidcProfile().thatAcceptsPreferredUsername().thatAcceptsGroups());

        assertThat(profile.isPreferredUsernameClaimAccepted(), is(equalTo(true)));
        assertThat(profile.isGroupsClaimMappedToGroupName(), is(equalTo(true)));
    }

    @Test
    public void shouldProvideSubPrincipalFromSubClaim() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP")).withStringClaim("sub", "sub-claim-value"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldProvideGroupsPrincipalFromGroupsClaim() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("groups", "[\"group-A\", \"group-B\"]"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, hasItem(new OpenIdGroupPrincipal("group-A")));
        assertThat(principals, hasItem(new OpenIdGroupPrincipal("group-B")));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldIgnoreGroupsIfGroupsClaimNotArray() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("groups", "group-A"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test // REVISIT: should oidc profile react to 'wlcg.groups' ?
    public void shouldAddGroupsIfWlcgGroupsClaimPresent() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("wlcg.groups", "[\"group-A\", \"group-B\"]"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, hasItem(new OpenIdGroupPrincipal("group-A")));
        assertThat(principals, hasItem(new OpenIdGroupPrincipal("group-B")));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test // REVISIT: should oidc profile react to 'wlcg.groups' ?
    public void shouldIgnoreGroupsIfWlcgGroupsClainsInvalidValues() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("wlcg.groups", "[\"group-A\", null, true, 42]"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, hasItem(new OpenIdGroupPrincipal("group-A")));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test // REVISIT: should oidc profile react to 'wlcg.groups' ?
    public void shouldIgnoreGroupsIfWlcgGroupsNotArray() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("wlcg.groups", "group-A"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldAddNameIfNameClaim() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("name", "Paul Millar"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, hasItem(new FullNamePrincipal("Paul Millar")));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldIgnoreNameIfNameClaimNotString() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("name", "[\"Paul\", \"Millar\"]"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldIgnoreNameIfNameClaimEmpty() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("name", ""));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldAddConstructedNameGivenNameAndFamilyNameClaimsPresent() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("given_name", "Paul")
                .withStringClaim("family_name", "Millar"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, hasItem(new FullNamePrincipal("Paul Millar")));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldIgnoreGivenNameIfFamilyNameMissing() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("given_name", "Paul"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldIgnoreGivenNameIfFamilyNameEmpty() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("given_name", "Paul")
                .withStringClaim("family_name", ""));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldIgnoreFamilyNameIfGivenNameMissing() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("family_name", "Millar"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldIgnoreFamilyNameIfGivenNameEmpty() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("given_name", "")
                .withStringClaim("family_name", "Millar"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldProvideEmailIfEmailClaim() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("email", "paul.millar@desy.de"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, hasItem(new EmailAddressPrincipal("paul.millar@desy.de")));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldAddNoLoAPrincipalsIfAssuranceHasEmptyArray() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("eduperson_assurance", "[]"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldAddNoLoAPrincipalsIfAssuranceHasWrongType() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("eduperson_assurance", "https://refeds.org/assurance/ID/unique"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }


    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasRefedsIdUniqueAssuranceClaims() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("eduperson_assurance", "[\"https://refeds.org/assurance/ID/unique\"]"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.REFEDS_ID_UNIQUE)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasRefedsIdUniqueNoReassignAssuranceClaims() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("eduperson_assurance", "[\"https://refeds.org/assurance/ID/eppn-unique-no-reassign\"]"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.REFEDS_ID_EPPN_UNIQUE_NO_REASSIGN)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasRefedsIdUniqueReassignAfter1Y() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("eduperson_assurance", "[\"https://refeds.org/assurance/ID/eppn-unique-reassign-1y\"]"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.REFEDS_ID_EPPN_UNIQUE_REASSIGN_1Y)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasRefedsIapLow() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("eduperson_assurance", "[\"https://refeds.org/assurance/IAP/low\"]"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.REFEDS_IAP_LOW)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasRefedsIapMedium() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("eduperson_assurance", "[\"https://refeds.org/assurance/IAP/medium\"]"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.REFEDS_IAP_MEDIUM)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasRefedsIapHigh() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("eduperson_assurance", "[\"https://refeds.org/assurance/IAP/high\"]"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.REFEDS_IAP_HIGH)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldIgnoreLoAPrincipalIfAssuranceHasRefedsIapLocalEnterprise() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("eduperson_assurance", "[\"https://refeds.org/assurance/IAP/local-enterprise\"]"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasRefedsAtp1M() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("eduperson_assurance", "[\"https://refeds.org/assurance/ATP/ePA-1m\"]"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.REFEDS_ATP_1M)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasRefedsAtp1D() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("eduperson_assurance", "[\"https://refeds.org/assurance/ATP/ePA-1d\"]"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.REFEDS_ATP_1D)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasRefedsProfileCappuccino() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("eduperson_assurance", "[\"https://refeds.org/assurance/profile/cappuccino\"]"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.REFEDS_PROFILE_CAPPUCCINO)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasRefedsProfileEspresso() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("eduperson_assurance", "[\"https://refeds.org/assurance/profile/espresso\"]"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.REFEDS_PROFILE_ESPRESSO)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasIgtfAspen() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("eduperson_assurance", "[\"https://igtf.net/ap/authn-assurance/aspen\"]"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.IGTF_LOA_ASPEN)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasIgtfBirch() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("eduperson_assurance", "[\"https://igtf.net/ap/authn-assurance/birch\"]"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.IGTF_LOA_BIRCH)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasIgtfCedar() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("eduperson_assurance", "[\"https://igtf.net/ap/authn-assurance/cedar\"]"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.IGTF_LOA_CEDAR)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasIgtfDogwood() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("eduperson_assurance", "[\"https://igtf.net/ap/authn-assurance/dogwood\"]"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.IGTF_LOA_DOGWOOD)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasAarcAssam() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("eduperson_assurance", "[\"https://aarc-project.eu/policy/authn-assurance/assam\"]"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.AARC_PROFILE_ASSAM)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasEgiLow() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("eduperson_assurance", "[\"https://aai.egi.eu/LoA#Low\"]"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.EGI_LOW)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasEgiSubstantial() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("eduperson_assurance", "[\"https://aai.egi.eu/LoA#Substantial\"]"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.EGI_SUBSTANTIAL)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasEgiHigh() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("eduperson_assurance", "[\"https://aai.egi.eu/LoA#High\"]"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, hasItem(new LoAPrincipal(LoA.EGI_HIGH)));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldProvideLoAPrincipalIfAssuranceHasSeveralValues() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                // The following set of assurance claims consitute the Refeds Cappuccino profile.
                .withClaim("eduperson_assurance", "[\"https://refeds.org/assurance\","
                        + " \"https://refeds.org/assurance/ID/unique\","
                        + " \"https://refeds.org/assurance/IAP/low\","
                        + " \"https://refeds.org/assurance/IAP/medium\","
                        + " \"https://refeds.org/assurance/ATP/ePA-1m\", "
                        + " \"https://refeds.org/assurance/profile/cappuccino\"]"));

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
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldProvideEntitlementPrincipalIfAssuranceHasSingleEntitlement() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("eduperson_entitlement", "foo:bar:baz"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, hasItem(new EntitlementPrincipal("foo:bar:baz")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldIgnoreEntitlementPrincipalIfAssuranceHasSingleInvalidString() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("eduperson_entitlement", ":not:a:valid:uri"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldIgnoreEntitlementPrincipalIfSingleAssuranceNotStringType() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("eduperson_entitlement", "42"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldIgnoreEntitlementPrincipalIfAssuranceObject() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("eduperson_entitlement", "{\"foo\": \"bar\"}"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldProvideEntitlementPrincipalIfAssuranceHasSingleArrayEntitlement() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("eduperson_entitlement", "[\"foo:bar:baz\"]"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, hasItem(new EntitlementPrincipal("foo:bar:baz")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldProvideEntitlementPrincipalIfAssuranceHasMultipleArrayEntitlement() throws Exception {
        given(anOidcProfile());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("eduperson_entitlement", "[\"foo:bar:baz\", \"a:b:c\"]"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, hasItem(new EntitlementPrincipal("foo:bar:baz")));
        assertThat(principals, hasItem(new EntitlementPrincipal("a:b:c")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldProvideUsernamePrincipalIfAcceptUsernameWithPreferredUsername() throws Exception {
        given(anOidcProfile().thatAcceptsPreferredUsername());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("preferred_username", "paul"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, hasItem(new UserNamePrincipal("paul")));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldIgnoreUsernameIfPreferredUsernameHasWrongType() throws Exception {
        given(anOidcProfile().thatAcceptsPreferredUsername());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("preferred_username", "42"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldNotHaveUsernameIfPreferredUsernameMissing() throws Exception {
        given(anOidcProfile().thatAcceptsPreferredUsername());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldAddGroupIfAcceptGroups() throws Exception {
        given(anOidcProfile().thatAcceptsGroups());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("groups", "[\"group-A\", \"group-B\"]"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, hasItem(new GroupNamePrincipal("group-A")));
        assertThat(principals, hasItem(new GroupNamePrincipal("group-B")));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldAddGroupWithoutInitialSlashIfAcceptGroups() throws Exception {
        given(anOidcProfile().thatAcceptsGroups());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("groups", "[\"/group-A\", \"/group-B\"]"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, hasItem(new GroupNamePrincipal("group-A")));
        assertThat(principals, hasItem(new GroupNamePrincipal("group-B")));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldIgnoreGroupIfAcceptGroupsWithWrongType() throws Exception {
        given(anOidcProfile().thatAcceptsGroups());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withClaim("groups", "42"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldIgnoreGroupIfAcceptGroupsWithMissingGroupsClaim() throws Exception {
        given(anOidcProfile().thatAcceptsGroups());

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    private void given(OidcProfileBuilder builder) {
        profile = builder.build();
    }

    private void when(ProfileInvocationBuilder builder) throws AuthenticationException {
        ProfileResult result = builder.invoke();
        principals = result.getPrincipals();
        restriction = result.getRestriction();
    }

    private OidcProfileBuilder anOidcProfile() {
        return new OidcProfileBuilder();
    }

    private ProfileInvocationBuilder invoked() {
        return new ProfileInvocationBuilder(profile);
    }

    private static class OidcProfileBuilder {
        private boolean acceptUsername;
        private boolean acceptGroups;

        public OidcProfileBuilder thatAcceptsPreferredUsername() {
            acceptUsername = true;
            return this;
        }

        public OidcProfileBuilder thatAcceptsGroups() {
            acceptGroups = true;
            return this;
        }

        public OidcProfile build() {
            return new OidcProfile(acceptUsername, acceptGroups);
        }
    }
}