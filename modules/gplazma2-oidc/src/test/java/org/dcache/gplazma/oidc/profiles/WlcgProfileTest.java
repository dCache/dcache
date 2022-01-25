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

import diskCacheV111.util.FsPath;
import java.security.Principal;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import org.dcache.auth.ExemptFromNamespaceChecks;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.JwtJtiPrincipal;
import org.dcache.auth.JwtSubPrincipal;
import org.dcache.auth.OidcSubjectPrincipal;
import org.dcache.auth.OpenIdGroupPrincipal;
import org.dcache.auth.attributes.Activity;
import org.dcache.auth.attributes.Restriction;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.oidc.ProfileResult;
import org.dcache.util.PrincipalSetMaker;
import org.junit.Test;

import static com.github.npathai.hamcrestopt.OptionalMatchers.isEmpty;
import static com.github.npathai.hamcrestopt.OptionalMatchers.isPresent;
import static org.dcache.gplazma.oidc.MockIdentityProviderBuilder.anIp;
import static org.dcache.util.PrincipalSetMaker.aSetOfPrincipals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WlcgProfileTest {
    private WlcgProfile profile;
    private Set<Principal> principals;
    private Optional<Restriction> restriction;

    @Test
    public void shouldAcceptEmptyIdentities() throws Exception {
        given(aWlcgProfile().withPrefix("/prefix"));

        assertThat(profile.getPrefix(), is(equalTo(FsPath.create("/prefix"))));
        assertThat(profile.getAuthzIdentity(), is(empty()));
        assertThat(profile.getNonAuthzIdentity(), is(empty()));
    }

    @Test
    public void shouldAcceptNonEmptyAuthzIdentities() throws Exception {
        given(aWlcgProfile().withPrefix("/prefix")
                .withAuthzIdentity(aSetOfPrincipals().withGroupname("authz-group")));

        assertThat(profile.getPrefix(), is(equalTo(FsPath.create("/prefix"))));
        assertThat(profile.getAuthzIdentity(), contains(new GroupNamePrincipal("authz-group")));
        assertThat(profile.getNonAuthzIdentity(), is(empty()));
    }

    @Test
    public void shouldAcceptNonEmptyNonAuthzIdentities() throws Exception {
        given(aWlcgProfile().withPrefix("/prefix")
                .withNonAuthzIdentity(aSetOfPrincipals().withGroupname("non-authz-group")));

        assertThat(profile.getPrefix(), is(equalTo(FsPath.create("/prefix"))));
        assertThat(profile.getAuthzIdentity(), is(empty()));
        assertThat(profile.getNonAuthzIdentity(), contains(new GroupNamePrincipal("non-authz-group")));
    }

    @Test
    public void shouldAcceptNonEmptyAuthzAndNonAuthzIdentities() throws Exception {
        given(aWlcgProfile().withPrefix("/prefix")
                .withAuthzIdentity(aSetOfPrincipals().withGroupname("authz-group"))
                .withNonAuthzIdentity(aSetOfPrincipals().withGroupname("non-authz-group")));

        assertThat(profile.getPrefix(), is(equalTo(FsPath.create("/prefix"))));
        assertThat(profile.getAuthzIdentity(), contains(new GroupNamePrincipal("authz-group")));
        assertThat(profile.getNonAuthzIdentity(), contains(new GroupNamePrincipal("non-authz-group")));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectTokenWithoutWlcgVersion() throws Exception {
        given(aWlcgProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("scope", "openid"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectTokenWithIncompatibleWlcgVersion() throws Exception {
        given(aWlcgProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("wlcg.ver", "2.0")
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("scope", "openid"));
    }

    @Test
    public void shouldAcceptTokenWithSubAndNoJtiAndNoAuthzAndNoGroups() throws Exception {
        given(aWlcgProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("wlcg.ver", "1.0")
                .withStringClaim("scope", "openid"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, hasItem(new JwtSubPrincipal("MY-OP", "sub-claim-value")));
        assertThat(principals, not(hasItem(any(JwtJtiPrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldAcceptTokenWithJtiAndNoSubAndNoAuthzAndNoGroups() throws Exception {
        given(aWlcgProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("jti", "jti-value")
                .withStringClaim("wlcg.ver", "1.0")
                .withStringClaim("scope", "openid"));

        assertThat(principals, not(hasItem(any(JwtSubPrincipal.class))));
        assertThat(principals, not(hasItem(any(OidcSubjectPrincipal.class))));
        assertThat(principals, hasItem(new JwtJtiPrincipal("MY-OP", "jti-value")));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldIgnoreNonTextualJti() throws Exception {
        given(aWlcgProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withClaim("jti", "42")
                .withStringClaim("wlcg.ver", "1.0")
                .withStringClaim("scope", "openid"));

        assertThat(principals, not(hasItem(any(JwtSubPrincipal.class))));
        assertThat(principals, not(hasItem(any(OidcSubjectPrincipal.class))));
        assertThat(principals, not(hasItem(any(JwtJtiPrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldAcceptTokenWithSubAndJtiAndNoAuthzAndNoGroups() throws Exception {
        given(aWlcgProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("jti", "jti-value")
                .withStringClaim("wlcg.ver", "1.0")
                .withStringClaim("scope", "openid"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
        assertThat(principals, hasItem(new JwtSubPrincipal("MY-OP", "sub-claim-value")));
        assertThat(principals, hasItem(new JwtJtiPrincipal("MY-OP", "jti-value")));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldAddGroupsIfWlcgGroupsClaimPresent() throws Exception {
        given(aWlcgProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("wlcg.ver", "1.0")
                .withClaim("wlcg.groups", "[\"group-A\", \"group-B\"]")
                .withStringClaim("scope", "openid"));

        assertThat(principals, hasItem(new OpenIdGroupPrincipal("group-A")));
        assertThat(principals, hasItem(new OpenIdGroupPrincipal("group-B")));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldIgnoreGroupsIfWlcgGroupsClainsInvalidValues() throws Exception {
        given(aWlcgProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("wlcg.ver", "1.0")
                .withClaim("wlcg.groups", "[\"group-A\", null, true, 42]")
                .withStringClaim("scope", "openid"));

        assertThat(principals, hasItem(new OpenIdGroupPrincipal("group-A")));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldIgnoreGroupsIfWlcgGroupsNotArray() throws Exception {
        given(aWlcgProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("wlcg.ver", "1.0")
                .withStringClaim("wlcg.groups", "group-A")
                .withStringClaim("scope", "openid"));

        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(ExemptFromNamespaceChecks.class))));
        assertThat(restriction, isEmpty());
    }

    @Test
    public void shouldIncludeNonAuthzIdentity() throws Exception {
        given(aWlcgProfile().withPrefix("/prefix")
                .withAuthzIdentity(aSetOfPrincipals().withGroupname("authz-group"))
                .withNonAuthzIdentity(aSetOfPrincipals().withGroupname("non-authz-group")));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("wlcg.ver", "1.0")
                .withStringClaim("scope", "openid"));

        assertThat(principals, not(hasItem(new GroupNamePrincipal("authz-group"))));
        assertThat(principals, hasItem(new GroupNamePrincipal("non-authz-group")));
    }

    @Test
    public void shouldAcceptRootReadScope() throws Exception {
        given(aWlcgProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("wlcg.ver", "1.0")
                .withStringClaim("scope", "openid storage.read:/"));

        assertThat(principals, hasItem(any(ExemptFromNamespaceChecks.class)));
        assertThat(restriction, isPresent());
        Restriction r = restriction.get();
        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/my-file")));

        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/other/my-file")));

        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/prefix/my-file")));
        assertFalse(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/prefix/my-file")));
    }

    @Test
    public void shouldAcceptSubdirReadScope() throws Exception {
        given(aWlcgProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("wlcg.ver", "1.0")
                .withStringClaim("scope", "openid storage.read:/target"));

        assertThat(principals, hasItem(any(ExemptFromNamespaceChecks.class)));
        assertThat(restriction, isPresent());
        Restriction r = restriction.get();
        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/my-file")));

        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/other/my-file")));

        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/prefix/my-file")));

        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/prefix/target/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/prefix/target/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/prefix/target/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/prefix/target/my-file")));
        assertFalse(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/prefix/target/my-file")));
    }

    @Test
    public void shouldAcceptRootCreateScope() throws Exception {
        given(aWlcgProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("wlcg.ver", "1.0")
                .withStringClaim("scope", "openid storage.create:/"));

        assertThat(principals, hasItem(any(ExemptFromNamespaceChecks.class)));
        assertThat(restriction, isPresent());
        Restriction r = restriction.get();
        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/my-file")));

        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/other/my-file")));

        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/prefix/my-file")));
        assertFalse(r.isRestricted(Activity.UPLOAD, FsPath.create("/prefix/my-file")));
        assertFalse(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/prefix/my-file")));
        assertFalse(r.isRestricted(Activity.MANAGE, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/prefix/my-file")));
    }

    @Test
    public void shouldAcceptRootModifyScope() throws Exception {
        given(aWlcgProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("wlcg.ver", "1.0")
                .withStringClaim("scope", "openid storage.modify:/"));

        assertThat(principals, hasItem(any(ExemptFromNamespaceChecks.class)));
        assertThat(restriction, isPresent());
        Restriction r = restriction.get();
        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/my-file")));

        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/other/my-file")));

        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/prefix/my-file")));
        assertFalse(r.isRestricted(Activity.UPLOAD, FsPath.create("/prefix/my-file")));
        assertFalse(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/prefix/my-file")));
        assertFalse(r.isRestricted(Activity.MANAGE, FsPath.create("/prefix/my-file")));
        assertFalse(r.isRestricted(Activity.DELETE, FsPath.create("/prefix/my-file")));
    }

    @Test
    public void shouldAcceptRootReadAndModifyWlcgScope() throws Exception {
        given(aWlcgProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("wlcg.ver", "1.0")
                .withStringClaim("scope", "openid storage.read:/ storage.modify:/"));

        assertThat(principals, hasItem(any(ExemptFromNamespaceChecks.class)));
        assertThat(restriction, isPresent());
        Restriction r = restriction.get();
        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/my-file")));

        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/other/my-file")));

        assertFalse(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/prefix/my-file")));
        assertFalse(r.isRestricted(Activity.DELETE, FsPath.create("/prefix/my-file")));
        assertFalse(r.isRestricted(Activity.UPLOAD, FsPath.create("/prefix/my-file")));
        assertFalse(r.isRestricted(Activity.MANAGE, FsPath.create("/prefix/my-file")));
        assertFalse(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/prefix/my-file")));
    }

    @Test
    public void shouldAcceptNonRootReadAndModifyWlcgScope() throws Exception {
        given(aWlcgProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("wlcg.ver", "1.0")
                .withStringClaim("scope", "openid storage.read:/read-target storage.modify:/write-target"));

        assertThat(principals, hasItem(any(ExemptFromNamespaceChecks.class)));
        assertThat(restriction, isPresent());
        Restriction r = restriction.get();
        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/my-file")));

        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/other/my-file")));

        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/read-target/my-file")));
        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/read-target/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/read-target/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/read-target/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/read-target/my-file")));

        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/write-target/my-file")));
        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/write-target/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/write-target/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/write-target/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/write-target/my-file")));

        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/prefix/my-file")));

        assertFalse(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/prefix/read-target/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/prefix/read-target/my-file")));
        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/prefix/read-target/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/prefix/read-target/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/prefix/read-target/my-file")));

        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/prefix/write-target/my-file")));
        assertFalse(r.isRestricted(Activity.DELETE, FsPath.create("/prefix/write-target/my-file")));
        assertFalse(r.isRestricted(Activity.UPLOAD, FsPath.create("/prefix/write-target/my-file")));
        assertFalse(r.isRestricted(Activity.MANAGE, FsPath.create("/prefix/write-target/my-file")));
        assertFalse(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/prefix/write-target/my-file")));
    }

    @Test
    public void shouldAcceptRootReadAndNonRootModifyWlcgScope() throws Exception {
        given(aWlcgProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("wlcg.ver", "1.0")
                .withStringClaim("scope", "openid storage.read:/ storage.modify:/write-target"));

        assertThat(principals, hasItem(any(ExemptFromNamespaceChecks.class)));
        assertThat(restriction, isPresent());
        Restriction r = restriction.get();
        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/my-file")));

        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/other/my-file")));

        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/write-target/my-file")));
        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/write-target/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/write-target/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/write-target/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/write-target/my-file")));

        assertFalse(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/prefix/my-file")));

        assertFalse(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/prefix/write-target/my-file")));
        assertFalse(r.isRestricted(Activity.DELETE, FsPath.create("/prefix/write-target/my-file")));
        assertFalse(r.isRestricted(Activity.UPLOAD, FsPath.create("/prefix/write-target/my-file")));
        assertFalse(r.isRestricted(Activity.MANAGE, FsPath.create("/prefix/write-target/my-file")));
        assertFalse(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/prefix/write-target/my-file")));
    }

    @Test
    public void shouldIncludeAuthzIdentity() throws Exception {
        given(aWlcgProfile().withPrefix("/prefix")
                .withAuthzIdentity(aSetOfPrincipals().withGroupname("authz-group"))
                .withNonAuthzIdentity(aSetOfPrincipals().withGroupname("non-authz-group")));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("wlcg.ver", "1.0")
                .withStringClaim("scope", "openid storage.read:/"));

        assertThat(principals, hasItem(new GroupNamePrincipal("authz-group")));
        assertThat(principals, not(hasItem(new GroupNamePrincipal("non-authz-group"))));
    }

    private void given(WlcgProfileBuilder builder) {

        profile = builder.build();
    }

    private void when(ProfileInvocationBuilder builder) throws AuthenticationException {
        ProfileResult result = builder.invoke();
        principals = result.getPrincipals();
        restriction = result.getRestriction();
    }

    private WlcgProfileBuilder aWlcgProfile() {
        return new WlcgProfileBuilder();
    }

    private ProfileInvocationBuilder invoked() {
        return new ProfileInvocationBuilder(profile);
    }

    private static class WlcgProfileBuilder {
        private FsPath prefix;
        private Set<Principal> authzIdentity = Collections.emptySet();
        private Set<Principal> nonAuthzIdentity = Collections.emptySet();

        public WlcgProfileBuilder withPrefix(String prefix) {
            this.prefix = FsPath.create(prefix);
            return this;
        }

        public WlcgProfileBuilder withAuthzIdentity(PrincipalSetMaker maker) {
            authzIdentity = maker.build();
            return this;
        }

        public WlcgProfileBuilder withNonAuthzIdentity(PrincipalSetMaker maker) {
            nonAuthzIdentity = maker.build();
            return this;
        }

        public WlcgProfile build() {
            return new WlcgProfile(prefix, authzIdentity, nonAuthzIdentity);
        }
    }
}