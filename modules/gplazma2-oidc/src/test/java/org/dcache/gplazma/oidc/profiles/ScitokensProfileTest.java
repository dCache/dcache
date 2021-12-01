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
import java.util.Optional;
import java.util.Set;
import org.dcache.auth.EmailAddressPrincipal;
import org.dcache.auth.EntitlementPrincipal;
import org.dcache.auth.ExemptFromNamespaceChecks;
import org.dcache.auth.FullNamePrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.LoAPrincipal;
import org.dcache.auth.OidcSubjectPrincipal;
import org.dcache.auth.OpenIdGroupPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.auth.attributes.Activity;
import org.dcache.auth.attributes.Restriction;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.oidc.ProfileResult;
import org.junit.Test;

import static com.github.npathai.hamcrestopt.OptionalMatchers.isPresent;
import static org.dcache.gplazma.oidc.MockIdentityProviderBuilder.anIp;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class ScitokensProfileTest {
    private ScitokensProfile profile;
    private Set<Principal> principals;
    private Optional<Restriction> restriction;

    @Test(expected=AuthenticationException.class)
    public void shouldRejectTokenWithoutScitokenScope() throws Exception {
        given(aScitokensProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("scope", "oidc profile"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectTokenWithReadScopeWithoutPath() throws Exception {
        given(aScitokensProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("scope", "oidc profile read:"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectTokenWithReadScopeWithRelativePath() throws Exception {
        given(aScitokensProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("scope", "oidc profile read:foo"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectTokenWithoutScopeClaim() throws Exception {
        given(aScitokensProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value"));
    }

    @Test
    public void shouldIgnoreNonTextualSubClaim() throws Exception {
        given(aScitokensProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withClaim("sub", "42")
                .withStringClaim("scope", "read:/"));

        assertThat(principals, not(hasItem(any(OidcSubjectPrincipal.class))));
    }

    @Test
    public void shouldAcceptWithoutSubClaim() throws Exception {
        given(aScitokensProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("scope", "read:/"));

        assertThat(principals, not(hasItem(any(OidcSubjectPrincipal.class))));
    }

    @Test
    public void shouldIncludeTextualSubClaim() throws Exception {
        given(aScitokensProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("sub", "sub-claim-value")
                .withStringClaim("scope", "read:/"));

        assertThat(principals, hasItem(new OidcSubjectPrincipal("sub-claim-value", "MY-OP")));
    }

    @Test
    public void shouldNotFrabricateMissingInformation() throws Exception {
        given(aScitokensProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("scope", "read:/"));

        assertThat(principals, not(hasItem(any(GroupNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(UserNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(EmailAddressPrincipal.class))));
        assertThat(principals, not(hasItem(any(FullNamePrincipal.class))));
        assertThat(principals, not(hasItem(any(OpenIdGroupPrincipal.class))));
        assertThat(principals, not(hasItem(any(LoAPrincipal.class))));
        assertThat(principals, not(hasItem(any(EntitlementPrincipal.class))));
    }

    @Test
    public void shouldAcceptRootReadScitokenScope() throws Exception {
        given(aScitokensProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("scope", "read:/"));

        assertThat(principals, hasItem(any(ExemptFromNamespaceChecks.class)));
        assertThat(restriction, isPresent());
        Restriction r = restriction.get();
        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/other/my-file")));
        assertFalse(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/prefix/my-file")));
    }

    @Test
    public void shouldAcceptSubdirReadScitokenScope() throws Exception {
        given(aScitokensProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("scope", "read:/target"));

        assertThat(principals, hasItem(any(ExemptFromNamespaceChecks.class)));
        assertThat(restriction, isPresent());
        Restriction r = restriction.get();
        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/my-file")));
        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/other/my-file")));
        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/prefix/my-file")));
        assertFalse(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/prefix/target/my-file")));
    }

    @Test
    public void shouldAcceptRootWriteScitokenScope() throws Exception {
        given(aScitokensProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("scope", "write:/"));

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
        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/prefix/my-file"))); // write does not imply read
        assertFalse(r.isRestricted(Activity.UPLOAD, FsPath.create("/prefix/my-file")));
        assertFalse(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/prefix/my-file")));
        assertFalse(r.isRestricted(Activity.MANAGE, FsPath.create("/prefix/my-file")));
        assertFalse(r.isRestricted(Activity.DELETE, FsPath.create("/prefix/my-file")));
    }

    @Test
    public void shouldAcceptRootReadAndWriteScitokenScope() throws Exception {
        given(aScitokensProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("scope", "read:/ write:/"));

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
        assertFalse(r.isRestricted(Activity.UPLOAD, FsPath.create("/prefix/my-file")));
        assertFalse(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/prefix/my-file")));
        assertFalse(r.isRestricted(Activity.MANAGE, FsPath.create("/prefix/my-file")));
        assertFalse(r.isRestricted(Activity.DELETE, FsPath.create("/prefix/my-file")));
    }

    @Test
    public void shouldAcceptRootReadAndTargetWriteScitokenScope() throws Exception {
        given(aScitokensProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("scope", "read:/ write:/target"));

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
        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/prefix/my-file")));

        assertFalse(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/prefix/target/my-file")));
        assertFalse(r.isRestricted(Activity.UPLOAD, FsPath.create("/prefix/target/my-file")));
        assertFalse(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/prefix/target/my-file")));
        assertFalse(r.isRestricted(Activity.MANAGE, FsPath.create("/prefix/target/my-file")));
        assertFalse(r.isRestricted(Activity.DELETE, FsPath.create("/prefix/target/my-file")));
    }

    @Test
    public void shouldAcceptTargetedReadAndTargetedWriteScitokenScope() throws Exception {
        given(aScitokensProfile().withPrefix("/prefix"));

        when(invoked().withIdP(anIp("MY-OP"))
                .withStringClaim("scope", "read:/read-target write:/write-target"));

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
        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/prefix/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/prefix/my-file")));

        assertFalse(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/prefix/read-target/my-file")));
        assertTrue(r.isRestricted(Activity.UPLOAD, FsPath.create("/prefix/read-target/my-file")));
        assertTrue(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/prefix/read-target/my-file")));
        assertTrue(r.isRestricted(Activity.MANAGE, FsPath.create("/prefix/read-target/my-file")));
        assertTrue(r.isRestricted(Activity.DELETE, FsPath.create("/prefix/read-target/my-file")));

        assertTrue(r.isRestricted(Activity.DOWNLOAD, FsPath.create("/prefix/write-target/my-file")));
        assertFalse(r.isRestricted(Activity.UPLOAD, FsPath.create("/prefix/write-target/my-file")));
        assertFalse(r.isRestricted(Activity.UPDATE_METADATA, FsPath.create("/prefix/write-target/my-file")));
        assertFalse(r.isRestricted(Activity.MANAGE, FsPath.create("/prefix/write-target/my-file")));
        assertFalse(r.isRestricted(Activity.DELETE, FsPath.create("/prefix/write-target/my-file")));
    }

    private void given(ScitokensProfileBuilder builder) {
        profile = builder.build();
    }

    private void when(ProfileInvocationBuilder builder) throws AuthenticationException {
        ProfileResult result = builder.invoke();
        principals = result.getPrincipals();
        restriction = result.getRestriction();
    }

    private ScitokensProfileBuilder aScitokensProfile() {
        return new ScitokensProfileBuilder();
    }

    private ProfileInvocationBuilder invoked() {
        return new ProfileInvocationBuilder(profile);
    }

    private static class ScitokensProfileBuilder {
        private FsPath prefix;

        public ScitokensProfileBuilder withPrefix(String prefix) {
            this.prefix = FsPath.create(prefix);
            return this;
        }

        public ScitokensProfile build() {
            return new ScitokensProfile(prefix);
        }
    }
}