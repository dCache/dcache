/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019-2022 Deutsches Elektronen-Synchrotron
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

import org.dcache.gplazma.oidc.profiles.InvalidScopeException;

import static org.dcache.auth.attributes.Activity.DELETE;
import static org.dcache.auth.attributes.Activity.DOWNLOAD;
import static org.dcache.auth.attributes.Activity.LIST;
import static org.dcache.auth.attributes.Activity.MANAGE;
import static org.dcache.auth.attributes.Activity.READ_METADATA;
import static org.dcache.auth.attributes.Activity.UPDATE_METADATA;
import static org.dcache.auth.attributes.Activity.UPLOAD;
import static org.dcache.gplazma.oidc.profiles.ScitokensScope.Operation.QUEUE;
import static org.dcache.gplazma.oidc.profiles.ScitokensScope.Operation.READ;
import static org.dcache.gplazma.oidc.profiles.ScitokensScope.Operation.WRITE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import diskCacheV111.util.FsPath;
import java.util.Optional;
import org.dcache.auth.attributes.MultiTargetedRestriction.Authorisation;
import org.junit.Test;

public class ScitokenScopeTest {

    @Test(expected = InvalidScopeException.class)
    public void shouldRejectUnknownOperation() {
        ScitokensScope ignored = new ScitokensScope("unknown");
    }

    @Test
    public void shouldIdentifySimpleReadScope() {
        boolean isScope = ScitokensScope.isSciTokenScope("read");

        assertTrue("simple read with no path", isScope);
    }

    @Test
    public void shouldIdentifyUrlReadScope() {
        boolean isScope = ScitokensScope.isSciTokenScope("https://scitokens.org/v1/authz/read");

        assertTrue("url read with no path", isScope);
    }

    @Test
    public void shouldIdentifySimpleWriteScope() {
        boolean isScope = ScitokensScope.isSciTokenScope("write");

        assertTrue("simple write with no path", isScope);
    }

    @Test
    public void shouldIdentifyUrlWriteScope() {
        boolean isScope = ScitokensScope.isSciTokenScope("https://scitokens.org/v1/authz/write");

        assertTrue("url read with no path", isScope);
    }

    @Test
    public void shouldIdentifySimpleQueueScope() {
        boolean isScope = ScitokensScope.isSciTokenScope("queue");

        assertTrue("simple queue with no path", isScope);
    }

    @Test
    public void shouldIdentifySimpleExecuteScope() {
        boolean isScope = ScitokensScope.isSciTokenScope("execute");

        assertTrue("simple execute with no path", isScope);
    }

    @Test
    public void shouldIdentifyReadWithAbsoluteRootPathScope() {
        boolean isScope = ScitokensScope.isSciTokenScope("read:/");

        assertTrue("simple read with root path", isScope);
    }

    @Test
    public void shouldIdentifyReadWithAbsoluteNonRootPathScope() {
        boolean isScope = ScitokensScope.isSciTokenScope("read:/path/to/dir");

        assertTrue("simple read with non-root path", isScope);
    }

    @Test
    public void shouldIdentifyUrlReadWithAbsoluateRootPath() {
        boolean isScope = ScitokensScope.isSciTokenScope("https://scitokens.org/v1/authz/read:/");

        assertTrue("url read with root path", isScope);
    }

    @Test
    public void shouldIdentifyUrlReadWithAbsoluteNonRootPath() {
        boolean isScope = ScitokensScope.isSciTokenScope(
              "https://scitokens.org/v1/authz/read:/path/to/dir");

        assertTrue("url read with non-root path", isScope);
    }

    @Test
    public void shouldIdentifyWriteWithAbsoluteRootPathScope() {
        boolean isScope = ScitokensScope.isSciTokenScope("write:/");

        assertTrue("simple read with root path", isScope);
    }

    @Test
    public void shouldIdentifyWriteWithAbsoluteNonRootPathScope() {
        boolean isScope = ScitokensScope.isSciTokenScope("write:/path/to/dir");

        assertTrue("simple read with root path", isScope);
    }

    @Test
    public void shouldIdentifyUrlWriteWithAbsoluateRootPath() {
        boolean isScope = ScitokensScope.isSciTokenScope("https://scitokens.org/v1/authz/write:/");

        assertTrue("url write with root path", isScope);
    }

    @Test
    public void shouldIdentifyUrlWriteWithAbsoluteNonRootPath() {
        boolean isScope = ScitokensScope.isSciTokenScope(
              "https://scitokens.org/v1/authz/write:/path/to/dir");

        assertTrue("url write with non-root path", isScope);
    }

    @Test
    public void shouldNotIdentifyUnknownScope() {
        boolean isScope = ScitokensScope.isSciTokenScope("unknown");

        assertFalse("unknown scope", isScope);
    }

    @Test
    public void shouldNotIdentifyReadWithRelativePath() {
        boolean isScope = ScitokensScope.isSciTokenScope("read:some-path");

        assertFalse("simple read with relative path", isScope);
    }

    @Test
    public void shouldNotAuthoriseQueue() {
        ScitokensScope scope = new ScitokensScope("queue");

        Optional<Authorisation> maybeAuthorisation = scope.authorisation(
              FsPath.create("/prefix/path"));

        assertFalse(maybeAuthorisation.isPresent());
    }

    @Test
    public void shouldNotAuthoriseExecute() {
        ScitokensScope scope = new ScitokensScope("execute");

        Optional<Authorisation> maybeAuthorisation = scope.authorisation(
              FsPath.create("/prefix/path"));

        assertFalse(maybeAuthorisation.isPresent());
    }

    @Test
    public void shouldAuthoriseReadPaths() {
        ScitokensScope scope = new ScitokensScope("read:/foo");

        Optional<Authorisation> maybeAuthorisation = scope.authorisation(
              FsPath.create("/prefix/path"));

        assertTrue(maybeAuthorisation.isPresent());

        Authorisation authz = maybeAuthorisation.get();

        assertThat(authz.getPath(), equalTo(FsPath.create("/prefix/path/foo")));
        assertThat(authz.getActivity(), containsInAnyOrder(LIST, READ_METADATA, DOWNLOAD));
    }

    @Test
    public void shouldAuthoriseWritePaths() {
        ScitokensScope scope = new ScitokensScope("write:/foo");

        Optional<Authorisation> maybeAuthorisation = scope.authorisation(
              FsPath.create("/prefix/path"));

        assertTrue(maybeAuthorisation.isPresent());

        Authorisation authz = maybeAuthorisation.get();

        assertThat(authz.getPath(), equalTo(FsPath.create("/prefix/path/foo")));
        assertThat(authz.getActivity(),
              containsInAnyOrder(LIST, READ_METADATA, UPLOAD, MANAGE, DELETE, UPDATE_METADATA));
    }

    @Test
    public void shouldNotEqualDifferentObject() {
        ScitokensScope scope1 = new ScitokensScope(READ, "/foo");

        assertThat(scope1, is(not(equalTo("read:/foo"))));
    }

    @Test
    public void shouldEqualSameOperationAndPath() {
        ScitokensScope scope1 = new ScitokensScope(READ, "/foo");
        ScitokensScope scope2 = new ScitokensScope(READ, "/foo");

        assertThat(scope1, is(equalTo(scope2)));
    }

    @Test
    public void shouldHaveSameHashCodeWithSameOperationAndPath() {
        ScitokensScope scope1 = new ScitokensScope(READ, "/foo");
        ScitokensScope scope2 = new ScitokensScope(READ, "/foo");

        assertThat(scope1.hashCode(), equalTo(scope2.hashCode()));
    }

    @Test
    public void shouldNotEqualSameOperationAndDifferentPath() {
        ScitokensScope scope1 = new ScitokensScope(READ, "/foo");
        ScitokensScope scope2 = new ScitokensScope(READ, "/bar");

        assertThat(scope1, is(not(equalTo(scope2))));
    }

    @Test
    public void shouldNotEqualDifferentOperationAndSamePath() {
        ScitokensScope scope1 = new ScitokensScope(READ, "/foo");
        ScitokensScope scope2 = new ScitokensScope(WRITE, "/foo");

        assertThat(scope1, is(not(equalTo(scope2))));
    }

    @Test
    public void shouldAcceptReadAsSciTokenScope() {
        ScitokensScope scope = new ScitokensScope("read");

        assertThat(scope, equalTo(new ScitokensScope(READ, "/")));
    }

    @Test
    public void shouldAcceptQueueAsSciTokenScope() {
        ScitokensScope scope = new ScitokensScope("queue");

        assertThat(scope, equalTo(new ScitokensScope(QUEUE, "/")));
    }

    @Test
    public void shouldAcceptReadWithPathAsSciTokenScope() {
        ScitokensScope scope = new ScitokensScope("read:/foo/bar");

        assertThat(scope, equalTo(new ScitokensScope(READ, "/foo/bar")));
    }

    @Test
    public void shouldAcceptPrefixReadAsSciTokenScope() {
        ScitokensScope scope = new ScitokensScope("https://scitokens.org/v1/authz/read");

        assertThat(scope, equalTo(new ScitokensScope(READ, "/")));
    }
}
