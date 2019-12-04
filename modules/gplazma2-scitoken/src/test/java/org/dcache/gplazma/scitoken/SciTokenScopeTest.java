/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019-2020 Deutsches Elektronen-Synchrotron
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

import org.junit.Test;

import java.util.List;
import java.util.Optional;

import diskCacheV111.util.FsPath;

import org.dcache.auth.attributes.MultiTargetedRestriction.Authorisation;

import static org.dcache.auth.attributes.Activity.*;
import static org.dcache.gplazma.scitoken.SciTokenScope.Operation.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class SciTokenScopeTest
{
    @Test(expected=InvalidScopeException.class)
    public void shouldRejectUnknownOperation()
    {
        SciTokenScope ignored = new SciTokenScope("unknown");
    }

    @Test
    public void shouldIdentifySimpleReadScope()
    {
        boolean isScope = SciTokenScope.isSciTokenScope("read");

        assertTrue("simple read with no path", isScope);
    }

    @Test
    public void shouldIdentifyUrlReadScope()
    {
        boolean isScope = SciTokenScope.isSciTokenScope("https://scitokens.org/v1/authz/read");

        assertTrue("url read with no path", isScope);
    }

    @Test
    public void shouldIdentifySimpleWriteScope()
    {
        boolean isScope = SciTokenScope.isSciTokenScope("write");

        assertTrue("simple write with no path", isScope);
    }

    @Test
    public void shouldIdentifyUrlWriteScope()
    {
        boolean isScope = SciTokenScope.isSciTokenScope("https://scitokens.org/v1/authz/write");

        assertTrue("url read with no path", isScope);
    }

    @Test
    public void shouldIdentifySimpleQueueScope()
    {
        boolean isScope = SciTokenScope.isSciTokenScope("queue");

        assertTrue("simple queue with no path", isScope);
    }

    @Test
    public void shouldIdentifySimpleExecuteScope()
    {
        boolean isScope = SciTokenScope.isSciTokenScope("execute");

        assertTrue("simple execute with no path", isScope);
    }

    @Test
    public void shouldIdentifyReadWithAbsoluteRootPathScope()
    {
        boolean isScope = SciTokenScope.isSciTokenScope("read:/");

        assertTrue("simple read with root path", isScope);
    }

    @Test
    public void shouldIdentifyReadWithAbsoluteNonRootPathScope()
    {
        boolean isScope = SciTokenScope.isSciTokenScope("read:/path/to/dir");

        assertTrue("simple read with non-root path", isScope);
    }

    @Test
    public void shouldIdentifyUrlReadWithAbsoluateRootPath()
    {
        boolean isScope = SciTokenScope.isSciTokenScope("https://scitokens.org/v1/authz/read:/");

        assertTrue("url read with root path", isScope);
    }

    @Test
    public void shouldIdentifyUrlReadWithAbsoluteNonRootPath()
    {
        boolean isScope = SciTokenScope.isSciTokenScope("https://scitokens.org/v1/authz/read:/path/to/dir");

        assertTrue("url read with non-root path", isScope);
    }

    @Test
    public void shouldIdentifyWriteWithAbsoluteRootPathScope()
    {
        boolean isScope = SciTokenScope.isSciTokenScope("write:/");

        assertTrue("simple read with root path", isScope);
    }

    @Test
    public void shouldIdentifyWriteWithAbsoluteNonRootPathScope()
    {
        boolean isScope = SciTokenScope.isSciTokenScope("write:/path/to/dir");

        assertTrue("simple read with root path", isScope);
    }

    @Test
    public void shouldIdentifyUrlWriteWithAbsoluateRootPath()
    {
        boolean isScope = SciTokenScope.isSciTokenScope("https://scitokens.org/v1/authz/write:/");

        assertTrue("url write with root path", isScope);
    }

    @Test
    public void shouldIdentifyUrlWriteWithAbsoluteNonRootPath()
    {
        boolean isScope = SciTokenScope.isSciTokenScope("https://scitokens.org/v1/authz/write:/path/to/dir");

        assertTrue("url write with non-root path", isScope);
    }

    @Test
    public void shouldNotIdentifyUnknownScope()
    {
        boolean isScope = SciTokenScope.isSciTokenScope("unknown");

        assertFalse("unknown scope", isScope);
    }

    @Test
    public void shouldNotIdentifyReadWithRelativePath()
    {
        boolean isScope = SciTokenScope.isSciTokenScope("read:some-path");

        assertFalse("simple read with relative path", isScope);
    }

    @Test
    public void shouldNotAuthoriseQueue()
    {
        SciTokenScope scope = new SciTokenScope("queue");

        Optional<Authorisation> maybeAuthorisation = scope.authorisation(FsPath.create("/prefix/path"));

        assertFalse(maybeAuthorisation.isPresent());
    }

    @Test
    public void shouldNotAuthoriseExecute()
    {
        SciTokenScope scope = new SciTokenScope("execute");

        Optional<Authorisation> maybeAuthorisation = scope.authorisation(FsPath.create("/prefix/path"));

        assertFalse(maybeAuthorisation.isPresent());
    }

    @Test
    public void shouldAuthoriseReadPaths()
    {
        SciTokenScope scope = new SciTokenScope("read:/foo");

        Optional<Authorisation> maybeAuthorisation = scope.authorisation(FsPath.create("/prefix/path"));

        assertTrue(maybeAuthorisation.isPresent());

        Authorisation authz = maybeAuthorisation.get();

        assertThat(authz.getPath(), equalTo(FsPath.create("/prefix/path/foo")));
        assertThat(authz.getActivity(), containsInAnyOrder(LIST, READ_METADATA, DOWNLOAD));
    }

    @Test
    public void shouldAuthoriseWritePaths()
    {
        SciTokenScope scope = new SciTokenScope("write:/foo");

        Optional<Authorisation> maybeAuthorisation = scope.authorisation(FsPath.create("/prefix/path"));

        assertTrue(maybeAuthorisation.isPresent());

        Authorisation authz = maybeAuthorisation.get();

        assertThat(authz.getPath(), equalTo(FsPath.create("/prefix/path/foo")));
        assertThat(authz.getActivity(), containsInAnyOrder(LIST, READ_METADATA, UPLOAD, MANAGE, DELETE, UPDATE_METADATA));
    }

    @Test
    public void shouldNotEqualDifferentObject()
    {
        SciTokenScope scope1 = new SciTokenScope(READ, "/foo");

        assertThat(scope1, is(not(equalTo("read:/foo"))));
    }

    @Test
    public void shouldEqualSameOperationAndPath()
    {
        SciTokenScope scope1 = new SciTokenScope(READ, "/foo");
        SciTokenScope scope2 = new SciTokenScope(READ, "/foo");

        assertThat(scope1, is(equalTo(scope2)));
    }

    @Test
    public void shouldHaveSameHashCodeWithSameOperationAndPath()
    {
        SciTokenScope scope1 = new SciTokenScope(READ, "/foo");
        SciTokenScope scope2 = new SciTokenScope(READ, "/foo");

        assertThat(scope1.hashCode(), equalTo(scope2.hashCode()));
    }

    @Test
    public void shouldNotEqualSameOperationAndDifferentPath()
    {
        SciTokenScope scope1 = new SciTokenScope(READ, "/foo");
        SciTokenScope scope2 = new SciTokenScope(READ, "/bar");

        assertThat(scope1, is(not(equalTo(scope2))));
    }

    @Test
    public void shouldNotEqualDifferentOperationAndSamePath()
    {
        SciTokenScope scope1 = new SciTokenScope(READ, "/foo");
        SciTokenScope scope2 = new SciTokenScope(WRITE, "/foo");

        assertThat(scope1, is(not(equalTo(scope2))));
    }

    @Test
    public void shouldAcceptReadAsSciTokenScope()
    {
        SciTokenScope scope = new SciTokenScope("read");

        assertThat(scope, equalTo(new SciTokenScope(READ, "/")));
    }

    @Test
    public void shouldAcceptQueueAsSciTokenScope()
    {
        SciTokenScope scope = new SciTokenScope("queue");

        assertThat(scope, equalTo(new SciTokenScope(QUEUE, "/")));
    }

    @Test
    public void shouldAcceptReadWithPathAsSciTokenScope()
    {
        SciTokenScope scope = new SciTokenScope("read:/foo/bar");

        assertThat(scope, equalTo(new SciTokenScope(READ, "/foo/bar")));
    }

    @Test
    public void shouldAcceptPrefixReadAsSciTokenScope()
    {
        SciTokenScope scope = new SciTokenScope("https://scitokens.org/v1/authz/read");

        assertThat(scope, equalTo(new SciTokenScope(READ, "/")));
    }
}
