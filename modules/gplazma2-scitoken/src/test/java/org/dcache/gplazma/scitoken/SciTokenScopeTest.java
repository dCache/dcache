/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 Deutsches Elektronen-Synchrotron
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

import static org.dcache.gplazma.scitoken.SciTokenScope.Operation.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class SciTokenScopeTest
{
    @Test
    public void shouldReturnOperationPath()
    {
        SciTokenScope scope = new SciTokenScope(READ, "/foo");

        assertThat(scope.getOperation(), is(equalTo(READ)));
        assertThat(scope.getPath(), is(equalTo("/foo")));
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
    public void shouldNotAcceptEmailAsSciTokenScope()
    {
        List<SciTokenScope> scopes = SciTokenScope.parseScope("email");

        assertThat(scopes, is(empty()));
    }

    @Test
    public void shouldAcceptReadAsSciTokenScope()
    {
        List<SciTokenScope> scopes = SciTokenScope.parseScope("read");

        assertThat(scopes, contains(new SciTokenScope(READ, "/")));
    }

    @Test
    public void shouldAcceptWriteAsSciTokenScope()
    {
        List<SciTokenScope> scopes = SciTokenScope.parseScope("write");

        assertThat(scopes, contains(new SciTokenScope(WRITE, "/")));
    }

    @Test
    public void shouldAcceptReadAndWriteAsSciTokenScope()
    {
        List<SciTokenScope> scopes = SciTokenScope.parseScope("read write");

        assertThat(scopes, contains(new SciTokenScope(READ, "/"), new SciTokenScope(WRITE, "/")));
    }

    @Test
    public void shouldAcceptReadAndWriteAsSciTokenScopeWithUnrelatedScopes()
    {
        List<SciTokenScope> scopes = SciTokenScope.parseScope("email read write oidc");

        assertThat(scopes, contains(new SciTokenScope(READ, "/"), new SciTokenScope(WRITE, "/")));
    }

    @Test
    public void shouldAcceptQueueAsSciTokenScope()
    {
        List<SciTokenScope> scopes = SciTokenScope.parseScope("queue");

        assertThat(scopes, contains(new SciTokenScope(QUEUE, "/")));
    }

    @Test
    public void shouldAcceptExecuteAsSciTokenScope()
    {
        List<SciTokenScope> scopes = SciTokenScope.parseScope("execute");

        assertThat(scopes, contains(new SciTokenScope(EXECUTE, "/")));
    }

    @Test
    public void shouldNotAcceptReadWithOnlyColonAsSciTokenScope()
    {
        List<SciTokenScope> scopes = SciTokenScope.parseScope("read:");

        assertThat(scopes, is(empty()));
    }

    @Test
    public void shouldNotAcceptReadWithRelativePathAsSciTokenScope()
    {
        List<SciTokenScope> scopes = SciTokenScope.parseScope("read:foo/bar");

        assertThat(scopes, is(empty()));
    }

    @Test
    public void shouldAcceptReadWithPathAsSciTokenScope()
    {
        List<SciTokenScope> scopes = SciTokenScope.parseScope("read:/foo/bar");

        assertThat(scopes, contains(new SciTokenScope(READ, "/foo/bar")));
    }

    @Test
    public void shouldNotAcceptWrongPrefixAsSciTokenScope()
    {
        List<SciTokenScope> scopes = SciTokenScope.parseScope("https://dcache.org/v1/authz/read");

        assertThat(scopes, is(empty()));
    }

    @Test
    public void shouldAcceptPrefixReadAsSciTokenScope()
    {
        List<SciTokenScope> scopes = SciTokenScope.parseScope("https://scitokens.org/v1/authz/read");

        assertThat(scopes, contains(new SciTokenScope(READ, "/")));
    }

    @Test
    public void shouldNotAcceptPrefixReadWithOnlyColonAsSciTokenScope()
    {
        List<SciTokenScope> scopes = SciTokenScope.parseScope("https://scitokens.org/v1/authz/read:");

        assertThat(scopes, is(empty()));
    }

    @Test
    public void shouldNotAcceptPrefixReadWithRelativePathAsSciTokenScope()
    {
        List<SciTokenScope> scopes = SciTokenScope.parseScope("https://scitokens.org/v1/authz/read:foo/bar");

        assertThat(scopes, is(empty()));
    }


    @Test
    public void shouldIdentifyPrefixReadWithPathAsSciTokenScope()
    {
        List<SciTokenScope> scopes = SciTokenScope.parseScope("https://scitokens.org/v1/authz/read:/");

        assertThat(scopes, contains(new SciTokenScope(READ, "/")));
    }
}
