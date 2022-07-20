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
package diskCacheV111.services.space;

import java.util.NoSuchElementException;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import diskCacheV111.util.VOInfo;

import org.dcache.util.files.LineBasedParser.UnrecoverableParsingException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class LinkGroupAuthorizationFileParserTest
{
    private LinkGroupAuthorizationFileParser parser;

    @Before
    public void setup() {
        parser = new LinkGroupAuthorizationFileParser();
    }

    @Test
    public void shouldAcceptEmptyFile() {
        var results = parser.build();

        assertThat(results, anEmptyMap());
    }

    @Test(expected=UnrecoverableParsingException.class)
    public void shouldRejectFileWithWrongFirstLine() throws Exception {
        parser.accept("Foo");
    }

    // REVISIT: is this the correct exception?
    @Test(expected=NoSuchElementException.class)
    public void shouldRejectWithNoLinkgroupName() throws Exception {
        parser.accept("LinkGroup");
    }

    @Test
    public void shouldAcceptLinkgroupWithNoAuthzStatements() throws Exception {
        parser.accept("LinkGroup foo");

        var results = parser.build();

        assertThat(results, aMapWithSize(1));
        var lgAuthz = results.get("foo");
        assertThat(lgAuthz.getLinkGroupName(), equalTo("foo"));
        assertThat(lgAuthz.voinfos, empty());
        assertThat(lgAuthz.getVOInfoArray(), emptyArray());
    }

    @Test
    public void shouldAcceptAuthZWithRole() throws Exception {
        parser.accept("LinkGroup foo");
        parser.accept("/foo/Role=bar");

        var results = parser.build();

        assertThat(results, aMapWithSize(1));
        var lgAuthz = results.get("foo");
        assertThat(lgAuthz.getLinkGroupName(), equalTo("foo"));
        assertThat(lgAuthz.voinfos, hasSize(1));
        assertThat(lgAuthz.getVOInfoArray(), arrayWithSize(1));
        assertThat(lgAuthz.getVOInfoArray()[0], sameInstance(lgAuthz.voinfos.get(0)));
        VOInfo firstAuthz = lgAuthz.voinfos.get(0);
        assertThat(firstAuthz.getVoGroup(), equalTo("/foo"));
        assertThat(firstAuthz.getVoRole(), equalTo("bar"));
    }

    @Test
    public void shouldAcceptAuthZWithoutRole() throws Exception {
        parser.accept("LinkGroup foo");
        parser.accept("/foo");

        var results = parser.build();

        assertThat(results, aMapWithSize(1));
        var lgAuthz = results.get("foo");
        assertThat(lgAuthz.getLinkGroupName(), equalTo("foo"));
        assertThat(lgAuthz.voinfos, hasSize(1));
        assertThat(lgAuthz.getVOInfoArray(), arrayWithSize(1));
        assertThat(lgAuthz.getVOInfoArray()[0], sameInstance(lgAuthz.voinfos.get(0)));
        VOInfo firstAuthz = lgAuthz.voinfos.get(0);
        assertThat(firstAuthz.getVoGroup(), equalTo("/foo"));
        assertThat(firstAuthz.getVoRole(), equalTo("*")); // This is dubious.
    }

    @Test
    public void shouldAcceptAuthZWithPreceedingWhitespace() throws Exception {
        parser.accept("  LinkGroup foo");
        parser.accept("/foo");

        var results = parser.build();

        assertThat(results, aMapWithSize(1));
        var lgAuthz = results.get("foo");
        assertThat(lgAuthz.getLinkGroupName(), equalTo("foo"));
        assertThat(lgAuthz.voinfos, hasSize(1));
        assertThat(lgAuthz.getVOInfoArray(), arrayWithSize(1));
        assertThat(lgAuthz.getVOInfoArray()[0], sameInstance(lgAuthz.voinfos.get(0)));
        VOInfo firstAuthz = lgAuthz.voinfos.get(0);
        assertThat(firstAuthz.getVoGroup(), equalTo("/foo"));
        assertThat(firstAuthz.getVoRole(), equalTo("*")); // This is dubious.
    }

    @Test
    public void shouldAcceptAuthZWithTrailingWhitespace() throws Exception {
        parser.accept("LinkGroup foo    ");
        parser.accept("/foo");

        var results = parser.build();

        assertThat(results, aMapWithSize(1));
        var lgAuthz = results.get("foo");
        assertThat(lgAuthz.getLinkGroupName(), equalTo("foo"));
        assertThat(lgAuthz.voinfos, hasSize(1));
        assertThat(lgAuthz.getVOInfoArray(), arrayWithSize(1));
        assertThat(lgAuthz.getVOInfoArray()[0], sameInstance(lgAuthz.voinfos.get(0)));
        VOInfo firstAuthz = lgAuthz.voinfos.get(0);
        assertThat(firstAuthz.getVoGroup(), equalTo("/foo"));
        assertThat(firstAuthz.getVoRole(), equalTo("*")); // This is dubious.
    }

    @Test
    public void shouldIgnoreInitialComments() throws Exception {
        parser.accept("# This should be ignored");
        parser.accept("LinkGroup foo");
        parser.accept("/foo");

        var results = parser.build();

        assertThat(results, aMapWithSize(1));
        var lgAuthz = results.get("foo");
        assertThat(lgAuthz.getLinkGroupName(), equalTo("foo"));
        assertThat(lgAuthz.voinfos, hasSize(1));
        assertThat(lgAuthz.getVOInfoArray(), arrayWithSize(1));
        assertThat(lgAuthz.getVOInfoArray()[0], sameInstance(lgAuthz.voinfos.get(0)));
        VOInfo firstAuthz = lgAuthz.voinfos.get(0);
        assertThat(firstAuthz.getVoGroup(), equalTo("/foo"));
        assertThat(firstAuthz.getVoRole(), equalTo("*")); // This is dubious.
    }

    @Test
    public void shouldIgnoreCommentsAfterLinkGroup() throws Exception {
        parser.accept("LinkGroup foo");
        parser.accept("# This should be ignored");
        parser.accept("/foo");

        var results = parser.build();

        assertThat(results, aMapWithSize(1));
        var lgAuthz = results.get("foo");
        assertThat(lgAuthz.getLinkGroupName(), equalTo("foo"));
        assertThat(lgAuthz.voinfos, hasSize(1));
        assertThat(lgAuthz.getVOInfoArray(), arrayWithSize(1));
        assertThat(lgAuthz.getVOInfoArray()[0], sameInstance(lgAuthz.voinfos.get(0)));
        VOInfo firstAuthz = lgAuthz.voinfos.get(0);
        assertThat(firstAuthz.getVoGroup(), equalTo("/foo"));
        assertThat(firstAuthz.getVoRole(), equalTo("*")); // This is dubious.
    }

    @Test
    public void shouldIgnoreCommentsAfterAuthz() throws Exception {
        parser.accept("LinkGroup foo");
        parser.accept("/foo");
        parser.accept("# This should be ignored");

        var results = parser.build();

        assertThat(results, aMapWithSize(1));
        var lgAuthz = results.get("foo");
        assertThat(lgAuthz.getLinkGroupName(), equalTo("foo"));
        assertThat(lgAuthz.voinfos, hasSize(1));
        assertThat(lgAuthz.getVOInfoArray(), arrayWithSize(1));
        assertThat(lgAuthz.getVOInfoArray()[0], sameInstance(lgAuthz.voinfos.get(0)));
        VOInfo firstAuthz = lgAuthz.voinfos.get(0);
        assertThat(firstAuthz.getVoGroup(), equalTo("/foo"));
        assertThat(firstAuthz.getVoRole(), equalTo("*")); // This is dubious.
    }

    @Test
    public void shouldAcceptAuthZWithGroup() throws Exception {
        parser.accept("LinkGroup foo");
        parser.accept("my-group");

        var results = parser.build();

        assertThat(results, aMapWithSize(1));
        var lgAuthz = results.get("foo");
        assertThat(lgAuthz.getLinkGroupName(), equalTo("foo"));
        assertThat(lgAuthz.voinfos, hasSize(1));
        assertThat(lgAuthz.getVOInfoArray(), arrayWithSize(1));
        assertThat(lgAuthz.getVOInfoArray()[0], sameInstance(lgAuthz.voinfos.get(0)));
        VOInfo firstAuthz = lgAuthz.voinfos.get(0);
        assertThat(firstAuthz.getVoGroup(), equalTo("my-group"));
        assertThat(firstAuthz.getVoRole(), equalTo("*")); // This is dubious.
    }

    @Ignore("see https://github.com/dCache/dcache/issues/6739")
    @Test
    public void shouldAcceptMultipleLinkgroups() throws Exception {
        parser.accept("LinkGroup foo");
        parser.accept("group-foo");
        parser.accept("LinkGroup bar");
        parser.accept("group-bar");

        var results = parser.build();

        assertThat(results, aMapWithSize(2));

        var lgFooAuthz = results.get("foo");
        assertThat(lgFooAuthz.getLinkGroupName(), equalTo("foo"));
        assertThat(lgFooAuthz.voinfos, hasSize(1));
        assertThat(lgFooAuthz.getVOInfoArray(), arrayWithSize(1));
        assertThat(lgFooAuthz.getVOInfoArray()[0], sameInstance(lgFooAuthz.voinfos.get(0)));
        VOInfo firstAuthzForFoo = lgFooAuthz.voinfos.get(0);
        assertThat(firstAuthzForFoo.getVoGroup(), equalTo("group-foo"));
        assertThat(firstAuthzForFoo.getVoRole(), equalTo("*")); // This is dubious.

        var lgBarAuthz = results.get("bar");
        assertThat(lgBarAuthz.getLinkGroupName(), equalTo("bar"));
        assertThat(lgBarAuthz.voinfos, hasSize(1));
        assertThat(lgBarAuthz.getVOInfoArray(), arrayWithSize(1));
        assertThat(lgBarAuthz.getVOInfoArray()[0], sameInstance(lgBarAuthz.voinfos.get(0)));
        VOInfo firstAuthzForBar = lgBarAuthz.voinfos.get(0);
        assertThat(firstAuthzForBar.getVoGroup(), equalTo("group-bar"));
        assertThat(firstAuthzForBar.getVoRole(), equalTo("*")); // This is dubious.
    }

    @Test
    public void shouldAcceptMultipleLinkgroupsWithEmptyLine() throws Exception {
        parser.accept("LinkGroup foo");
        parser.accept("group-foo");
        parser.accept("");
        parser.accept("LinkGroup bar");
        parser.accept("group-bar");

        var results = parser.build();

        assertThat(results, aMapWithSize(2));

        var lgFooAuthz = results.get("foo");
        assertThat(lgFooAuthz.getLinkGroupName(), equalTo("foo"));
        assertThat(lgFooAuthz.voinfos, hasSize(1));
        assertThat(lgFooAuthz.getVOInfoArray(), arrayWithSize(1));
        assertThat(lgFooAuthz.getVOInfoArray()[0], sameInstance(lgFooAuthz.voinfos.get(0)));
        VOInfo firstAuthzForFoo = lgFooAuthz.voinfos.get(0);
        assertThat(firstAuthzForFoo.getVoGroup(), equalTo("group-foo"));
        assertThat(firstAuthzForFoo.getVoRole(), equalTo("*")); // This is dubious.

        var lgBarAuthz = results.get("bar");
        assertThat(lgBarAuthz.getLinkGroupName(), equalTo("bar"));
        assertThat(lgBarAuthz.voinfos, hasSize(1));
        assertThat(lgBarAuthz.getVOInfoArray(), arrayWithSize(1));
        assertThat(lgBarAuthz.getVOInfoArray()[0], sameInstance(lgBarAuthz.voinfos.get(0)));
        VOInfo firstAuthzForBar = lgBarAuthz.voinfos.get(0);
        assertThat(firstAuthzForBar.getVoGroup(), equalTo("group-bar"));
        assertThat(firstAuthzForBar.getVoRole(), equalTo("*")); // This is dubious.
    }
}
