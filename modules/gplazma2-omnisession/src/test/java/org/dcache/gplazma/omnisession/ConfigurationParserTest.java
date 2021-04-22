/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.omnisession;

import org.junit.Test;

import java.util.List;

import diskCacheV111.util.FsPath;

import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.MaxUploadSize;
import org.dcache.auth.attributes.PrefixRestriction;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.omnisession.LineBasedParser.UnrecoverableParsingException;
import org.dcache.util.PrincipalSetMaker;

import static org.dcache.util.ByteUnit.BYTES;
import static org.dcache.util.ByteUnit.GiB;
import static org.dcache.util.PrincipalSetMaker.aSetOfPrincipals;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

public class ConfigurationParserTest
{
    private Configuration configuration;

    @Test(expected=AuthenticationException.class)
    public void shouldRejectUsersIfFileIsEmpty() throws Exception
    {
        givenConfig();

        attributesFor(aSetOfPrincipals().withUsername("paul"));
    }

    @Test
    public void shouldMatchLineWithUsername() throws Exception
    {
        givenConfig("username:paul root:/ home:/");

        var loginAttributes = attributesFor(aSetOfPrincipals().withUsername("paul"));

        assertThat(loginAttributes, containsInAnyOrder(new RootDirectory("/"),
                new HomeDirectory("/")));
    }

    @Test
    public void shouldAcceptCommentLine() throws Exception
    {
        givenConfig("# This is a line with a comment",
                "username:paul root:/ home:/");

        var loginAttributes = attributesFor(aSetOfPrincipals().withUsername("paul"));

        assertThat(loginAttributes, containsInAnyOrder(new RootDirectory("/"),
                new HomeDirectory("/")));
    }

    @Test
    public void shouldAcceptEmptyLine() throws Exception
    {
        givenConfig("",
                "username:paul root:/ home:/");

        var loginAttributes = attributesFor(aSetOfPrincipals().withUsername("paul"));

        assertThat(loginAttributes, containsInAnyOrder(new RootDirectory("/"),
                new HomeDirectory("/")));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldNotMatchWithWrongUsername() throws Exception
    {
        givenConfig("username:paul root:/ home:/");

        attributesFor(aSetOfPrincipals().withUsername("tigran"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRejectLineWithoutAttributes() throws Exception
    {
        givenConfig("username:paul");

        attributesFor(aSetOfPrincipals().withUsername("paul"));
    }

    @Test
    public void shouldMatchLineWithUsernameAndRoot() throws Exception
    {
        givenConfig("username:paul root:/Users/paul");

        var loginAttributes = attributesFor(aSetOfPrincipals().withUsername("paul"));

        assertThat(loginAttributes, contains(new RootDirectory("/Users/paul")));
    }

    @Test
    public void shouldMatchLineWithUsernameAndHome() throws Exception
    {
        givenConfig("username:paul home:/Users/paul");

        var loginAttributes = attributesFor(aSetOfPrincipals().withUsername("paul"));

        assertThat(loginAttributes, contains(new HomeDirectory("/Users/paul")));
    }

    @Test
    public void shouldMatchLineWithUsernameAndReadOnly() throws Exception
    {
        givenConfig("username:paul read-only");

        var loginAttributes = attributesFor(aSetOfPrincipals().withUsername("paul"));

        assertThat(loginAttributes, contains(Restrictions.readOnly()));
    }

    @Test
    public void shouldMatchLineWithUsernameAndPrefix() throws Exception
    {
        givenConfig("username:paul prefix:/path");

        var attributes = attributesFor(aSetOfPrincipals().withUsername("paul"));

        assertThat(attributes, contains(new PrefixRestriction(FsPath.create("/path"))));
    }

    @Test
    public void shouldMatchLineWithUsernameAndMaxUpload() throws Exception
    {
        givenConfig("username:paul max-upload:5GiB");

        var attributes = attributesFor(aSetOfPrincipals().withUsername("paul"));

        assertThat(attributes.size(), equalTo(1));
        var restriction = (MaxUploadSize)attributes.get(0);

        assertThat(restriction.getMaximumSize(), equalTo(BYTES.convert(5l, GiB)));
    }

    @Test
    public void shouldMatchLineWithUsernameAndPrefixAndRoot() throws Exception
    {
        givenConfig("username:paul root:/root-path prefix:/prefix-path");

        var attributes = attributesFor(aSetOfPrincipals().withUsername("paul"));

        assertThat(attributes, containsInAnyOrder(
                new PrefixRestriction(FsPath.create("/prefix-path")),
                new RootDirectory("/root-path")));
    }

    @Test
    public void shouldMatchFirstOfTwoLineWithUsernameAndRoot() throws Exception
    {
        givenConfig("username:paul root:/pauls-root",
                "username:tigran root:/tigrans-root");

        var attributes = attributesFor(aSetOfPrincipals().withUsername("paul"));

        assertThat(attributes, contains(new RootDirectory("/pauls-root")));
    }

    @Test
    public void shouldMatchSecondOfTwoLineWithUsernameAndRoot() throws Exception
    {
        givenConfig("username:tigran root:/tigrans-root",
                "username:paul root:/pauls-root");

        var attributes = attributesFor(aSetOfPrincipals().withUsername("paul"));

        assertThat(attributes, contains(new RootDirectory("/pauls-root")));
    }

    @Test
    public void shouldMatchFirstOfThreeLineWithUsernameAndRoot() throws Exception
    {
        givenConfig("username:paul root:/pauls-root",
                "username:tigran root:/tigrans-root",
                "username:lea root:/leas-root");

        var attributes = attributesFor(aSetOfPrincipals().withUsername("paul"));

        assertThat(attributes, contains(new RootDirectory("/pauls-root")));
    }

    @Test
    public void shouldMatchSecondOfThreeLineWithUsernameAndRoot() throws Exception
    {
        givenConfig("username:tigran root:/tigrans-root",
                "username:paul root:/pauls-root",
                "username:lea root:/leas-root");

        var attributes = attributesFor(aSetOfPrincipals().withUsername("paul"));

        assertThat(attributes, contains(new RootDirectory("/pauls-root")));
    }

    @Test
    public void shouldMatchThirdOfThreeLineWithUsernameAndRoot() throws Exception
    {
        givenConfig("username:tigran root:/tigrans-root",
                "username:lea root:/leas-root",
                "username:paul root:/pauls-root");

        var attributes = attributesFor(aSetOfPrincipals().withUsername("paul"));

        assertThat(attributes, contains(new RootDirectory("/pauls-root")));
    }

    @Test
    public void shouldMatchDefaultWithRoot() throws Exception
    {
        givenConfig("DEFAULT root:/general-root");

        var attributes = attributesFor(aSetOfPrincipals().withUsername("paul"));

        assertThat(attributes, contains(new RootDirectory("/general-root")));
    }

    @Test
    public void shouldMatchFirstLineWithDefault() throws Exception
    {
        givenConfig("username:paul root:/pauls-root",
                "DEFAULT root:/general-root");

        var attributes = attributesFor(aSetOfPrincipals().withUsername("paul"));

        assertThat(attributes, contains(new RootDirectory("/pauls-root")));
    }

    @Test
    public void shouldMatchDefaultLineWithUserAndDefault() throws Exception
    {
        givenConfig("username:tigran root:/tigrans-root",
                "DEFAULT root:/general-root");

        var attributes = attributesFor(aSetOfPrincipals().withUsername("paul"));

        assertThat(attributes, contains(new RootDirectory("/general-root")));
    }

    @Test
    public void shouldMatchSecondLineWithFirstLineDefault() throws Exception
    {
        givenConfig("DEFAULT root:/general-root",
                "username:paul root:/pauls-root");

        var attributes = attributesFor(aSetOfPrincipals().withUsername("paul"));

        assertThat(attributes, contains(new RootDirectory("/pauls-root")));
    }

    @Test
    public void shouldMatchFirstLineAndDefault() throws Exception
    {
        givenConfig("username:paul read-only",
                "DEFAULT home:/ root:/");

        var attributes = attributesFor(aSetOfPrincipals().withUsername("paul"));

        assertThat(attributes, containsInAnyOrder(new HomeDirectory("/"),
                new RootDirectory("/"), Restrictions.readOnly()));
    }

    @Test
    public void shouldMatchFirstLineAndSecondLineAndDefault() throws Exception
    {
        givenConfig("username:paul read-only",
                "gid:1000 home:/",
                "DEFAULT root:/");

        var attributes = attributesFor(aSetOfPrincipals().withUsername("paul").withGid(1000));

        assertThat(attributes, containsInAnyOrder(new HomeDirectory("/"),
                new RootDirectory("/"), Restrictions.readOnly()));
    }

    @Test
    public void shouldIgnoreRepeatedDeclarationInSubsequentLines() throws Exception
    {
        givenConfig("username:paul home:/Users/paul",
                "gid:1000 home:/group-1000",
                "DEFAULT home:/");

        var attributes = attributesFor(aSetOfPrincipals().withUsername("paul").withGid(1000));

        assertThat(attributes, contains(new HomeDirectory("/Users/paul")));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldFailBadAttributeOnMatchedLine() throws Exception
    {
        givenConfig("username:paul INVALID",
                "username:tigran read-only");

        attributesFor(aSetOfPrincipals().withUsername("paul"));
    }

    @Test
    public void shouldIgnoreBadAttributeOnUnmatchedLine() throws Exception
    {
        givenConfig("username:paul read-only",
                "username:tigran INVALID");

        var attributes = attributesFor(aSetOfPrincipals().withUsername("paul"));

        assertThat(attributes, contains(Restrictions.readOnly()));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldFailBadMaxpUload() throws Exception
    {
        givenConfig("username:paul max-upload:INVALID");

        attributesFor(aSetOfPrincipals().withUsername("paul"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldFailAttributeDefinedTwice() throws Exception
    {
        givenConfig("username:paul max-upload:1GiB max-upload:2GiB");

        attributesFor(aSetOfPrincipals().withUsername("paul"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldFailUnknownType() throws Exception
    {
        givenConfig("username:paul INVALID:INVALID");

        attributesFor(aSetOfPrincipals().withUsername("paul"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldFailMissingType() throws Exception
    {
        givenConfig("username:paul :INVALID");

        attributesFor(aSetOfPrincipals().withUsername("paul"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldFailMissingArgument() throws Exception
    {
        givenConfig("username:paul INVALID:");

        attributesFor(aSetOfPrincipals().withUsername("paul"));
    }

    @Test(expected=UnrecoverableParsingException.class)
    public void shouldRejectConfigWithBadPredicate() throws Exception
    {
        ConfigurationParser parser = new ConfigurationParser();
        parser.accept("BAD-PREDICATE read-only");
    }

    @Test(expected=UnrecoverableParsingException.class)
    public void shouldRejectConfigWithMultipleDefault() throws Exception
    {
        ConfigurationParser parser = new ConfigurationParser();
        parser.accept("DEFAULT read-only");
        parser.accept("DEFAULT read-only");
    }

    private List<LoginAttribute> attributesFor(PrincipalSetMaker maker)
            throws AuthenticationException
    {
        return configuration.attributesFor(maker.build());
    }

    private void givenConfig(String...lines)
    {
        ConfigurationParser parser = new ConfigurationParser();
        for (String line : lines) {
            try {
                parser.accept(line);
            } catch (UnrecoverableParsingException e) {
                fail("Parsing line \"" + line + "\" failed unexpectedly: "
                        + e.getMessage());
            }
        }
        configuration = parser.build();
    }
}
