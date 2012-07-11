package org.dcache.gplazma.plugins;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.dcache.gplazma.plugins.PrincipalSetMaker.aSetOfPrincipals;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.security.Principal;
import java.util.Set;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.util.NameRolePair;
import org.dcache.gplazma.AuthenticationException;
import org.globus.gsi.jaas.GlobusPrincipal;
import org.junit.Test;
import org.hamcrest.Matcher;


public class VoRoleMapPluginTest
{
    private LineSource _source;
    private VoRoleMapFileMaker _content;
    private Set<Principal> _principals;
    private Set<Principal> _authorizedPrincipals;

    @Test(expected=NullPointerException.class)
    public void shouldThrowNPEWhenGivenNullArgs() throws AuthenticationException
    {
        givenVoRoleMapFile().thatIsEmpty();

        whenMapPluginCalledWith(null, null);
    }


    @Test
    public void shouldAddPrimaryGroupWhenDnAndPrimaryFqanMatchEntryWithDnAndFqanWithNulls()
        throws AuthenticationException
    {
        givenVoRoleMapFile().withSingleLine(
                "\"/O=ACME/CN=Wile E Coyote\" \"/acme/Role=NULL/Capability=NULL\" acme01");

        whenMapPluginCalledWith(aSetOfPrincipals().
                withDn("/O=ACME/CN=Wile E Coyote").
                withPrimaryFqan("/acme"));

        assertThat(_principals, hasDn("/O=ACME/CN=Wile E Coyote"));
        assertThat(_principals, hasPrimaryFqan("/acme"));
        assertThat(_principals, hasPrimaryGroupName("acme01"));
        assertThat(_principals, not(hasGroupName("acme01")));

        assertThat(_authorizedPrincipals, hasDn("/O=ACME/CN=Wile E Coyote"));
        assertThat(_authorizedPrincipals, hasPrimaryFqan("/acme"));
    }


    @Test
    public void shouldAddPrimaryGroupWhenDnAndPrimaryFqanMatchEntryWithDnAndFqan()
        throws AuthenticationException
    {
        givenVoRoleMapFile().
                withSingleLine("\"/O=ACME/CN=Wile E Coyote\" \"/acme\" acme01");

        whenMapPluginCalledWith(aSetOfPrincipals().
                withDn("/O=ACME/CN=Wile E Coyote").
                withPrimaryFqan("/acme"));

        assertThat(_principals, hasDn("/O=ACME/CN=Wile E Coyote"));
        assertThat(_principals, hasPrimaryFqan("/acme"));
        assertThat(_principals, hasPrimaryGroupName("acme01"));
        assertThat(_principals, not(hasGroupName("acme01")));

        assertThat(_authorizedPrincipals, hasDn("/O=ACME/CN=Wile E Coyote"));
        assertThat(_authorizedPrincipals, hasPrimaryFqan("/acme"));
    }


    @Test
    public void shouldIgnoreCommentAndAdditionalWhiteSpaceAndEmptyLines()
        throws AuthenticationException
    {
        givenVoRoleMapFile().withLines(
                "",
                "#  This is a comment, which should be ignored",
                "",
                " \t \"/O=ACME/CN=Wile E Coyote\" \t \"/acme\" acme01 \t # this is a comment",
                "# this should be ignored, too");

        whenMapPluginCalledWith(aSetOfPrincipals().
                withDn("/O=ACME/CN=Wile E Coyote").
                withPrimaryFqan("/acme"));

        assertThat(_principals, hasDn("/O=ACME/CN=Wile E Coyote"));
        assertThat(_principals, hasPrimaryFqan("/acme"));
        assertThat(_principals, hasPrimaryGroupName("acme01"));
        assertThat(_principals, not(hasGroupName("acme01")));

        assertThat(_authorizedPrincipals, hasDn("/O=ACME/CN=Wile E Coyote"));
        assertThat(_authorizedPrincipals, hasPrimaryFqan("/acme"));
    }


    @Test
    public void shouldAddGroupWhenDnAndFqanMatchEntryWithDnAndFqan()
        throws AuthenticationException
    {
        givenVoRoleMapFile().
                withSingleLine("\"/O=ACME/CN=Wile E Coyote\" \"/acme\" acme01");

        whenMapPluginCalledWith(aSetOfPrincipals().
                withDn("/O=ACME/CN=Wile E Coyote").
                withPrimaryFqan("/acme/Role=genius").
                withFqan("/acme"));

        assertThat(_principals, hasDn("/O=ACME/CN=Wile E Coyote"));
        assertThat(_principals, hasPrimaryFqan("/acme/Role=genius"));
        assertThat(_principals, hasFqan("/acme"));
        assertThat(_principals, hasGroupName("acme01")); // FIXME should be primaryGroupName?

        assertThat(_authorizedPrincipals, hasDn("/O=ACME/CN=Wile E Coyote"));
        assertThat(_authorizedPrincipals, hasFqan("/acme"));
    }



    @Test(expected=AuthenticationException.class)
    public void shouldNotMatchWhenDnDoesNotMatchAndFqanMatches()
        throws AuthenticationException
    {
        givenVoRoleMapFile().
                withSingleLine("\"/O=ACME/CN=Wile E Coyote\" \"/acme\" acme01");

        whenMapPluginCalledWith(aSetOfPrincipals().
                withDn("/O=ACME/CN=Road Runner").
                withPrimaryFqan("/acme"));
    }



    @Test(expected=AuthenticationException.class)
    public void shouldNotMatchWhenDnMatchesAndFqanDoesNotMatch()
        throws AuthenticationException
    {
        givenVoRoleMapFile().
                withSingleLine("\"/O=ACME/CN=Wile E Coyote\" \"/acme\" acme01");

        whenMapPluginCalledWith(aSetOfPrincipals().
                withDn("/O=ACME/CN=Wile E Coyote").
                withPrimaryFqan("/atlas"));
    }



    @Test
    public void shouldAddGroupWithDnAndFqanWhenMatchesFirstLine()
        throws AuthenticationException
    {
        givenVoRoleMapFile().withLines(
                "\"/O=ACME/CN=Wile E Coyote\" \"/acme\" acme01",
                "\"/O=ACME/CN=Road Runner\"   \"/acme\" acme02");

        whenMapPluginCalledWith(aSetOfPrincipals().
                withDn("/O=ACME/CN=Wile E Coyote").
                withPrimaryFqan("/acme"));

        assertThat(_principals, hasDn("/O=ACME/CN=Wile E Coyote"));
        assertThat(_principals, hasPrimaryFqan("/acme"));
        assertThat(_principals, hasPrimaryGroupName("acme01"));
        assertThat(_principals, not(hasGroupName("acme01")));
        assertThat(_principals, not(hasPrimaryGroupName("acme02")));
        assertThat(_principals, not(hasGroupName("acme02")));

        assertThat(_authorizedPrincipals, hasDn("/O=ACME/CN=Wile E Coyote"));
        assertThat(_authorizedPrincipals, hasPrimaryFqan("/acme"));
    }


    @Test
    public void shouldAddGroupWithDnAndFqanWhenMatchesSecondLine()
        throws AuthenticationException
    {
        givenVoRoleMapFile().withLines(
                "\"/O=ACME/CN=Road Runner\"   \"/acme\" acme02",
                "\"/O=ACME/CN=Wile E Coyote\" \"/acme\" acme01");

        whenMapPluginCalledWith(aSetOfPrincipals().
                withDn("/O=ACME/CN=Wile E Coyote").
                withPrimaryFqan("/acme"));

        assertThat(_principals, hasDn("/O=ACME/CN=Wile E Coyote"));
        assertThat(_principals, hasPrimaryFqan("/acme"));
        assertThat(_principals, hasPrimaryGroupName("acme01"));
        assertThat(_principals, not(hasGroupName("acme01")));
        assertThat(_principals, not(hasPrimaryGroupName("acme02")));
        assertThat(_principals, not(hasGroupName("acme02")));

        assertThat(_authorizedPrincipals, hasDn("/O=ACME/CN=Wile E Coyote"));
        assertThat(_authorizedPrincipals, hasPrimaryFqan("/acme"));
    }


    @Test
    public void shouldAddTwoGroupsWhenDnMatchesBothLinesAndTwoFqansEachMatchALineWithFirstLineMatchesPrimaryFqan()
        throws AuthenticationException
    {
        givenVoRoleMapFile().withLines(
                "\"/O=ACME/CN=Wile E Coyote\" \"/acme/Role=genius\" genius01",
                "\"/O=ACME/CN=Wile E Coyote\" \"/acme\"             acme01");

        whenMapPluginCalledWith(aSetOfPrincipals().
                withDn("/O=ACME/CN=Wile E Coyote").
                withPrimaryFqan("/acme/Role=genius").
                withFqan("/acme"));

        assertThat(_principals, hasDn("/O=ACME/CN=Wile E Coyote"));
        assertThat(_principals, hasPrimaryFqan("/acme/Role=genius"));
        assertThat(_principals, hasFqan("/acme"));
        assertThat(_principals, hasPrimaryGroupName("genius01"));
        assertThat(_principals, not(hasGroupName("genius01")));
        assertThat(_principals, not(hasPrimaryGroupName("acme01")));
        assertThat(_principals, hasGroupName("acme01"));

        assertThat(_authorizedPrincipals, hasDn("/O=ACME/CN=Wile E Coyote"));
        assertThat(_authorizedPrincipals, hasPrimaryFqan("/acme/Role=genius"));
        assertThat(_authorizedPrincipals, hasFqan("/acme"));
    }


    @Test
    public void shouldAddTwoGroupsWhenDnMatchesBothLinesAndTwoFqansEachMatchALineWithSecondLineMatchesPrimaryFqan()
        throws AuthenticationException
    {
        givenVoRoleMapFile().withLines(
                "\"/O=ACME/CN=Wile E Coyote\" \"/acme/Role=genius\" genius01",
                "\"/O=ACME/CN=Wile E Coyote\" \"/acme\"             acme01");

        whenMapPluginCalledWith(aSetOfPrincipals().
                withDn("/O=ACME/CN=Wile E Coyote").
                withPrimaryFqan("/acme").
                withFqan("/acme/Role=genius"));

        assertThat(_principals, hasDn("/O=ACME/CN=Wile E Coyote"));
        assertThat(_principals, hasPrimaryFqan("/acme"));
        assertThat(_principals, hasFqan("/acme/Role=genius"));
        assertThat(_principals, hasPrimaryGroupName("acme01"));
        assertThat(_principals, not(hasGroupName("acme01")));
        assertThat(_principals, not(hasPrimaryGroupName("genius01")));
        assertThat(_principals, hasGroupName("genius01"));

        assertThat(_authorizedPrincipals, hasDn("/O=ACME/CN=Wile E Coyote"));
        assertThat(_authorizedPrincipals, hasPrimaryFqan("/acme"));
        assertThat(_authorizedPrincipals, hasFqan("/acme/Role=genius"));
    }


    @Test
    public void shouldMatchUnquotedWildcardDnAndMatchingPrimaryFqan()
        throws AuthenticationException
    {
        givenVoRoleMapFile().withSingleLine("* \"/acme\" acme01");

        whenMapPluginCalledWith(aSetOfPrincipals().
                withDn("/O=ACME/CN=Wile E Coyote").
                withPrimaryFqan("/acme"));

        assertThat(_principals, hasDn("/O=ACME/CN=Wile E Coyote"));
        assertThat(_principals, hasPrimaryFqan("/acme"));
        assertThat(_principals, hasPrimaryGroupName("acme01"));
        assertThat(_principals, not(hasGroupName("acme01")));

        assertThat(_authorizedPrincipals, hasDn("/O=ACME/CN=Wile E Coyote"));
        assertThat(_authorizedPrincipals, hasPrimaryFqan("/acme"));
    }


    @Test
    public void shouldMatchQuotedWildcardDnAndMatchingPrimaryFqan()
        throws AuthenticationException
    {
        givenVoRoleMapFile().withSingleLine("\"*\" \"/acme\" acme01");

        whenMapPluginCalledWith(aSetOfPrincipals().
                withDn("/O=ACME/CN=Wile E Coyote").
                withPrimaryFqan("/acme"));

        assertThat(_principals, hasDn("/O=ACME/CN=Wile E Coyote"));
        assertThat(_principals, hasPrimaryFqan("/acme"));
        assertThat(_principals, hasPrimaryGroupName("acme01"));

        assertThat(_authorizedPrincipals, hasDn("/O=ACME/CN=Wile E Coyote"));
        assertThat(_authorizedPrincipals, hasPrimaryFqan("/acme"));
    }


    @Test
    public void shouldMatchQuotedWildcardDnAndMatchingFqan()
        throws AuthenticationException
    {
        givenVoRoleMapFile().withSingleLine("\"*\" \"/acme\" acme01");

        whenMapPluginCalledWith(aSetOfPrincipals().
                withDn("/O=ACME/CN=Wile E Coyote").
                withPrimaryFqan("/acme/Role=genius").
                withFqan("/acme"));

        assertThat(_principals, hasDn("/O=ACME/CN=Wile E Coyote"));
        assertThat(_principals, hasPrimaryFqan("/acme/Role=genius"));
        assertThat(_principals, hasFqan("/acme"));
        assertThat(_principals, hasGroupName("acme01")); // FIXME should be primaryGroupName

        assertThat(_authorizedPrincipals, hasDn("/O=ACME/CN=Wile E Coyote"));
        assertThat(_authorizedPrincipals, hasFqan("/acme"));
        assertThat(_authorizedPrincipals, not(hasPrimaryFqan("/acme/Role=genius")));
    }


    @Test
    public void shouldHaveTwoMatchQuotedWildcardDnAndMatchingFqanAndPrimaryFqan()
        throws AuthenticationException
    {
        givenVoRoleMapFile().withLines(
                "\"*\" \"/acme\"             acme01",
                "\"*\" \"/acme/Role=genius\" genius01");

        whenMapPluginCalledWith(aSetOfPrincipals().
                withDn("/O=ACME/CN=Wile E Coyote").
                withPrimaryFqan("/acme").
                withFqan("/acme/Role=genius"));

        assertThat(_principals, hasDn("/O=ACME/CN=Wile E Coyote"));
        assertThat(_principals, hasPrimaryFqan("/acme"));
        assertThat(_principals, hasFqan("/acme/Role=genius"));
        assertThat(_principals, hasPrimaryGroupName("acme01"));
        assertThat(_principals, not(hasGroupName("acme01")));
        assertThat(_principals, not(hasPrimaryGroupName("genius01")));
        assertThat(_principals, hasGroupName("genius01"));

        assertThat(_authorizedPrincipals, hasDn("/O=ACME/CN=Wile E Coyote"));
        assertThat(_authorizedPrincipals, hasPrimaryFqan("/acme"));
        assertThat(_authorizedPrincipals, hasFqan("/acme/Role=genius"));
    }



    public static Matcher<Iterable<? super GlobusPrincipal>> hasDn(String dn)
    {
        return hasItem(new GlobusPrincipal(dn));
    }

    public static Matcher<Iterable<? super UidPrincipal>> hasUid(int uid)
    {
        return hasItem(new UidPrincipal(uid));
    }

    public static Matcher<Iterable<? super FQANPrincipal>>
            hasFqan(String fqan)
    {
        return hasItem(new FQANPrincipal(fqan));
    }

    public static Matcher<Iterable<? super FQANPrincipal>>
            hasPrimaryFqan(String fqan)
    {
        return hasItem(new FQANPrincipal(fqan, true));
    }

    public static Matcher<Iterable<? super GroupNamePrincipal>>
            hasGroupName(String group)
    {
        return hasItem(new GroupNamePrincipal(group));
    }

    public static Matcher<Iterable<? super UserNamePrincipal>>
            hasUserName(String group)
    {
        return hasItem(new UserNamePrincipal(group));
    }

    public static Matcher<Iterable<? super GroupNamePrincipal>>
            hasPrimaryGroupName(String group)
    {
        return hasItem(new GroupNamePrincipal(group, true));
    }


    private void whenMapPluginCalledWith(PrincipalSetMaker maker)
            throws AuthenticationException
    {
        Set<Principal> principals = Sets.newHashSet(maker.build());
        whenMapPluginCalledWith(principals, new HashSet<Principal>());
    }

    private void whenMapPluginCalledWith(Set<Principal> principals,
            Set<Principal> authorizedPrincipals) throws AuthenticationException
    {
        _principals = principals;
        _authorizedPrincipals = authorizedPrincipals;

        VOMapLineParser parser = new VOMapLineParser();

        SourceBackedPredicateMap<NameRolePair,String> map =
                new SourceBackedPredicateMap<NameRolePair,String>(_source,
                parser);

        GPlazmaMappingPlugin plugin = new VoRoleMapPlugin(map);
        plugin.map(_principals, _authorizedPrincipals);
    }

    private VoRoleMapFileMaker givenVoRoleMapFile()
    {
        _content = new VoRoleMapFileMaker();
        return _content;
    }

    /** A builder for the content of a VoRoleMapFile with a fluent interface. */
    private class VoRoleMapFileMaker
    {
        private final List<String> _lines = new LinkedList<String>();

        private VoRoleMapFileMaker()
        {
            update();
        }

        public VoRoleMapFileMaker thatIsEmpty()
        {
            _lines.clear();
            update();
            return this;
        }

        public VoRoleMapFileMaker withSingleLine(String line)
        {
            _lines.clear();
            _lines.add(line);
            update();
            return this;
        }

        public VoRoleMapFileMaker withLines(String... line)
        {
            _lines.clear();
            Collections.addAll(_lines, line);
            update();
            return this;
        }

        private void update()
        {
            _source = new MemoryLineSource(_lines);
        }
   }
}
