package org.dcache.gplazma.plugins;

import com.google.common.collect.Sets;
import org.globus.gsi.jaas.GlobusPrincipal;
import org.hamcrest.Matcher;
import org.junit.Test;

import java.security.Principal;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.util.NameRolePair;

import static org.dcache.gplazma.plugins.PrincipalSetMaker.aSetOfPrincipals;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;


public class VoRoleMapPluginTest
{
    private LineSource _source;
    private VoRoleMapFileMaker _content;
    private Set<Principal> _principals;

    @Test(expected=NullPointerException.class)
    public void shouldThrowNPEWhenGivenNullArgs() throws AuthenticationException
    {
        givenVoRoleMapFile().thatIsEmpty();

        whenMapPluginCalledWith((Set<Principal>) null);
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
    }

    @Test
    public void shouldMatchLongestPrefixForPrimaryFqan() throws AuthenticationException
    {
        givenVoRoleMapFile().withLines(
                "\"*\" \"/cms\" cms001");

        whenMapPluginCalledWith(aSetOfPrincipals()
                .withDn("/DC=es/DC=irisgrid/O=ciemat/CN=antonio-delgado-peris")
                .withPrimaryFqan("/cms/escms")
                .withFqan("/cms"));

        assertThat(_principals, hasPrimaryGroupName("cms001"));
        assertThat(_principals, hasDn("/DC=es/DC=irisgrid/O=ciemat/CN=antonio-delgado-peris"));
        assertThat(_principals, hasPrimaryFqan("/cms/escms"));
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
        whenMapPluginCalledWith(principals);
    }

    private void whenMapPluginCalledWith(Set<Principal> principals) throws AuthenticationException
    {
        _principals = principals;

        VOMapLineParser parser = new VOMapLineParser();

        SourceBackedPredicateMap<NameRolePair,String> map =
                new SourceBackedPredicateMap<>(_source,
                parser);

        GPlazmaMappingPlugin plugin = new VoRoleMapPlugin(map);
        plugin.map(_principals);
    }

    private VoRoleMapFileMaker givenVoRoleMapFile()
    {
        _content = new VoRoleMapFileMaker();
        return _content;
    }

    /** A builder for the content of a VoRoleMapFile with a fluent interface. */
    private class VoRoleMapFileMaker
    {
        private final List<String> _lines = new LinkedList<>();

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
