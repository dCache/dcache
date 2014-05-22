package org.dcache.gplazma.plugins;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.Principal;
import java.util.Set;

import org.dcache.auth.GidPrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.LoginGidPrincipal;
import org.dcache.auth.LoginNamePrincipal;
import org.dcache.auth.LoginUidPrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.plugins.AuthzDbPlugin.PrincipalType;
import org.dcache.gplazma.plugins.AuthzMapLineParser.UserAuthzInformation;

import static org.dcache.gplazma.plugins.AuthzDbPlugin.PrincipalType.*;
import static org.junit.Assert.assertEquals;

public class AuthzDbPluginTest
{
    private final static URL TEST_FIXTURE =
        Resources.getResource("org/dcache/gplazma/plugins/authzdb.fixture");

    private SourceBackedPredicateMap<String,UserAuthzInformation> testFixture;

    @Before
    public void setup()
        throws IOException
    {
        testFixture =
            new SourceBackedPredicateMap<>(new MemoryLineSource(Resources.readLines(TEST_FIXTURE, Charset.defaultCharset())), new AuthzMapLineParser());
    }

    public void check(Set<? extends Principal> input,
                      Set<? extends Principal> output)
        throws AuthenticationException
    {
        AuthzDbPlugin plugin =
            new AuthzDbPlugin(testFixture,
                              ImmutableList.of(UID,LOGIN,USER,GROUP),
                              ImmutableList.of(GID,LOGIN,GROUP,USER));
        Set<Principal> principals = Sets.newHashSet(input);
        plugin.map(principals);
        assertEquals(output, principals);
    }

    @Test
    public void testParseOrder()
    {
        assertEquals(ImmutableList.<PrincipalType>of(),
                     AuthzDbPlugin.parseOrder(""));
        assertEquals(ImmutableList.of(UID),
                     AuthzDbPlugin.parseOrder("uid"));
        assertEquals(ImmutableList.of(GID),
                     AuthzDbPlugin.parseOrder("gid"));
        assertEquals(ImmutableList.of(LOGIN),
                     AuthzDbPlugin.parseOrder("login"));
        assertEquals(ImmutableList.of(USER),
                     AuthzDbPlugin.parseOrder("user"));
        assertEquals(ImmutableList.of(GROUP),
                     AuthzDbPlugin.parseOrder("group"));
        assertEquals(ImmutableList.of(UID,GID,LOGIN,USER,GROUP),
                     AuthzDbPlugin.parseOrder("uid,gid,login,user,group"));
        assertEquals(ImmutableList.of(USER,GROUP,LOGIN,UID,GID),
                     AuthzDbPlugin.parseOrder("user,group,login,uid,gid"));
    }

    @Test
    public void testGroupName()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new GroupNamePrincipal("behrmann", true)),
              ImmutableSet.of(new UidPrincipal(1000),
                              new GidPrincipal(1000, true),
                              new UserNamePrincipal("behrmann"),
                              new GroupNamePrincipal("behrmann", true)));
    }

    @Test(expected=AuthenticationException.class)
    public void testGroupNameNotPrimary()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new GroupNamePrincipal("behrmann", false)),
              ImmutableSet.<Principal>of());
    }

    @Test
    public void testGroupNameWithSecondaryGroup()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new GroupNamePrincipal("atlas-user", false),
                              new GroupNamePrincipal("atlas-prod", true)),
              ImmutableSet.of(new UidPrincipal(1002),
                              new GidPrincipal(1001, false),
                              new GidPrincipal(1002, true),
                              new UserNamePrincipal("atlas-prod"),
                              new GroupNamePrincipal("atlas-user", false),
                              new GroupNamePrincipal("atlas-prod", true)));
    }

    @Test
    public void testGroupNameWithSecondaryGroupAndLoginUid()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new GroupNamePrincipal("atlas-user", false),
                              new GroupNamePrincipal("atlas-prod", true),
                              new LoginUidPrincipal(1001)),
              ImmutableSet.of(new UidPrincipal(1001),
                              new GidPrincipal(1001, false),
                              new GidPrincipal(1002, true),
                              new GroupNamePrincipal("atlas-user", false),
                              new GroupNamePrincipal("atlas-prod", true),
                              new LoginUidPrincipal(1001)));
    }

    @Test
    public void testGroupNameWithSecondaryGroupAndLoginGid()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new GroupNamePrincipal("atlas-user", false),
                              new GroupNamePrincipal("atlas-prod", true),
                              new LoginGidPrincipal(1001)),
              ImmutableSet.of(new UidPrincipal(1002),
                              new GidPrincipal(1001, true),
                              new GidPrincipal(1002, false),
                              new LoginGidPrincipal(1001),
                              new GroupNamePrincipal("atlas-user", false),
                              new GroupNamePrincipal("atlas-prod", true),
                              new UserNamePrincipal("atlas-prod")));
    }

    @Test
    public void testUserName()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new UserNamePrincipal("behrmann")),
              ImmutableSet.of(new UidPrincipal(1000),
                              new GidPrincipal(1000, true),
                              new UserNamePrincipal("behrmann")));
    }

    @Test
    public void testUserNameWithPrimaryGroup()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new UserNamePrincipal("behrmann"),
                              new GroupNamePrincipal("atlas-user", true)),
              ImmutableSet.of(new UidPrincipal(1000),
                              new GidPrincipal(1000, false),
                              new GidPrincipal(1001, true),
                              new UserNamePrincipal("behrmann"),
                              new GroupNamePrincipal("atlas-user", true)));
    }

    @Test
    public void testUserNameWithSecondaryGroup()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new UserNamePrincipal("behrmann"),
                              new GroupNamePrincipal("atlas-user", false)),
              ImmutableSet.of(new UidPrincipal(1000),
                              new GidPrincipal(1000, true),
                              new GidPrincipal(1001, false),
                              new UserNamePrincipal("behrmann"),
                              new GroupNamePrincipal("atlas-user", false)));
    }

    @Test
    public void testUserNameWithGroupsAndLoginUidAndLoginGid()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new UserNamePrincipal("behrmann"),
                              new GroupNamePrincipal("atlas-user", false),
                              new GroupNamePrincipal("atlas-prod", true),
                              new LoginUidPrincipal(1001),
                              new LoginGidPrincipal(1001)),
              ImmutableSet.of(new UidPrincipal(1001),
                              new GidPrincipal(1000, false),
                              new GidPrincipal(1001, true),
                              new GidPrincipal(1002, false),
                              new UserNamePrincipal("behrmann"),
                              new GroupNamePrincipal("atlas-user", false),
                              new GroupNamePrincipal("atlas-prod", true),
                              new LoginUidPrincipal(1001),
                              new LoginGidPrincipal(1001)));
    }

    @Test
    public void testUserNameWithLoginName()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new UserNamePrincipal("behrmann"),
                              new GroupNamePrincipal("atlas-prod", true),
                              new LoginNamePrincipal("behrmann")),
              ImmutableSet.of(new UidPrincipal(1000),
                              new GidPrincipal(1000, true),
                              new GidPrincipal(1002, false),
                              new UserNamePrincipal("behrmann"),
                              new GroupNamePrincipal("atlas-prod", true),
                              new LoginNamePrincipal("behrmann")));
    }

    @Test
    public void testUserNameMapToMultipleGids()
        throws AuthenticationException
    {
            check(ImmutableSet.of(new UserNamePrincipal("martinelli")),
                  ImmutableSet.of(new UserNamePrincipal("martinelli"),
                                  new UidPrincipal(1003),
                                  new GidPrincipal(1004, true),
                                  new GidPrincipal(1005, false)));
    }

    @Test(expected=AuthenticationException.class)
    public void testMultipleUserNames()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new UserNamePrincipal("atlas-user"),
                              new UserNamePrincipal("atlas-prod")),
              ImmutableSet.<Principal>of());
    }

    @Test(expected=AuthenticationException.class)
    public void testMultiplePrimaryGroupNames()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new GroupNamePrincipal("atlas-user", true),
                              new GroupNamePrincipal("atlas-prod", true)),
              ImmutableSet.<Principal>of());
    }

    @Test(expected=AuthenticationException.class)
    public void testMultipleLoginNames()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new LoginNamePrincipal("atlas-user"),
                              new LoginNamePrincipal("atlas-prod")),
              ImmutableSet.<Principal>of());
    }

    @Test(expected=AuthenticationException.class)
    public void testMultipleLoginUid()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new LoginUidPrincipal(1),
                              new LoginUidPrincipal(2)),
              ImmutableSet.<Principal>of());
    }

    @Test(expected=AuthenticationException.class)
    public void testMultipleLoginGid()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new LoginGidPrincipal(1),
                              new LoginGidPrincipal(2)),
              ImmutableSet.<Principal>of());
    }

    @Test(expected=AuthenticationException.class)
    public void testInvalidLoginName()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new LoginNamePrincipal("behrmann")),
              ImmutableSet.<Principal>of());
    }

    @Test(expected=AuthenticationException.class)
    public void testInvalidLoginUid()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new LoginUidPrincipal(1000)),
              ImmutableSet.<Principal>of());
    }

    @Test(expected=AuthenticationException.class)
    public void testInvalidLoginGid()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new LoginGidPrincipal(1000)),
              ImmutableSet.<Principal>of());
    }
}
