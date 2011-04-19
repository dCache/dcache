package org.dcache.gplazma.plugins;

import java.io.IOException;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.nio.charset.Charset;
import java.net.URL;

import junit.framework.AssertionFailedError;

import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.LoginNamePrincipal;
import org.dcache.auth.LoginUidPrincipal;
import org.dcache.auth.LoginGidPrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.plugins.AuthzMapLineParser.UserAuthzInformation;
import org.globus.gsi.jaas.GlobusPrincipal;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import com.google.common.collect.Sets;
import com.google.common.io.Resources;

import static org.dcache.gplazma.plugins.CachedMapsProvider.*;

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
            new SourceBackedPredicateMap(new MemoryLineSource(Resources.readLines(TEST_FIXTURE, Charset.defaultCharset())), new AuthzMapLineParser());
    }

    @After
    public void cleanup()
    {
    }

    public void check(Set<? extends Principal> principals,
                      Set<? extends Principal> expectedAuthorizedPrincipals)
        throws AuthenticationException
    {
        AuthzDbPlugin plugin = new AuthzDbPlugin(testFixture);
        Set<Principal> expectedPrincipals = Sets.newHashSet(principals);
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        plugin.map(null, (Set<Principal>) principals, authorizedPrincipals);
        assertEquals(expectedPrincipals, principals);
        assertEquals(expectedAuthorizedPrincipals, authorizedPrincipals);
    }

    @Test
    public void testGroupName()
        throws AuthenticationException
    {
        check(Sets.newHashSet(new GroupNamePrincipal("behrmann", true)),
              Sets.newHashSet(new UidPrincipal(1000),
                              new GidPrincipal(1000, true),
                              new UserNamePrincipal("behrmann")));
    }

    @Test(expected=AuthenticationException.class)
    public void testGroupNameNotPrimary()
        throws AuthenticationException
    {
        check(Sets.newHashSet(new GroupNamePrincipal("behrmann", false)),
              Sets.<Principal>newHashSet());
    }

    @Test
    public void testGroupNameWithSecondaryGroup()
        throws AuthenticationException
    {
        check(Sets.newHashSet(new GroupNamePrincipal("atlas-user", false),
                              new GroupNamePrincipal("atlas-prod", true)),
              Sets.newHashSet(new UidPrincipal(1002),
                              new GidPrincipal(1001, false),
                              new GidPrincipal(1002, true),
                              new UserNamePrincipal("atlas-prod")));
    }

    @Test
    public void testGroupNameWithSecondaryGroupAndLoginUid()
        throws AuthenticationException
    {
        check(Sets.newHashSet(new GroupNamePrincipal("atlas-user", false),
                              new GroupNamePrincipal("atlas-prod", true),
                              new LoginUidPrincipal(1001)),
              Sets.newHashSet(new UidPrincipal(1001),
                              new GidPrincipal(1001, false),
                              new GidPrincipal(1002, true)));
    }

    @Test
    public void testGroupNameWithSecondaryGroupAndLoginGid()
        throws AuthenticationException
    {
        check(Sets.newHashSet(new GroupNamePrincipal("atlas-user", false),
                              new GroupNamePrincipal("atlas-prod", true),
                              new LoginGidPrincipal(1001, true)),
              Sets.newHashSet(new UidPrincipal(1002),
                              new GidPrincipal(1001, true),
                              new GidPrincipal(1002, false),
                              new UserNamePrincipal("atlas-prod")));
    }

    @Test(expected=AuthenticationException.class)
    public void testMultipleLoginNames()
        throws AuthenticationException
    {
        check(Sets.newHashSet(new LoginNamePrincipal("atlas-user"),
                              new LoginNamePrincipal("atlas-prod")),
              Sets.<Principal>newHashSet());
    }

    @Test(expected=AuthenticationException.class)
    public void testMultipleLoginUid()
        throws AuthenticationException
    {
        check(Sets.newHashSet(new LoginUidPrincipal(1),
                              new LoginUidPrincipal(2)),
              Sets.<Principal>newHashSet());
    }

    @Test(expected=AuthenticationException.class)
    public void testMultipleLoginGid()
        throws AuthenticationException
    {
        check(Sets.newHashSet(new LoginGidPrincipal(1, true),
                              new LoginGidPrincipal(2, true)),
              Sets.<Principal>newHashSet());
    }

    @Test(expected=AuthenticationException.class)
    public void testInvalidLoginName()
        throws AuthenticationException
    {
        check(Sets.newHashSet(new LoginNamePrincipal("behrmann")),
              Sets.<Principal>newHashSet());
    }

    @Test(expected=AuthenticationException.class)
    public void testInvalidLoginUid()
        throws AuthenticationException
    {
        check(Sets.newHashSet(new LoginUidPrincipal(1000)),
              Sets.<Principal>newHashSet());
    }

    @Test(expected=AuthenticationException.class)
    public void testInvalidLoginGid()
        throws AuthenticationException
    {
        check(Sets.newHashSet(new LoginGidPrincipal(1000, true)),
              Sets.<Principal>newHashSet());
    }

    private <T> void assertContains(T expected, Collection<T> collection)
        throws AssertionFailedError
    {
        if (!collection.contains(expected)) {
            throw new AssertionFailedError(String.format("Expected element %s does not exist in collection %s", expected, collection));
        }
    }
}
