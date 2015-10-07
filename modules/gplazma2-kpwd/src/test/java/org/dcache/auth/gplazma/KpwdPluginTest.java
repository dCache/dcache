package org.dcache.auth.gplazma;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import org.globus.gsi.gssapi.jaas.GlobusPrincipal;
import org.junit.Before;
import org.junit.Test;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.Principal;
import java.util.Set;

import org.dcache.auth.GidPrincipal;
import org.dcache.auth.KAuthFile;
import org.dcache.auth.LoginNamePrincipal;
import org.dcache.auth.PasswordCredential;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.gplazma.AuthenticationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KpwdPluginTest
{
    private final static URL TEST_FIXTURE =
        Resources.getResource("org/dcache/auth/gplazma/kpwd.fixture");

    private final static String DN_BEHRMANN =
        "/O=Grid/O=NorduGrid/OU=ndgf.org/CN=Gerd Behrmann";

    private KAuthFile testFixture;

    private final static Set<Principal> NO_PRINCIPALS = ImmutableSet.of();

    @Before
    public void setup()
        throws IOException
    {
        try (InputStream is = TEST_FIXTURE.openStream()) {
            testFixture = new KAuthFile(is);
        }

    }

    public void check(Set<?> credentials,
                      Set<? extends Principal> input,
                      Set<? extends Principal> output,
                      Set<?> expectedAttributes)
        throws AuthenticationException
    {
        KpwdPlugin plugin = new KpwdPlugin(testFixture);
        Set<Object> privateCredentials = Sets.newHashSet(credentials);
        Set<Principal> principals = Sets.newHashSet(input);
        Set<Object> attributes = Sets.newHashSet();

        plugin.authenticate(Sets.newHashSet(), privateCredentials, principals);

        plugin.map(principals);
        assertTrue("expected: " + output + " was: " + principals,
                   principals.containsAll(output));

        plugin.account(principals);

        plugin.session(principals, attributes);
        assertEquals(expectedAttributes, attributes);
    }

    public void check(Set<? extends Principal> input,
                      Set<? extends Principal> output,
                      Set<?> expectedAttributes)
        throws AuthenticationException
    {
        KpwdPlugin plugin = new KpwdPlugin(testFixture);
        Set<Principal> principals = Sets.newHashSet(input);
        Set<Object> attributes = Sets.newHashSet();

        plugin.map(principals);
        assertTrue("expected: " + output + " was: " + principals,
                   principals.containsAll(output));

        plugin.account(principals);

        plugin.session(principals, attributes);
        assertEquals(expectedAttributes, attributes);
    }

    @Test(expected=AuthenticationException.class)
    public void testNoSecureId()
        throws AuthenticationException
    {
        check(NO_PRINCIPALS,
              NO_PRINCIPALS,
              ImmutableSet.of());
    }

    @Test(expected=AuthenticationException.class)
    public void testTwoGlobusPrincipals()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new GlobusPrincipal("/bla"),
                              new GlobusPrincipal("/foo")),
              NO_PRINCIPALS,
              ImmutableSet.of());
    }

    @Test(expected=AuthenticationException.class)
    public void testTwoKerberosPrincipals()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new KerberosPrincipal("a@b"),
                              new KerberosPrincipal("a@c")),
              NO_PRINCIPALS,
              ImmutableSet.of());
    }

    @Test(expected=AuthenticationException.class)
    public void testTwoGlobusAndKerberosPrincipals()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new GlobusPrincipal("/bla"),
                              new KerberosPrincipal("a@b")),
              NO_PRINCIPALS,
              ImmutableSet.of());
    }

    @Test(expected=AuthenticationException.class)
    public void testTwoLoginNames()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new GlobusPrincipal(DN_BEHRMANN),
                              new LoginNamePrincipal("behrmann"),
                              new LoginNamePrincipal("behrmann2")),
              NO_PRINCIPALS,
              ImmutableSet.of());
    }

    @Test
    public void testGlobusPrincipalDefault()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new GlobusPrincipal(DN_BEHRMANN)),
              ImmutableSet.of(new GlobusPrincipal(DN_BEHRMANN),
                              new UidPrincipal(1000),
                              new GidPrincipal(1000, true),
                              new UserNamePrincipal("behrmann")),
              ImmutableSet.of(new HomeDirectory("/foo"),
                              new RootDirectory("/bar")));
    }

    @Test
    public void testGlobusPrincipalLoginName1()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new GlobusPrincipal(DN_BEHRMANN),
                              new LoginNamePrincipal("behrmann")),
              ImmutableSet.of(new GlobusPrincipal(DN_BEHRMANN),
                              new UidPrincipal(1000),
                              new GidPrincipal(1000, true),
                              new UserNamePrincipal("behrmann")),
              ImmutableSet.of(new HomeDirectory("/foo"),
                              new RootDirectory("/bar")));
    }

    @Test
    public void testGlobusPrincipalLoginName2()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new GlobusPrincipal(DN_BEHRMANN),
                              new LoginNamePrincipal("behrmann2")),
              ImmutableSet.of(new GlobusPrincipal(DN_BEHRMANN),
                              new UidPrincipal(1001),
                              new GidPrincipal(1001, true),
                              new UserNamePrincipal("behrmann2")),
              ImmutableSet.of(new HomeDirectory("/"),
                              new RootDirectory("/"),
                              Restrictions.readOnly()));
    }

    @Test(expected=AuthenticationException.class)
    public void testGlobusPrincipalUnknown()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new GlobusPrincipal("/bla/bla")),
              NO_PRINCIPALS,
              ImmutableSet.of());
    }

    @Test(expected=AuthenticationException.class)
    public void testGlobusPrincipalUnknownLoginName()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new GlobusPrincipal(DN_BEHRMANN),
                              new LoginNamePrincipal("foobar")),
              NO_PRINCIPALS,
              ImmutableSet.of());
    }

    @Test(expected=AuthenticationException.class)
    public void testGlobusPrincipalUnauthorizedLoginName()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new GlobusPrincipal(DN_BEHRMANN),
                              new LoginNamePrincipal("behrmann3")),
              NO_PRINCIPALS,
              ImmutableSet.of());
    }

    @Test
    public void testKerberosPrincipalDefault()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new KerberosPrincipal("behrmann@ndgf.org")),
              ImmutableSet.of(new KerberosPrincipal("behrmann@ndgf.org"),
                              new UidPrincipal(1001),
                              new GidPrincipal(1001, true),
                              new UserNamePrincipal("behrmann2")),
              ImmutableSet.of(new HomeDirectory("/"),
                              new RootDirectory("/"),
                              Restrictions.readOnly()));
    }

    @Test
    public void testKerberosPrincipalLoginName1()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new KerberosPrincipal("behrmann@ndgf.org"),
                              new LoginNamePrincipal("behrmann")),
              ImmutableSet.of(new KerberosPrincipal("behrmann@ndgf.org"),
                              new UidPrincipal(1000),
                              new GidPrincipal(1000, true),
                              new UserNamePrincipal("behrmann")),
              ImmutableSet.of(new HomeDirectory("/foo"),
                              new RootDirectory("/bar")));
    }

    @Test
    public void testKerberosPrincipalLoginName2()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new KerberosPrincipal("behrmann@ndgf.org"),
                              new LoginNamePrincipal("behrmann2")),
              ImmutableSet.of(new KerberosPrincipal("behrmann@ndgf.org"),
                              new UidPrincipal(1001),
                              new GidPrincipal(1001, true),
                              new UserNamePrincipal("behrmann2")),
              ImmutableSet.of(new HomeDirectory("/"),
                              new RootDirectory("/"),
                              Restrictions.readOnly()));
    }

    @Test(expected=AuthenticationException.class)
    public void testKerberosPrincipalUnknown()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new KerberosPrincipal("foo@bar")),
              NO_PRINCIPALS,
              ImmutableSet.of());
    }

    @Test(expected=AuthenticationException.class)
    public void testKerberosPrincipalUnknownLoginName()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new KerberosPrincipal("behrmann@ndgf.org"),
                              new LoginNamePrincipal("foobar")),
              NO_PRINCIPALS,
              ImmutableSet.of());
    }

    @Test(expected=AuthenticationException.class)
    public void testKerberosPrincipalUnauthorizedLoginName()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new KerberosPrincipal("behrmann@ndgf.org"),
                              new LoginNamePrincipal("behrmann3")),
              NO_PRINCIPALS,
              ImmutableSet.of());
    }

    @Test(expected=AuthenticationException.class)
    public void testPasswordMissing()
        throws AuthenticationException
    {
        check(ImmutableSet.of(),
              NO_PRINCIPALS,
              NO_PRINCIPALS,
              ImmutableSet.of());
    }

    @Test(expected=AuthenticationException.class)
    public void testPasswordWrongUser()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new PasswordCredential("behrmann2", "test")),
              NO_PRINCIPALS,
              NO_PRINCIPALS,
              ImmutableSet.of());
    }

    @Test(expected=AuthenticationException.class)
    public void testPasswordWrongPassword()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new PasswordCredential("behrmann", "test2")),
              NO_PRINCIPALS,
              NO_PRINCIPALS,
              ImmutableSet.of());
    }

    @Test
    public void testPassword()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new PasswordCredential("behrmann", "test")),
              NO_PRINCIPALS,
              ImmutableSet.of(new UserNamePrincipal("behrmann"),
                              new UidPrincipal(1000),
                              new GidPrincipal(1000, true)),
              ImmutableSet.of(new HomeDirectory("/"),
                              new RootDirectory("/")));
    }

    @Test
    public void testPasswordAnonymous1()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new PasswordCredential("anonymous", "")),
              NO_PRINCIPALS,
              ImmutableSet.of(new UserNamePrincipal("anonymous"),
                              new UidPrincipal(2000),
                              new GidPrincipal(2000, true)),
              ImmutableSet.of(new HomeDirectory("/"),
                              new RootDirectory("/"),
                              Restrictions.readOnly()));
    }

    @Test
    public void testPasswordAnonymous2()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new PasswordCredential("anonymous", "test")),
              NO_PRINCIPALS,
              ImmutableSet.of(new UserNamePrincipal("anonymous"),
                              new UidPrincipal(2000),
                              new GidPrincipal(2000, true)),
              ImmutableSet.of(new HomeDirectory("/"),
                              new RootDirectory("/"),
                              Restrictions.readOnly()));
    }

    @Test
    public void testPasswordAnonymous3()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new PasswordCredential("anonymous", "test2")),
              NO_PRINCIPALS,
              ImmutableSet.of(new UserNamePrincipal("anonymous"),
                              new UidPrincipal(2000),
                              new GidPrincipal(2000, true)),
              ImmutableSet.of(new HomeDirectory("/"),
                              new RootDirectory("/"),
                              Restrictions.readOnly()));
    }

    @Test(expected=AuthenticationException.class)
    public void testPasswordBlacklist()
        throws AuthenticationException
    {
        check(ImmutableSet.of(new PasswordCredential("banned", "test")),
              NO_PRINCIPALS,
              NO_PRINCIPALS,
              ImmutableSet.of());
    }

}
