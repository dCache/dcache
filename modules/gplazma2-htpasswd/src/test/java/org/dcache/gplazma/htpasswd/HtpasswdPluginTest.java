package org.dcache.gplazma.htpasswd;

import org.junit.Test;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.dcache.auth.PasswordCredential;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;

import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;

public class HtpasswdPluginTest
{
    @Test(expected = AuthenticationException.class)
    public void whenSuppliedWithNoCredentialsAuthenticationFails() throws Exception
    {
        HtpasswdPlugin plugin = new HtpasswdPlugin(new StringInputSupplier(""), 0);
        plugin.authenticate(
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.<Principal>emptySet());
    }

    @Test(expected = AuthenticationException.class)
    public void whenSuppliedWithCredentialsWithoutMappingAuthenticationFails() throws Exception
    {
        HtpasswdPlugin plugin = new HtpasswdPlugin(new StringInputSupplier(""), 0);
        plugin.authenticate(
                Collections.emptySet(),
                Collections.<Object>singleton(new PasswordCredential("user", "password")),
                Collections.<Principal>emptySet());
    }

    @Test(expected = AuthenticationException.class)
    public void whenSuppliedWithWrongPasswordAuthenticationFails() throws Exception
    {
        HtpasswdPlugin plugin = new HtpasswdPlugin(new StringInputSupplier("user:$apr1$X5ZCDJ6k$LmbjUJChwKdbrPb/3fFAU0"), 0);
        plugin.authenticate(
                Collections.emptySet(),
                Collections.<Object>singleton(new PasswordCredential("user", "wrong password")),
                Collections.<Principal>emptySet());
    }

    @Test(expected = AuthenticationException.class)
    public void whenSuppliedWithPasswordForAnotherAccountAuthenticationFails() throws Exception
    {
        HtpasswdPlugin plugin = new HtpasswdPlugin(new StringInputSupplier("user:$apr1$X5ZCDJ6k$LmbjUJChwKdbrPb/3fFAU0\nuser2:$apr1$cFgW0NZB$f50cUeavV3iz8dgVwRlWF."), 0);
        plugin.authenticate(
                Collections.emptySet(),
                Collections.<Object>singleton(new PasswordCredential("user", "wrong password")),
                Collections.<Principal>emptySet());
    }

    @Test
    public void whenSuppliedWithCorrectPasswordAuthenticationSucceedsForMD5() throws Exception
    {
        Set<Principal> principals = new HashSet<>();
        HtpasswdPlugin plugin = new HtpasswdPlugin(new StringInputSupplier("user:$apr1$X5ZCDJ6k$LmbjUJChwKdbrPb/3fFAU0"), 0);
        plugin.authenticate(
                Collections.emptySet(),
                Collections.<Object>singleton(new PasswordCredential("user", "password")),
                principals);
        assertThat(principals, hasItem(new UserNamePrincipal("user")));
    }

    @Test
    public void whenContainingTrailingWhitespaceAuthenticationSucceeds() throws Exception
    {
        Set<Principal> principals = new HashSet<>();
        HtpasswdPlugin plugin = new HtpasswdPlugin(new StringInputSupplier("user:$apr1$X5ZCDJ6k$LmbjUJChwKdbrPb/3fFAU0   "), 0);
        plugin.authenticate(
                Collections.emptySet(),
                Collections.<Object>singleton(new PasswordCredential("user", "password")),
                principals);
        assertThat(principals, hasItem(new UserNamePrincipal("user")));
    }

    @Test
    public void whenSuppliedWithCorrectPasswordAndHavingMultipleRecordsAuthenticationSucceeds() throws Exception
    {
        Set<Principal> principals = new HashSet<>();
        HtpasswdPlugin plugin = new HtpasswdPlugin(new StringInputSupplier("user:$apr1$X5ZCDJ6k$LmbjUJChwKdbrPb/3fFAU0\nuser2:$apr1$cFgW0NZB$f50cUeavV3iz8dgVwRlWF."), 0);
        plugin.authenticate(
                Collections.emptySet(),
                Collections.<Object>singleton(new PasswordCredential("user", "password")),
                principals);
        assertThat(principals, hasItem(new UserNamePrincipal("user")));
    }

    @Test
    public void whenDataChangesItIsReloaded() throws Exception
    {
        Set<Principal> principals1 = new HashSet<>();
        Set<Principal> principals2 = new HashSet<>();
        HtpasswdPlugin plugin = new HtpasswdPlugin(
                new MutableInputSupplier<Reader>()
                {
                    int i;
                    String[] lines = { "user:$apr1$X5ZCDJ6k$LmbjUJChwKdbrPb/3fFAU0", "user:$apr1$cFgW0NZB$f50cUeavV3iz8dgVwRlWF." };

                    @Override
                    public Reader getInput() throws IOException
                    {
                        return new StringReader(lines[i++ % 2]);
                    }

                    @Override
                    public long lastModified()
                    {
                        return Long.MAX_VALUE;
                    }
                }, 0);
        plugin.authenticate(
                Collections.emptySet(),
                Collections.<Object>singleton(new PasswordCredential("user", "password")),
                principals1);
        plugin.authenticate(
                Collections.emptySet(),
                Collections.<Object>singleton(new PasswordCredential("user", "wrong password")),
                principals2);
        assertThat(principals1, hasItem(new UserNamePrincipal("user")));
        assertThat(principals2, hasItem(new UserNamePrincipal("user")));
    }

    private static class StringInputSupplier implements MutableInputSupplier<Reader>
    {
        private String s;

        StringInputSupplier(String s)
        {
            this.s = s;
        }

        @Override
        public long lastModified()
        {
            return 0;
        }

        @Override
        public Reader getInput() throws IOException
        {
            return new StringReader(s);
        }
    }
}
