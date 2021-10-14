package org.dcache.gplazma.htpasswd;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;

import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.dcache.auth.PasswordCredential;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.junit.Test;

public class HtpasswdPluginTest {

    @Test(expected = AuthenticationException.class)
    public void whenSuppliedWithNoCredentialsAuthenticationFails() throws Exception {
        HtpasswdPlugin plugin = new HtpasswdPlugin(Stream::empty);
        plugin.authenticate(
              Collections.emptySet(),
              Collections.emptySet(),
              Collections.<Principal>emptySet());
    }

    @Test(expected = AuthenticationException.class)
    public void whenSuppliedWithCredentialsWithoutMappingAuthenticationFails() throws Exception {
        HtpasswdPlugin plugin = new HtpasswdPlugin(Stream::empty);
        plugin.authenticate(
              Collections.emptySet(),
              Collections.<Object>singleton(new PasswordCredential("user", "password")),
              Collections.<Principal>emptySet());
    }

    @Test(expected = AuthenticationException.class)
    public void whenSuppliedWithWrongPasswordAuthenticationFails() throws Exception {
        HtpasswdPlugin plugin = new HtpasswdPlugin(
              () -> Stream.of("user:$apr1$X5ZCDJ6k$LmbjUJChwKdbrPb/3fFAU0"));
        plugin.authenticate(
              Collections.emptySet(),
              Collections.<Object>singleton(new PasswordCredential("user", "wrong password")),
              Collections.<Principal>emptySet());
    }

    @Test(expected = AuthenticationException.class)
    public void whenSuppliedWithPasswordForAnotherAccountAuthenticationFails() throws Exception {
        HtpasswdPlugin plugin = new HtpasswdPlugin(
              () -> Stream.of("user:$apr1$X5ZCDJ6k$LmbjUJChwKdbrPb/3fFAU0",
                    "user2:$apr1$cFgW0NZB$f50cUeavV3iz8dgVwRlWF."));
        plugin.authenticate(
              Collections.emptySet(),
              Collections.<Object>singleton(new PasswordCredential("user", "wrong password")),
              Collections.<Principal>emptySet());
    }

    @Test
    public void whenSuppliedWithCorrectPasswordAuthenticationSucceedsForMD5() throws Exception {
        Set<Principal> principals = new HashSet<>();
        HtpasswdPlugin plugin = new HtpasswdPlugin(
              () -> Stream.of("user:$apr1$X5ZCDJ6k$LmbjUJChwKdbrPb/3fFAU0"));
        plugin.authenticate(
              Collections.emptySet(),
              Collections.<Object>singleton(new PasswordCredential("user", "password")),
              principals);
        assertThat(principals, hasItem(new UserNamePrincipal("user")));
    }

    @Test
    public void whenContainingTrailingWhitespaceAuthenticationSucceeds() throws Exception {
        Set<Principal> principals = new HashSet<>();
        HtpasswdPlugin plugin = new HtpasswdPlugin(
              () -> Stream.of("user:$apr1$X5ZCDJ6k$LmbjUJChwKdbrPb/3fFAU0   "));
        plugin.authenticate(
              Collections.emptySet(),
              Collections.<Object>singleton(new PasswordCredential("user", "password")),
              principals);
        assertThat(principals, hasItem(new UserNamePrincipal("user")));
    }

    @Test
    public void whenSuppliedWithCorrectPasswordAndHavingMultipleRecordsAuthenticationSucceeds()
          throws Exception {
        Set<Principal> principals = new HashSet<>();
        HtpasswdPlugin plugin = new HtpasswdPlugin(
              () -> Stream.of("user:$apr1$X5ZCDJ6k$LmbjUJChwKdbrPb/3fFAU0",
                    "user2:$apr1$cFgW0NZB$f50cUeavV3iz8dgVwRlWF."));
        plugin.authenticate(
              Collections.emptySet(),
              Collections.<Object>singleton(new PasswordCredential("user", "password")),
              principals);
        assertThat(principals, hasItem(new UserNamePrincipal("user")));
    }

    @Test
    public void whenDataChangesItIsReloaded() throws Exception {
        Set<Principal> principals1 = new HashSet<>();
        Set<Principal> principals2 = new HashSet<>();
        List<Stream<String>> configurations =
              asList(Stream.of("user:$apr1$X5ZCDJ6k$LmbjUJChwKdbrPb/3fFAU0"),
                    Stream.of("user:$apr1$cFgW0NZB$f50cUeavV3iz8dgVwRlWF."));
        HtpasswdPlugin plugin = new HtpasswdPlugin(configurations.iterator()::next);
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
}
