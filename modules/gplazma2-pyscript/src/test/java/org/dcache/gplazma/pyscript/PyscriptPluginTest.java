package org.dcache.gplazma.pyscript;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;

import java.security.Principal;
import java.util.*;

import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.junit.Before;
import org.junit.Test;

public class PyscriptPluginTest {
    /*
    Test the gplazma2-pyscript module

    This module reads the Python (2.7) files provided and runs each auth attempt
    through the auth function defined in each file. Authentication may fail and
    throw an AuthenticationException, or it may pass and add new principals.

    In any case, the principals are only added to, never removed from.

    The internal logic of the "sample_plugin.py" file is the following

    AUTHENTICATE:
        Condition:
        - Either "Tom" is in the public credentials
        - Or "Rose" is in the set of private credentials
        Result:
        - the username principal "Joad" is added (as string "username:joad")
        - we return True
    REFUSE:
        Condition:
        - Either "Connie" is in either one of the credentials
        - No passing condition from above is met
        Result:
        - we return False

     These JUnit tests are written accordingly.
     */

    private static PyscriptPlugin plugin;

    @Before
    public void setUp() {
        plugin = new PyscriptPlugin(new Properties());
    }

    @Test
    public void passesWithPublicCredential() throws Exception {
        Set<Object> publicCredentials = new HashSet<>();
        publicCredentials.add("Tom");
        Set<Principal> principals = new HashSet<>();
        principals.add(new UserNamePrincipal("Casy"));
        plugin.authenticate(
                publicCredentials,
                Collections.emptySet(),
                principals
        );
        assertThat(principals, hasItems(
                new UserNamePrincipal("Joad"), // new principal added by plugin
                new UserNamePrincipal("Casy")  // old principal still included
        ));
    }

    @Test
    public void passesWithPrivateCredential() throws Exception {
        Set<Object> privateCredentials = new HashSet<>();
        privateCredentials.add("Rose");
        Set<Principal> principals = new HashSet<>();
        principals.add(new UserNamePrincipal("Casy"));
        plugin.authenticate(
                Collections.emptySet(),
                privateCredentials,
                principals
        );
        assertThat(principals, hasItems(
                new UserNamePrincipal("Joad"), // new principal added by plugin
                new UserNamePrincipal("Casy")  // old principal still included
        ));
    }

    @Test(expected = AuthenticationException.class)
    public void failsOnNoParameters() throws Exception{
        plugin.authenticate(
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptySet()
        );
    }

    @Test(expected = AuthenticationException.class)
    public void failsOnNoParametersDespitePrincipals() throws Exception {
        Set<Principal> principals = new HashSet<>();
        principals.add(new UserNamePrincipal("Ruthie"));
        principals.add(new UserNamePrincipal("Casy"));
        plugin.authenticate(
                Collections.emptySet(),
                Collections.emptySet(),
                principals
        );
    }
}
