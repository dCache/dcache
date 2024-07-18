package org.dcache.gplazma.pyscript;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;

import java.security.Principal;
import java.util.*;

import org.dcache.auth.PasswordCredential;
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
        - public_credentials contains at least one username:password combination
        that is accepted (admin:dickerelch, Dust:Bowl)
        Result:
        - We add the username principal corresponding to the accepted username
        - we return True
    REFUSE:
        Condition:
        - The above condition is not met.
        Result:
        - we return False

     These JUnit tests are written accordingly.
     */

    private static PyscriptPlugin plugin;

    @Before
    public void setUp() {
        // Properties
        Properties properties = new Properties();
        properties.setProperty("gplazma.pyscript.workdir", "gplazma2-pyscript");
        plugin = new PyscriptPlugin(properties);
    }

    @Test
    public void passesWithPasswordCredential() throws Exception {
        // Credentials
        PasswordCredential pwcred = new PasswordCredential("Dust", "Bowl");
        Set<Object> publicCredentials = new HashSet<>();
        publicCredentials.add(pwcred);

        // Principals
        Set<Principal> principals = new HashSet<>();
        principals.add(new UserNamePrincipal("joad"));

        // Execution
        plugin.authenticate(
                publicCredentials,
                Collections.emptySet(),
                principals
        );

        // Assert that principals has been added to
        assertThat(principals, hasItems(
                new UserNamePrincipal("Dust"),
                new UserNamePrincipal("joad")
        ));
    }

    @Test(expected = AuthenticationException.class)
    public void failsOnBadPasswordCredential() throws Exception {
        // Credentials
        PasswordCredential pwcred = new PasswordCredential("Dustin", "Hoffman");
        Set<Object> publicCredentials = new HashSet<>();
        publicCredentials.add(pwcred);

        // Execution
        plugin.authenticate(
                publicCredentials,
                Collections.emptySet(),
                Collections.emptySet()
        );
    }

    @Test(expected = AuthenticationException.class)
    public void failsOnNoParameters() throws Exception{
        plugin.authenticate(
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptySet()
        );
    }
}
