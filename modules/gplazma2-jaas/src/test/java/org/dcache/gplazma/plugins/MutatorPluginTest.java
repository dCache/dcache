package org.dcache.gplazma.plugins;

import com.google.common.collect.Sets;
import org.junit.Test;

import java.security.Principal;
import java.util.Properties;
import java.util.Set;

import org.dcache.auth.LoginNamePrincipal;
import org.dcache.auth.UserNamePrincipal;

import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.collect.Iterables.find;

public class MutatorPluginTest {

    @Test(expected = IllegalArgumentException.class)
    public void testNoOption() throws Exception {
        new MutatorPlugin(new Properties());
    }

    @Test(expected = ClassNotFoundException.class)
    public void testNoClass() throws Exception {
        Properties prop = new Properties();
        prop.setProperty(MutatorPlugin.IN_OPTION, "BadINClassName");
        prop.setProperty(MutatorPlugin.OUT_OPTION, "BadOUTClassName");

        new MutatorPlugin(prop);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNotPrincipalClass() throws Exception {
        Properties prop = new Properties();
        prop.setProperty(MutatorPlugin.IN_OPTION, "java.lang.String");
        prop.setProperty(MutatorPlugin.OUT_OPTION, "java.lang.String");

        new MutatorPlugin(prop);
    }

    @Test
    public void testValidPrincipalClass() throws Exception {
        Properties prop = new Properties();

        prop.setProperty(MutatorPlugin.IN_OPTION, "org.dcache.auth.LoginNamePrincipal");
        prop.setProperty(MutatorPlugin.OUT_OPTION, "username");

        new MutatorPlugin(prop);
    }

    @Test
    public void testMutation() throws Exception {
        Properties prop = new Properties();

        prop.setProperty(MutatorPlugin.IN_OPTION, "org.dcache.auth.LoginNamePrincipal");
        prop.setProperty(MutatorPlugin.OUT_OPTION, "username");

        MutatorPlugin mutator = new MutatorPlugin(prop);
        Set<Principal> principals = Sets.newHashSet((Principal) new LoginNamePrincipal("myname"));

        mutator.map(principals);

        // will throw exception if no  match
        find(principals, instanceOf(UserNamePrincipal.class));
    }
}
