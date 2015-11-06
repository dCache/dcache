package org.dcache.gplazma.strategies;


import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.security.Principal;
import java.util.Arrays;
import java.util.Set;

import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.configuration.ConfigurationItemControl;
import org.dcache.gplazma.configuration.parser.FactoryConfigurationException;
import org.dcache.gplazma.monitor.IgnoringLoginMonitor;
import org.dcache.gplazma.monitor.LoginMonitor;
import org.dcache.gplazma.plugins.GPlazmaAccountPlugin;

import static org.dcache.gplazma.util.Preconditions.checkAuthentication;
import static org.junit.Assert.fail;

/**
 *  Test the default accounting strategy
 */
@SuppressWarnings("unchecked")
public class AccountStrategyTests
{
    private static final String DEFAULT_STRATEGY_FACTORY =
            "org.dcache.gplazma.strategies.DefaultStrategyFactory";

    private static final LoginMonitor IGNORING_LOGIN_MONITOR =
            new IgnoringLoginMonitor();

    private static final Principal PAUL_KERBEROS_PRINCIPAL =
            new KerberosPrincipal("paul@DESY.DE");

    private static final Principal TIGRAN_KERBEROS_PRINCIPAL =
            new KerberosPrincipal("tigran@DESY.DE");


    private AccountStrategy _strategy;
    private Set<Principal> _principals;


    @Before
    public void setup() throws FactoryConfigurationException
    {
        StrategyFactory factory = StrategyFactory.getInstance(DEFAULT_STRATEGY_FACTORY);
        _strategy = factory.newAccountStrategy();
        _principals = Sets.newHashSet();
    }


    @Test
    public void shouldSucceedForNoConfiguration()
            throws AuthenticationException
    {
        // given an empty configuration
        givenStrategyWithPlugins(noPlugins());

        // given an empty set of principals
        givenAuthorizedPrincipals(noPrincipals());

        runAccountPhase();
    }


    @Test
    public void shouldSucceedForSuccessfulRequiredPlugin()
            throws AuthenticationException
    {
        // given configuration of a REQUIRED plugin (which succeeds)
        givenStrategyWithPlugins(required(Succeeds.class));

        // given use an empty set of principals
        givenAuthorizedPrincipals(noPrincipals());

        runAccountPhase();
    }


    @Test
    public void shouldSucceedForSuccessfulRequisitePlugin()
            throws AuthenticationException
    {
        // given configuration of a REQUISITE plugin (which succeeds)
        givenStrategyWithPlugins(requisite(Succeeds.class));

        // given an empty set of principals
        givenAuthorizedPrincipals(noPrincipals());

        runAccountPhase();
    }


    @Test
    public void shouldSucceedForSuccessfulSufficientPlugin()
            throws AuthenticationException
    {
        // given configuration of a SUFFICIENT plugin (which succeeds)
        // followed by plugin that will fail the test, if run
        givenStrategyWithPlugins(sufficient(Succeeds.class),
                required(FailsTest.class));

        // given an empty set of principals
        givenAuthorizedPrincipals(noPrincipals());

        runAccountPhase();
    }


    @Test
    public void shouldSucceedForSuccessfulOptionalPlugin()
            throws AuthenticationException
    {
        // given configuration of an OPTIONAL plugin (which succeeds)
        givenStrategyWithPlugins(optional(Succeeds.class));

        // given an empty set of principals
        givenAuthorizedPrincipals(noPrincipals());

        runAccountPhase();
    }


    @Test(expected=AuthenticationException.class)
    public void shouldFailForFailingRequiredPlugin()
            throws AuthenticationException
    {
        // given configuration of a REQUIRED plugin (which fails)
        givenStrategyWithPlugins(required(Fails.class));

        // given an empty set of principals
        givenAuthorizedPrincipals(noPrincipals());

        runAccountPhase();
    }


    @Test(expected=AuthenticationException.class)
    public void shouldFailForFailingRequisitePlugin()
            throws AuthenticationException
    {
        // given configuration of a REQUISITE plugin (which fails) followed by
        // a plugin that will fail the test, if run
        givenStrategyWithPlugins(requisite(Fails.class),
                required(FailsTest.class));

        // given an empty set of principals
        givenAuthorizedPrincipals(noPrincipals());

        runAccountPhase();
    }


    @Test
    public void shouldSucceedForFailingOptionalPlugin()
            throws AuthenticationException
    {
        // given configuration of an OPTIONAL plugin (which fails)
        givenStrategyWithPlugins(optional(Fails.class));

        // given an empty set of principals
        givenAuthorizedPrincipals(noPrincipals());

        runAccountPhase();
    }


    @Test
    public void shouldSucceedForFailingSufficientPlugin()
            throws AuthenticationException
    {
        // given configuration of a SUFFICIENT plugin (which fails)
        givenStrategyWithPlugins(sufficient(Fails.class));

        // given an empty set of principals
        givenAuthorizedPrincipals(noPrincipals());

        runAccountPhase();
    }


    @Test
    public void shouldSucceedForTwoSuccessfulReqiredPlugins()
            throws AuthenticationException
    {
        // given configuration of a REQUIRED plugin (which succeeds) followed
        // by another REQUIRED plugin (which also succeeds)
        givenStrategyWithPlugins(
                required(Succeeds.class),
                required(Succeeds.class));

        // given an empty set of principals
        givenAuthorizedPrincipals(noPrincipals());

        runAccountPhase();
    }


    @Test
    public void shouldSucceedForTwoSuccessfulRequisitePlugins()
            throws AuthenticationException
    {
        // given configuration of a REQUISITE plugin (which succeeds) followed
        // by another REQUISITE plugin (which succeeds)
        givenStrategyWithPlugins(
                requisite(Succeeds.class),
                requisite(Succeeds.class));

        // given an empty set of principals
        givenAuthorizedPrincipals(noPrincipals());

        runAccountPhase();
    }


    @Test
    public void shouldSucceedForTwoSucceedingOptionalPlugins()
            throws AuthenticationException
    {
        // given configuration of an OPTIONAL plugin (which succeeds) followed
        // by another OPTIONAL plugin (which succeeds)
        givenStrategyWithPlugins(
                optional(Succeeds.class),
                optional(Succeeds.class));

        // given an empty set of principals
        givenAuthorizedPrincipals(noPrincipals());

        runAccountPhase();
    }


    @Test(expected=AuthenticationException.class)
    public void shouldFailForSuccessfulRequiredAndFailingRequiredPlugins()
            throws AuthenticationException
    {
        // given configuration of a REQUIRED plugin (which succeeds) followed
        // by another REQUIRED plugin (which fails).
        givenStrategyWithPlugins(
                required(Succeeds.class),
                required(Fails.class));

        // given an empty set of principals
        givenAuthorizedPrincipals(noPrincipals());

        runAccountPhase();
    }


    @Test
    public void shouldSucceedForSuccessfulRequiredAndFailingOptionalPlugins()
            throws AuthenticationException
    {
        // given configuration of a REQUIRED plugin (which succeeds) followed
        // by an OPTIONAL plugin (which fails).
        givenStrategyWithPlugins(
                required(Succeeds.class),
                optional(Fails.class));

        // given an empty set of principals
        givenAuthorizedPrincipals(noPrincipals());

        runAccountPhase();
    }


    @Test(expected=AuthenticationException.class)
    public void shouldFailForFailingRequiredAndFailingRequisitePlugins()
            throws AuthenticationException
    {
        // given configuration of a REQUIRED plugin (which fails) followed by
        // a REQUISITE plugin (which fails) followed by a plugin that will
        // fail the unit-test, if run.
        givenStrategyWithPlugins(
                required(Fails.class),
                requisite(Fails.class),
                required(FailsTest.class));

        // given an empty set of principals
        givenAuthorizedPrincipals(noPrincipals());

        runAccountPhase();
    }


    @Test(expected=AuthenticationException.class)
    public void shouldFailForRequisitePluginWithWrongPrincipal()
            throws AuthenticationException
    {
        // given configuration of a REQUISITE plugin (which fails) followed by
        // a plugin that will fail the unit-test, if run.
        givenStrategyWithPlugins(
                requisite(SucceedIfPaul.class),
                required(FailsTest.class));

        // given Tigran's Kerberos principal
        givenAuthorizedPrincipals(TIGRAN_KERBEROS_PRINCIPAL);

        runAccountPhase();
    }


    @Test
    public void shouldSucceedForSufficientPluginWithCorrectPrincipal()
            throws AuthenticationException
    {
        // given configuration of a SUFFICIENT plugin (which succeeds) followed
        // by a plugin that will fail the unit-test, if run.
        givenStrategyWithPlugins(
                sufficient(SucceedIfPaul.class),
                required(FailsTest.class));

        // given Paul's Kerberos principal
        givenAuthorizedPrincipals(PAUL_KERBEROS_PRINCIPAL);

        runAccountPhase();
    }


    @Test(expected=AuthenticationException.class)
    public void shouldFailForRequisiteBuggyPlugin()
            throws AuthenticationException
    {
        // given configuration of a REQUISITE plugin (which is buggy) followed
        // by a plugin that will fail the unit-test, if run.
        givenStrategyWithPlugins(
                requisite(Buggy.class),
                required(FailsTest.class));

        // given an empty set of principals
        givenAuthorizedPrincipals(noPrincipals());

        runAccountPhase();
    }


    @Test
    public void shouldSucceedForOptionalBuggyPlugin()
            throws AuthenticationException
    {
        // given configuration of an OPTIONAL plugin (which is buggy)
        givenStrategyWithPlugins(optional(Buggy.class));

        // given an empty set of principals
        givenAuthorizedPrincipals(noPrincipals());

        runAccountPhase();
    }


    private void runAccountPhase() throws AuthenticationException
    {
        _strategy.account(IGNORING_LOGIN_MONITOR, _principals);
    }


    private void givenAuthorizedPrincipals(Principal... principals)
    {
        _principals.clear();
        _principals.addAll(Arrays.asList(principals));
    }

    private static Principal[] noPrincipals()
    {
        return new Principal[0];
    }

    private void givenStrategyWithPlugins(GPlazmaPluginService<GPlazmaAccountPlugin>... plugins)
    {
        _strategy.setPlugins(Arrays.asList(plugins));
    }

    private static GPlazmaPluginService<GPlazmaAccountPlugin>[] noPlugins()
    {
        return new GPlazmaPluginService[0];
    }

    private static GPlazmaPluginService<GPlazmaAccountPlugin> sufficient(
            Class<? extends GPlazmaAccountPlugin> type)
    {
        return configuredPlugin(type, ConfigurationItemControl.SUFFICIENT);
    }

    private static GPlazmaPluginService<GPlazmaAccountPlugin> required(
            Class<? extends GPlazmaAccountPlugin> type)
    {
        return configuredPlugin(type, ConfigurationItemControl.REQUIRED);
    }

    private static GPlazmaPluginService<GPlazmaAccountPlugin> requisite(
            Class<? extends GPlazmaAccountPlugin> type)
    {
        return configuredPlugin(type, ConfigurationItemControl.REQUISITE);
    }

    private static GPlazmaPluginService<GPlazmaAccountPlugin> optional(
            Class<? extends GPlazmaAccountPlugin> type)
    {
        return configuredPlugin(type, ConfigurationItemControl.OPTIONAL);
    }

    private static GPlazmaPluginService<GPlazmaAccountPlugin> configuredPlugin(
            Class<? extends GPlazmaAccountPlugin> type,
            ConfigurationItemControl control)
    {
        GPlazmaAccountPlugin plugin;

        try {
            plugin = type.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        return new GPlazmaPluginService<>(plugin,
                type.getSimpleName(), control);
    }




    /**
     * AccountPlugin that always succeeds
     */
    public static final class Succeeds implements GPlazmaAccountPlugin
    {
        @Override
        public void account(Set<Principal> authorizedPrincipals)
                throws AuthenticationException
        {
            // do nothing here
        }
    }


    /**
     * AccountPlugin that always fails
     */
    public static final class Fails
            implements GPlazmaAccountPlugin
    {
        @Override
        public void account(Set<Principal> authorizedPrincipals)
                throws AuthenticationException
        {
            throw new AuthenticationException("I always fail");
        }
    }


    /**
     * AccountPlugin that fails the unit-test if called.  This is used to
     * ensure that the list of plugins does not extend beyond a particular
     * point
     */
    public static final class FailsTest
            implements GPlazmaAccountPlugin
    {
        @Override
        public void account(Set<Principal> authorizedPrincipals)
                throws AuthenticationException
        {
            fail("mistaken attempt to query plugin");
        }
    }


    /**
     * AccountPlugin that requires a principal.  If the set of principals
     * contains the KerberosPrincipal paul@DESY.DE then account will succeed;
     * if not, then the account will fail.
     */
    public static final class SucceedIfPaul implements GPlazmaAccountPlugin
    {
        @Override
        public void account(Set<Principal> authorizedPrincipals)
                throws AuthenticationException
        {
            checkAuthentication(
                    authorizedPrincipals.contains(PAUL_KERBEROS_PRINCIPAL),
                    "you are not Paul");
        }
    }


    /**
     * An AccountPlugin that contains a bug; any attempt to use the
     * account method will trigger this bug.
     */
    public static final class Buggy implements GPlazmaAccountPlugin
    {
        @Override
        public void account(Set<Principal> authorizedPrincipals)
                throws AuthenticationException
        {
            throw new RuntimeException("this is a bug");
        }
    }
}
