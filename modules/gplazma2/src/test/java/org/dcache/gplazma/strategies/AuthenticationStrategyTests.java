package org.dcache.gplazma.strategies;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import java.security.Principal;
import java.util.List;
import java.util.Set;

import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.configuration.parser.FactoryConfigurationException;
import org.dcache.gplazma.monitor.IgnoringLoginMonitor;
import org.dcache.gplazma.monitor.LoginMonitor;
import org.dcache.gplazma.plugins.GPlazmaAuthenticationPlugin;

import static org.dcache.gplazma.configuration.ConfigurationItemControl.*;
import static org.junit.Assert.assertNotNull;

/**
 *
 * @author timur
 */
public class AuthenticationStrategyTests
{
    private static final String DefaultStrategyFactory =
            "org.dcache.gplazma.strategies.DefaultStrategyFactory";
    private StrategyFactory strategyFactory;

    private static final LoginMonitor IGNORING_LOGIN_MONITOR =
            new IgnoringLoginMonitor();

    private List<GPlazmaPluginService<GPlazmaAuthenticationPlugin>> empltyList =
            Lists.newArrayList();

    private List<GPlazmaPluginService<GPlazmaAuthenticationPlugin>> oneDoNothingPlugins =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaAuthenticationPlugin>(new DoNotingStrategy(),"nothing",REQUIRED)
        );

    private List<GPlazmaPluginService<GPlazmaAuthenticationPlugin>> successRequiredPlugins =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaAuthenticationPlugin>(new DoNotingStrategy(),"nothing",REQUIRED),
            new GPlazmaPluginService<GPlazmaAuthenticationPlugin>(new AlwaysAuthenticateStrategy(),"always",REQUIRED)
        );

    private List<GPlazmaPluginService<GPlazmaAuthenticationPlugin>> successOptionalPlugins =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaAuthenticationPlugin>(new DoNotingStrategy(),"nothing",OPTIONAL),
            new GPlazmaPluginService<GPlazmaAuthenticationPlugin>(new AlwaysAuthenticateStrategy(),"always",OPTIONAL)
        );

    private List<GPlazmaPluginService<GPlazmaAuthenticationPlugin>> successRequisitePlugins =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaAuthenticationPlugin>(new DoNotingStrategy(),"nothing",REQUISITE),
            new GPlazmaPluginService<GPlazmaAuthenticationPlugin>(new AlwaysAuthenticateStrategy(),"always",REQUISITE)
        );

    private List<GPlazmaPluginService<GPlazmaAuthenticationPlugin>> successSufficientPlugins =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaAuthenticationPlugin>(new DoNotingStrategy(),"nothing",SUFFICIENT),
            new GPlazmaPluginService<GPlazmaAuthenticationPlugin>(new AlwaysAuthenticateStrategy(),"always",SUFFICIENT)
        );

    private List<GPlazmaPluginService<GPlazmaAuthenticationPlugin>> failedPlugins =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaAuthenticationPlugin>(new AlwaysAuthenticateStrategy(),"always",REQUIRED),
            new GPlazmaPluginService<GPlazmaAuthenticationPlugin>(new ThrowAuthenticationExceptionStrategy(),"throw-auth",REQUIRED)
        );

    private List<GPlazmaPluginService<GPlazmaAuthenticationPlugin>> testOptionalFailingPlugins =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaAuthenticationPlugin>(new AlwaysAuthenticateStrategy(),"always",REQUIRED),
            new GPlazmaPluginService<GPlazmaAuthenticationPlugin>(new ThrowAuthenticationExceptionStrategy(),"throw-auth",OPTIONAL)
        );

    private List<GPlazmaPluginService<GPlazmaAuthenticationPlugin>> testRequesitePlugins1 =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaAuthenticationPlugin>(new ThrowTestAuthenticationExceptionStrategy(),"throw-test-auth",REQUISITE),
            new GPlazmaPluginService<GPlazmaAuthenticationPlugin>(new ThrowRuntimeExceptionStrategy(),"throw-run",REQUIRED)
        );

    private List<GPlazmaPluginService<GPlazmaAuthenticationPlugin>> testRequesitePlugins2 =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaAuthenticationPlugin>(new ThrowTestAuthenticationExceptionStrategy(),"throw-test-auth",REQUIRED),
            new GPlazmaPluginService<GPlazmaAuthenticationPlugin>(new ThrowAuthenticationExceptionStrategy(),"throw-auth",REQUISITE),
            new GPlazmaPluginService<GPlazmaAuthenticationPlugin>(new ThrowRuntimeExceptionStrategy(),"throw-run",REQUIRED)
        );

    private List<GPlazmaPluginService<GPlazmaAuthenticationPlugin>> sufficientPluginFollowedByFailedArray =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaAuthenticationPlugin>(new AlwaysAuthenticateStrategy(),"always",SUFFICIENT),
            new GPlazmaPluginService<GPlazmaAuthenticationPlugin>(new ThrowRuntimeExceptionStrategy(),"throw-run",REQUIRED)
        );

    @Before
    public void setup() throws FactoryConfigurationException
    {
        strategyFactory = StrategyFactory.getInstance(DefaultStrategyFactory);
    }


    @Test
    public void testDefaultFactoryGetInstanceReturnsAFactory()
            throws FactoryConfigurationException
    {
        StrategyFactory factory =
                StrategyFactory.getInstance();
        assertNotNull(factory);
        AuthenticationStrategy authStrategy = factory.newAuthenticationStrategy();
        assertNotNull(authStrategy);
    }

    /**
     *
     * @throws AuthenticationException
     */
    @Test
    public void testEmptyConfig() throws AuthenticationException
    {
        AuthenticationStrategy strategy =
                strategyFactory.newAuthenticationStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(empltyList);
        Set<Object> publicCredential = Sets.newHashSet();
        Set<Object> privateCredential = Sets.newHashSet();
        Set<Principal> identifiedPrincipals = Sets.newHashSet();
        strategy.authenticate(IGNORING_LOGIN_MONITOR, publicCredential,
                privateCredential, identifiedPrincipals);
    }

    @Test
    public void testDoNothingOneElementConfig() throws AuthenticationException
    {
        AuthenticationStrategy strategy =
                strategyFactory.newAuthenticationStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(oneDoNothingPlugins);
        Set<Object> publicCredential = Sets.newHashSet();
        Set<Object> privateCredential = Sets.newHashSet();
        Set<Principal> identifiedPrincipals = Sets.newHashSet();
        strategy.authenticate(IGNORING_LOGIN_MONITOR, publicCredential,
                privateCredential, identifiedPrincipals);
    }

    @Test(expected=AuthenticationException.class)
    public void testFailedConfig() throws AuthenticationException
    {
        AuthenticationStrategy strategy =
                strategyFactory.newAuthenticationStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(failedPlugins);
        Set<Object> publicCredential = Sets.newHashSet();
        Set<Object> privateCredential = Sets.newHashSet();
        Set<Principal> identifiedPrincipals = Sets.newHashSet();
        strategy.authenticate(IGNORING_LOGIN_MONITOR, publicCredential,
                privateCredential, identifiedPrincipals);
    }

    @Test
    public void testRequiredConfig() throws AuthenticationException
    {
        AuthenticationStrategy strategy =
                strategyFactory.newAuthenticationStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(successRequiredPlugins);
        Set<Object> publicCredential = Sets.newHashSet();
        Set<Object> privateCredential = Sets.newHashSet();
        Set<Principal> identifiedPrincipals = Sets.newHashSet();
        strategy.authenticate(IGNORING_LOGIN_MONITOR, publicCredential,
                privateCredential, identifiedPrincipals);
    }

    @Test
    public void testRequisiteConfig() throws AuthenticationException
    {
        AuthenticationStrategy strategy =
                strategyFactory.newAuthenticationStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(successRequisitePlugins);
        Set<Object> publicCredential = Sets.newHashSet();
        Set<Object> privateCredential = Sets.newHashSet();
        Set<Principal> identifiedPrincipals = Sets.newHashSet();
        strategy.authenticate(IGNORING_LOGIN_MONITOR, publicCredential,
                privateCredential, identifiedPrincipals);
    }

    @Test
    public void testOptionalConfig() throws AuthenticationException
    {
        AuthenticationStrategy strategy =
                strategyFactory.newAuthenticationStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(successOptionalPlugins);
        Set<Object> publicCredential = Sets.newHashSet();
        Set<Object> privateCredential = Sets.newHashSet();
        Set<Principal> identifiedPrincipals = Sets.newHashSet();
        strategy.authenticate(IGNORING_LOGIN_MONITOR, publicCredential,
                privateCredential, identifiedPrincipals);
    }

    @Test
    public void testSufficientConfig() throws AuthenticationException
    {
        AuthenticationStrategy strategy =
                strategyFactory.newAuthenticationStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(successSufficientPlugins);
        Set<Object> publicCredential = Sets.newHashSet();
        Set<Object> privateCredential = Sets.newHashSet();
        Set<Principal> identifiedPrincipals = Sets.newHashSet();
        strategy.authenticate(IGNORING_LOGIN_MONITOR, publicCredential,
                privateCredential, identifiedPrincipals);
    }

    /**
     * in this case the first sufficient plugin should succeed and the second plugin
     * that throws RuntimeException should be never called
     */
    @Test
    public void testSufficientPluginFollowedByFailedConfig() throws AuthenticationException
    {
        AuthenticationStrategy strategy =
                strategyFactory.newAuthenticationStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(sufficientPluginFollowedByFailedArray);
        Set<Object> publicCredential = Sets.newHashSet();
        Set<Object> privateCredential = Sets.newHashSet();
        Set<Principal> identifiedPrincipals = Sets.newHashSet();
        strategy.authenticate(IGNORING_LOGIN_MONITOR, publicCredential,
                privateCredential, identifiedPrincipals);
    }

    /**
     * Failing plugin is optional in testOptionalPlugins
     * So overall authenticate should succeed
     * @throws AuthenticationException
     */
    @Test
    public void testOptionalFailingConfig() throws AuthenticationException
    {
        AuthenticationStrategy strategy =
                strategyFactory.newAuthenticationStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(testOptionalFailingPlugins);
        Set<Object> publicCredential = Sets.newHashSet();
        Set<Object> privateCredential = Sets.newHashSet();
        Set<Principal> identifiedPrincipals = Sets.newHashSet();
        strategy.authenticate(IGNORING_LOGIN_MONITOR, publicCredential,
                privateCredential, identifiedPrincipals);
    }

    /**
     * The exception thrown by first required plugin is
     * thrown when the requisite plugin failure is encountered
     * Third plugin should not be executed.
     * @throws AuthenticationException
     */
    @Test (expected=TestAuthenticationException.class)
    public void testRequesiteConfig1() throws AuthenticationException
    {
        AuthenticationStrategy strategy =
                strategyFactory.newAuthenticationStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(testRequesitePlugins1);
        Set<Object> publicCredential = Sets.newHashSet();
        Set<Object> privateCredential = Sets.newHashSet();
        Set<Principal> identifiedPrincipals = Sets.newHashSet();
        strategy.authenticate(IGNORING_LOGIN_MONITOR, publicCredential,
                privateCredential, identifiedPrincipals);
    }

    /**
     * The exception thrown by first required plugin is
     * thrown when the requisite plugin failure is encountered
     * Third plugin should not be executed.
     * @throws org.dcache.gplazma.TestAuthenticationException
     */
    @Test (expected=TestAuthenticationException.class)
    public void testRequesiteConfig2() throws AuthenticationException
    {
        AuthenticationStrategy strategy =
                strategyFactory.newAuthenticationStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(testRequesitePlugins2);
        Set<Object> publicCredential = Sets.newHashSet();
        Set<Object> privateCredential = Sets.newHashSet();
        Set<Principal> identifiedPrincipals = Sets.newHashSet();
        strategy.authenticate(IGNORING_LOGIN_MONITOR, publicCredential,
                privateCredential, identifiedPrincipals);
    }

    private static final class DoNotingStrategy
            implements GPlazmaAuthenticationPlugin
    {
        @Override
        public void authenticate(Set<Object> publicCredentials,
                Set<Object> privateCredentials,
                Set<Principal> identifiedPrincipals)
                throws AuthenticationException
        {
        }
    }

    private static final class AlwaysAuthenticateStrategy
            implements GPlazmaAuthenticationPlugin
    {
        @Override
        public void authenticate(Set<Object> publicCredentials,
                Set<Object> privateCredentials,
                Set<Principal> identifiedPrincipals)
                throws AuthenticationException
        {
            identifiedPrincipals.add(new TestPrincipal());
        }
    }

    private static final class ThrowAuthenticationExceptionStrategy
            implements GPlazmaAuthenticationPlugin
    {
        @Override
        public void authenticate(Set<Object> publicCredentials,
                Set<Object> privateCredentials,
                Set<Principal> identifiedPrincipals)
                throws AuthenticationException
        {
            throw new AuthenticationException("I always fail");
        }
    }

    private static final class ThrowTestAuthenticationExceptionStrategy
            implements GPlazmaAuthenticationPlugin
    {
        @Override
        public void authenticate(Set<Object> publicCredentials,
                Set<Object> privateCredentials,
                Set<Principal> identifiedPrincipals)
                throws AuthenticationException
        {
            throw new TestAuthenticationException("I always fail too");
        }
    }

    private static final class ThrowRuntimeExceptionStrategy
            implements GPlazmaAuthenticationPlugin
    {
        @Override
        public void authenticate(Set<Object> publicCredentials,
                Set<Object> privateCredentials,
                Set<Principal> identifiedPrincipals)
                throws AuthenticationException
        {
            throw new RuntimeException("That is what I call an exception");
        }
    }

    private static final class TestPrincipal implements Principal
    {
        @Override
        public String getName()
        {
            return "TestPrincipal";
        }
    }

    private static final class TestAuthenticationException
            extends AuthenticationException
    {
        static final long serialVersionUID = 1261734010814147892L;

        public TestAuthenticationException(String message)
        {
            super(message);
        }
    }
}
