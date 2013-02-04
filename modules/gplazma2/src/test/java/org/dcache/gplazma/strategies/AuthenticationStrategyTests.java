package org.dcache.gplazma.strategies;

import java.security.Principal;
import java.util.Set;
import org.dcache.gplazma.AuthenticationException;
import static org.junit.Assert.assertNotNull;
import org.junit.Before;
import org.junit.Test;
import org.dcache.gplazma.plugins.GPlazmaAuthenticationPlugin;
import org.dcache.gplazma.configuration.parser.FactoryConfigurationException;
import static org.dcache.gplazma.configuration.ConfigurationItemControl.*;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.ImmutableList;
import org.dcache.gplazma.monitor.IgnoringLoginMonitor;
import org.dcache.gplazma.monitor.LoginMonitor;

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

    private List<GPlazmaPluginElement<GPlazmaAuthenticationPlugin>> empltyList =
            Lists.newArrayList();

    private List<GPlazmaPluginElement<GPlazmaAuthenticationPlugin>> oneDoNothingPlugins =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new DoNotingStrategy(),"nothing",REQUIRED)
        );

    private List<GPlazmaPluginElement<GPlazmaAuthenticationPlugin>> successRequiredPlugins =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new DoNotingStrategy(),"nothing",REQUIRED),
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new AlwaysAuthenticateStrategy(),"always",REQUIRED)
        );

    private List<GPlazmaPluginElement<GPlazmaAuthenticationPlugin>> successOptionalPlugins =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new DoNotingStrategy(),"nothing",OPTIONAL),
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new AlwaysAuthenticateStrategy(),"always",OPTIONAL)
        );

    private List<GPlazmaPluginElement<GPlazmaAuthenticationPlugin>> successRequisitePlugins =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new DoNotingStrategy(),"nothing",REQUISITE),
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new AlwaysAuthenticateStrategy(),"always",REQUISITE)
        );

    private List<GPlazmaPluginElement<GPlazmaAuthenticationPlugin>> successSufficientPlugins =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new DoNotingStrategy(),"nothing",SUFFICIENT),
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new AlwaysAuthenticateStrategy(),"always",SUFFICIENT)
        );

    private List<GPlazmaPluginElement<GPlazmaAuthenticationPlugin>> failedPlugins =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new AlwaysAuthenticateStrategy(),"always",REQUIRED),
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new ThrowAuthenticationExceptionStrategy(),"throw-auth",REQUIRED)
        );

    private List<GPlazmaPluginElement<GPlazmaAuthenticationPlugin>> testOptionalFailingPlugins =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new AlwaysAuthenticateStrategy(),"always",REQUIRED),
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new ThrowAuthenticationExceptionStrategy(),"throw-auth",OPTIONAL)
        );

    private List<GPlazmaPluginElement<GPlazmaAuthenticationPlugin>> testRequesitePlugins1 =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new ThrowTestAuthenticationExceptionStrategy(),"throw-test-auth",REQUISITE),
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new ThrowRuntimeExceptionStrategy(),"throw-run",REQUIRED)
        );

    private List<GPlazmaPluginElement<GPlazmaAuthenticationPlugin>> testRequesitePlugins2 =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new ThrowTestAuthenticationExceptionStrategy(),"throw-test-auth",REQUIRED),
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new ThrowAuthenticationExceptionStrategy(),"throw-auth",REQUISITE),
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new ThrowRuntimeExceptionStrategy(),"throw-run",REQUIRED)
        );

    private List<GPlazmaPluginElement<GPlazmaAuthenticationPlugin>> sufficientPluginFollowedByFailedArray =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new AlwaysAuthenticateStrategy(),"always",SUFFICIENT),
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new ThrowRuntimeExceptionStrategy(),"throw-run",REQUIRED)
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
