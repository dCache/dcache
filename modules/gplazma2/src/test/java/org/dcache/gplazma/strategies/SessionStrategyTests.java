package org.dcache.gplazma.strategies;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import java.security.Principal;
import java.util.List;
import java.util.Set;

import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.configuration.parser.FactoryConfigurationException;
import org.dcache.gplazma.monitor.IgnoringLoginMonitor;
import org.dcache.gplazma.monitor.LoginMonitor;
import org.dcache.gplazma.plugins.GPlazmaSessionPlugin;

import static org.dcache.gplazma.configuration.ConfigurationItemControl.*;
import static org.junit.Assert.assertNotNull;

/**
 *
 * @author timur
 */
public class SessionStrategyTests
{
    private static final String DefaultStrategyFactory =
            "org.dcache.gplazma.strategies.DefaultStrategyFactory";
    private StrategyFactory strategyFactory;

    private static final LoginMonitor IGNORING_LOGIN_MONITOR =
            new IgnoringLoginMonitor();

    private List<GPlazmaPluginService<GPlazmaSessionPlugin>> emptyList =
        Lists.newArrayList();

    private List<GPlazmaPluginService<GPlazmaSessionPlugin>> oneDoNothingPlugins =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaSessionPlugin>(new DoNotingStrategy(),"nothing",REQUIRED)
        );

    private List<GPlazmaPluginService<GPlazmaSessionPlugin>> successRequiredPlugins =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaSessionPlugin>(new DoNotingStrategy(),"nothing",REQUIRED),
            new GPlazmaPluginService<GPlazmaSessionPlugin>(new AlwaysAssignAttributesStrategy(),"always",REQUIRED)
        );

    private List<GPlazmaPluginService<GPlazmaSessionPlugin>> successOptionalPlugins =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaSessionPlugin>(new DoNotingStrategy(),"nothing",OPTIONAL),
            new GPlazmaPluginService<GPlazmaSessionPlugin>(new AlwaysAssignAttributesStrategy(),"always",OPTIONAL)
        );

    private List<GPlazmaPluginService<GPlazmaSessionPlugin>> successRequisitePlugins =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaSessionPlugin>(new DoNotingStrategy(),"nothing",REQUISITE),
            new GPlazmaPluginService<GPlazmaSessionPlugin>(new AlwaysAssignAttributesStrategy(),"always",REQUISITE)
        );

    private List<GPlazmaPluginService<GPlazmaSessionPlugin>> successSufficientPlugins =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaSessionPlugin>(new DoNotingStrategy(),"nothing",SUFFICIENT),
            new GPlazmaPluginService<GPlazmaSessionPlugin>(new AlwaysAssignAttributesStrategy(),"always",SUFFICIENT)
        );

    private List<GPlazmaPluginService<GPlazmaSessionPlugin>> failedPlugins =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaSessionPlugin>(new AlwaysAssignAttributesStrategy(),"always",REQUIRED),
            new GPlazmaPluginService<GPlazmaSessionPlugin>(new ThrowAuthenticationExceptionStrategy(),"throw-auth",REQUIRED)
        );

    private List<GPlazmaPluginService<GPlazmaSessionPlugin>> testOptionalFailingPlugins =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaSessionPlugin>(new AlwaysAssignAttributesStrategy(),"always",REQUIRED),
            new GPlazmaPluginService<GPlazmaSessionPlugin>(new ThrowAuthenticationExceptionStrategy(),"throw-auth",OPTIONAL)
        );

    private List<GPlazmaPluginService<GPlazmaSessionPlugin>> testRequesitePlugins1 =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaSessionPlugin>(new ThrowTestAuthenticationExceptionStrategy(),"throw-test-auth",REQUISITE),
            new GPlazmaPluginService<GPlazmaSessionPlugin>(new ThrowRuntimeExceptionStrategy(),"throw-run",REQUIRED)
        );

    private List<GPlazmaPluginService<GPlazmaSessionPlugin>> testRequesitePlugins2 =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaSessionPlugin>(new ThrowTestAuthenticationExceptionStrategy(),"throw-test-auth",REQUIRED),
            new GPlazmaPluginService<GPlazmaSessionPlugin>(new ThrowAuthenticationExceptionStrategy(),"throw-auth",REQUISITE),
            new GPlazmaPluginService<GPlazmaSessionPlugin>(new ThrowRuntimeExceptionStrategy(),"throw-run",REQUIRED)
        );

    private List<GPlazmaPluginService<GPlazmaSessionPlugin>> sufficientPluginFollowedByFailedArray =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaSessionPlugin>(new AlwaysAssignAttributesStrategy(),"always",SUFFICIENT),
            new GPlazmaPluginService<GPlazmaSessionPlugin>(new ThrowRuntimeExceptionStrategy(),"throw-run",REQUIRED)
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
        AccountStrategy authStrategy = factory.newAccountStrategy();
        assertNotNull(authStrategy);
    }

    /**
     * This is expected to succeed as a configuration without
     * any plugins imposes no constraints on a login account, so
     * cannot fail.
     */
    @Test
    public void testEmptyConfig() throws AuthenticationException
    {
        SessionStrategy strategy =
                strategyFactory.newSessionStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(emptyList);
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        Set<Object> sessionAttributes = Sets.newHashSet();
        strategy.session(IGNORING_LOGIN_MONITOR, authorizedPrincipals,
                sessionAttributes);
    }

    /**
     * This is expected to succeed as a configuration without
     * a single plugin that always succeeds imposes no
     * constraints on a login account, so cannot fail.
     */
    @Test
    public void testDoNothingOneElementConfig() throws AuthenticationException
    {
        SessionStrategy strategy =
                strategyFactory.newSessionStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(oneDoNothingPlugins);
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        Set<Object> sessionAttributes = Sets.newHashSet();
        strategy.session(IGNORING_LOGIN_MONITOR, authorizedPrincipals,
                sessionAttributes);
    }

    @Test(expected=AuthenticationException.class)
    public void testFailedConfig() throws AuthenticationException
    {
        SessionStrategy strategy =
                strategyFactory.newSessionStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(failedPlugins);
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        Set<Object> sessionAttributes = Sets.newHashSet();
        strategy.session(IGNORING_LOGIN_MONITOR, authorizedPrincipals,
                sessionAttributes);
    }

    @Test
    public void testRequiredConfig() throws AuthenticationException
    {
        SessionStrategy strategy =
                strategyFactory.newSessionStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(successRequiredPlugins);
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        Set<Object> sessionAttributes = Sets.newHashSet();
        strategy.session(IGNORING_LOGIN_MONITOR, authorizedPrincipals,
                sessionAttributes);
    }

    @Test
    public void testRequisiteConfig() throws AuthenticationException
    {
        SessionStrategy strategy =
                strategyFactory.newSessionStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(successRequisitePlugins);
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        Set<Object> sessionAttributes = Sets.newHashSet();
        strategy.session(IGNORING_LOGIN_MONITOR, authorizedPrincipals,
                sessionAttributes);
    }

    @Test
    public void testOptionalConfig() throws AuthenticationException
    {
        SessionStrategy strategy =
                strategyFactory.newSessionStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(successOptionalPlugins);
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        Set<Object> sessionAttributes = Sets.newHashSet();
        strategy.session(IGNORING_LOGIN_MONITOR, authorizedPrincipals,
                sessionAttributes);
    }

    @Test
    public void testSufficientConfig() throws AuthenticationException
    {
        SessionStrategy strategy =
                strategyFactory.newSessionStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(successSufficientPlugins);
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        Set<Object> sessionAttributes = Sets.newHashSet();
        strategy.session(IGNORING_LOGIN_MONITOR, authorizedPrincipals,
                sessionAttributes);
    }

    /**
     * in this case the first sufficient plugin should suceed and the second plugin
     * that throws RuntimeException should be never called
     * @throws AuthenticationException
     */
    @Test
    public void testSufficientPluginFollowedByFailedConfig()
            throws AuthenticationException
    {
        SessionStrategy strategy =
                strategyFactory.newSessionStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(sufficientPluginFollowedByFailedArray);
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        Set<Object> sessionAttributes = Sets.newHashSet();
        strategy.session(IGNORING_LOGIN_MONITOR, authorizedPrincipals,
                sessionAttributes);
    }

    /**
     * Failing plugin is optional in testOptionalPlugins
     * So overall authenticate should succeed
     * @throws AuthenticationException
     */
    @Test
    public void testOptionalFailingConfig() throws AuthenticationException
    {
        SessionStrategy strategy =
                strategyFactory.newSessionStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(testOptionalFailingPlugins);
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        Set<Object> sessionAttributes = Sets.newHashSet();
        strategy.session(IGNORING_LOGIN_MONITOR, authorizedPrincipals,
                sessionAttributes);
    }

    /**
     * The exception thrown by first required plugin is
     * thrown when the requisite plugin failure is encountered
     * Third plugin should not be executed.
     * @throws AuthenticationException
     */
    @Test(expected=TestAuthenticationException.class)
    public void testRequesiteConfig1() throws AuthenticationException
    {
        SessionStrategy strategy =
                strategyFactory.newSessionStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(testRequesitePlugins1);
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        Set<Object> sessionAttributes = Sets.newHashSet();
        strategy.session(IGNORING_LOGIN_MONITOR, authorizedPrincipals,
                sessionAttributes);
    }

    /**
     * The exception thrown by first required plugin is
     * thrown when the requisite plugin failure is encountered
     * Third plugin should not be executed.
     * @throws org.dcache.gplazma.TestAuthenticationException
     */
    @Test(expected=TestAuthenticationException.class)
    public void testRequesiteConfig2() throws AuthenticationException
    {
        SessionStrategy strategy =
                strategyFactory.newSessionStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(testRequesitePlugins2);
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        Set<Object> sessionAttributes = Sets.newHashSet();
        strategy.session(IGNORING_LOGIN_MONITOR, authorizedPrincipals,
                sessionAttributes);
    }

    private static final class DoNotingStrategy implements GPlazmaSessionPlugin
    {
        @Override
        public void session(Set<Principal> authorizedPrincipals,
                Set<Object> sessionAttributes)
                throws AuthenticationException {
        }
    }

    private static final class AlwaysAssignAttributesStrategy
            implements GPlazmaSessionPlugin
    {
        @Override
        public void session(Set<Principal> authorizedPrincipals,
                Set<Object> sessionAttributes) throws AuthenticationException
        {
            HomeDirectory homeDir = new HomeDirectory("/home/user");
            RootDirectory rootDir = new RootDirectory("/pnfs/org");
            sessionAttributes.add(homeDir);
            sessionAttributes.add(rootDir);

        }
    }

    private static final class ThrowAuthenticationExceptionStrategy
            implements GPlazmaSessionPlugin
    {
        @Override
        public void session(Set<Principal> authorizedPrincipals,
                Set<Object> sessionAttributes) throws AuthenticationException
        {
            throw new AuthenticationException("I always fail");
        }
    }

    private static final class ThrowTestAuthenticationExceptionStrategy
            implements GPlazmaSessionPlugin
    {
        @Override
        public void session(Set<Principal> authorizedPrincipals,
                Set<Object> sessionAttributes) throws AuthenticationException
        {
            throw new TestAuthenticationException("I always fail too");
        }
    }

    private static final class ThrowRuntimeExceptionStrategy
            implements GPlazmaSessionPlugin
    {
        @Override
        public void session(Set<Principal> authorizedPrincipals,
                Set<Object> sessionAttributes) throws AuthenticationException
        {
            throw new RuntimeException("That is what I call an exception");
        }
    }

    private static final class TestAuthenticationException
            extends AuthenticationException
    {
        static final long serialVersionUID = -3072227909975189097L;

        public TestAuthenticationException(String message)
        {
            super(message);
        }
    }
}
