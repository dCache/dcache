package org.dcache.gplazma.strategies;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import java.security.Principal;
import java.util.List;
import java.util.Set;

import org.dcache.auth.GidPrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.configuration.parser.FactoryConfigurationException;
import org.dcache.gplazma.monitor.IgnoringLoginMonitor;
import org.dcache.gplazma.monitor.LoginMonitor;
import org.dcache.gplazma.plugins.GPlazmaMappingPlugin;

import static org.dcache.gplazma.configuration.ConfigurationItemControl.*;
import static org.junit.Assert.assertNotNull;

/**
 *
 * @author timur
 */
public class MappingStrategyMapTests
{
    private static final String DefaultStrategyFactory =
            "org.dcache.gplazma.strategies.DefaultStrategyFactory";
    private StrategyFactory strategyFactory;

    private static final LoginMonitor IGNORING_LOGIN_MONITOR =
            new IgnoringLoginMonitor();

    private List<GPlazmaPluginService<GPlazmaMappingPlugin>> emptyList =
            Lists.newArrayList();

    private List<GPlazmaPluginService<GPlazmaMappingPlugin>> oneDoNothingPlugins =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaMappingPlugin>(new DoNotingStrategy(),"nothing",REQUIRED)
        );

    private List<GPlazmaPluginService<GPlazmaMappingPlugin>> successRequiredPlugins =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaMappingPlugin>(new DoNotingStrategy(),"nothing",REQUIRED),
            new GPlazmaPluginService<GPlazmaMappingPlugin>(new AlwaysMapToCompleteSetStrategy(),"always",REQUIRED)
        );

    private List<GPlazmaPluginService<GPlazmaMappingPlugin>> successOptionalPlugins =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaMappingPlugin>(new DoNotingStrategy(),"nothing",OPTIONAL),
            new GPlazmaPluginService<GPlazmaMappingPlugin>(new AlwaysMapToCompleteSetStrategy(),"always",OPTIONAL)
        );

    private List<GPlazmaPluginService<GPlazmaMappingPlugin>> successRequisitePlugins =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaMappingPlugin>(new DoNotingStrategy(),"nothing",REQUISITE),
            new GPlazmaPluginService<GPlazmaMappingPlugin>(new AlwaysMapToCompleteSetStrategy(),"always",REQUISITE)
        );

    private List<GPlazmaPluginService<GPlazmaMappingPlugin>> successSufficientPlugins =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaMappingPlugin>(new DoNotingStrategy(),"nothing",SUFFICIENT),
            new GPlazmaPluginService<GPlazmaMappingPlugin>(new AlwaysMapToCompleteSetStrategy(),"always",SUFFICIENT)
        );

    private List<GPlazmaPluginService<GPlazmaMappingPlugin>> failedPlugins =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaMappingPlugin>(new AlwaysMapToCompleteSetStrategy(),"always",REQUIRED),
            new GPlazmaPluginService<GPlazmaMappingPlugin>(new ThrowAuthenticationExceptionStrategy(),"throw-auth",REQUIRED)
        );

    private List<GPlazmaPluginService<GPlazmaMappingPlugin>> testOptionalFailingPlugins =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaMappingPlugin>(new AlwaysMapToCompleteSetStrategy(),"always",REQUIRED),
            new GPlazmaPluginService<GPlazmaMappingPlugin>(new ThrowAuthenticationExceptionStrategy(),"throw-auth",OPTIONAL)
        );

    private List<GPlazmaPluginService<GPlazmaMappingPlugin>> testRequesitePlugins1 =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaMappingPlugin>(new ThrowTestAuthenticationExceptionStrategy(),"throw-test-auth",REQUISITE),
            new GPlazmaPluginService<GPlazmaMappingPlugin>(new ThrowRuntimeExceptionStrategy(),"throw-run",REQUIRED)
        );

    private List<GPlazmaPluginService<GPlazmaMappingPlugin>> testRequesitePlugins2 =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaMappingPlugin>(new ThrowTestAuthenticationExceptionStrategy(),"throw-test-auth",REQUIRED),
            new GPlazmaPluginService<GPlazmaMappingPlugin>(new ThrowAuthenticationExceptionStrategy(),"throw-auth",REQUISITE),
            new GPlazmaPluginService<GPlazmaMappingPlugin>(new ThrowRuntimeExceptionStrategy(),"throw-run",REQUIRED)
        );

    private List<GPlazmaPluginService<GPlazmaMappingPlugin>> sufficientPluginFollowedByFailedArray =
        ImmutableList.of(
            new GPlazmaPluginService<GPlazmaMappingPlugin>(new AlwaysMapToCompleteSetStrategy(),"always",SUFFICIENT),
            new GPlazmaPluginService<GPlazmaMappingPlugin>(new ThrowRuntimeExceptionStrategy(),"throw-run",REQUIRED)
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
     * This is expected to pass as we don't require
     * gPlazma mapping to produce Root or Home directory
     * attributes.
     */
    @Test
    public void testEmptyConfig() throws AuthenticationException
    {
        MappingStrategy strategy =
                strategyFactory.newMappingStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(emptyList);
        Set<Principal> principals = Sets.newHashSet();
        strategy.map(IGNORING_LOGIN_MONITOR, principals);
    }

    /**
     * This is expected to pass as we don't require
     * gPlazma mapping to produce Root or Home directory
     * attributes.
     */
    @Test
    public void testDoNothingOneElementConfig() throws AuthenticationException
    {
        MappingStrategy strategy =
                strategyFactory.newMappingStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(oneDoNothingPlugins);
        Set<Principal> principals = Sets.newHashSet();
        strategy.map(IGNORING_LOGIN_MONITOR, principals);
    }

    @Test (expected=AuthenticationException.class)
    public void testFailedConfig() throws AuthenticationException
    {
        MappingStrategy strategy =
                strategyFactory.newMappingStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(failedPlugins);
        Set<Principal> principals = Sets.newHashSet();
        strategy.map(IGNORING_LOGIN_MONITOR, principals);
    }

    @Test
    public void testRequiredConfig() throws AuthenticationException
    {
        MappingStrategy strategy =
                strategyFactory.newMappingStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(successRequiredPlugins);
        Set<Principal> principals = Sets.newHashSet();
        strategy.map(IGNORING_LOGIN_MONITOR, principals);
    }

    @Test
    public void testRequisiteConfig() throws AuthenticationException
    {
        MappingStrategy strategy =
                strategyFactory.newMappingStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(successRequisitePlugins);
        Set<Principal> principals = Sets.newHashSet();
        strategy.map(IGNORING_LOGIN_MONITOR, principals);
    }

    @Test
    public void testOptionalConfig() throws AuthenticationException
    {
        MappingStrategy strategy =
                strategyFactory.newMappingStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(successOptionalPlugins);
        Set<Principal> principals = Sets.newHashSet();
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
    }

    @Test
    public void testSufficientConfig() throws AuthenticationException
    {
        MappingStrategy strategy =
                strategyFactory.newMappingStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(successSufficientPlugins);
        Set<Principal> principals = Sets.newHashSet();
        strategy.map(IGNORING_LOGIN_MONITOR, principals);
    }

    /**
     * in this case the first sufficient plugin should suceed and the second plugin
     * that throws RuntimeException should be never called
     * @throws AuthenticationException
     */
    @Test
    public void testSufficientPluginFollowedByFailedConfig() throws AuthenticationException
    {
        MappingStrategy strategy =
                strategyFactory.newMappingStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(sufficientPluginFollowedByFailedArray);
        Set<Principal> principals = Sets.newHashSet();
        strategy.map(IGNORING_LOGIN_MONITOR, principals);
    }

    /**
     * Failing plugin is optional in testOptionalPlugins
     * So overall authenticate should succeed
     * @throws AuthenticationException
     */
    @Test
    public void testOptionalFailingConfig() throws AuthenticationException
    {
        MappingStrategy strategy =
                strategyFactory.newMappingStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(testOptionalFailingPlugins);
        Set<Principal> principals = Sets.newHashSet();
        strategy.map(IGNORING_LOGIN_MONITOR, principals);
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
        MappingStrategy strategy =
                strategyFactory.newMappingStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(testRequesitePlugins1);
        Set<Principal> principals = Sets.newHashSet();
        strategy.map(IGNORING_LOGIN_MONITOR, principals);
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
        MappingStrategy strategy =
                strategyFactory.newMappingStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(testRequesitePlugins2);
        Set<Principal> principals = Sets.newHashSet();
        strategy.map(IGNORING_LOGIN_MONITOR, principals);
    }

    private static final class DoNotingStrategy implements GPlazmaMappingPlugin
    {
        @Override
        public void map(Set<Principal> principals)
                throws AuthenticationException
        {
        }
    }

    private static final class AlwaysMapToCompleteSetStrategy
            implements GPlazmaMappingPlugin
    {
        @Override
        public void map(Set<Principal> principals)
                throws AuthenticationException
        {
            UidPrincipal uid = new UidPrincipal(1L);
            GidPrincipal gid = new GidPrincipal(1L, true);
            UserNamePrincipal userName = new UserNamePrincipal("user");
            principals.add(uid);
            principals.add(gid);
            principals.add(userName);
        }
    }

    private static final class ThrowAuthenticationExceptionStrategy
            implements GPlazmaMappingPlugin
    {
        @Override
        public void map(Set<Principal> principals)
                throws AuthenticationException
        {
            throw new AuthenticationException("I always fail");
        }
    }

    private static final class ThrowTestAuthenticationExceptionStrategy
            implements GPlazmaMappingPlugin
    {
        @Override
        public void map(Set<Principal> principal)
                throws AuthenticationException
        {
            throw new TestAuthenticationException("I always fail too");
        }
    }

    private static final class ThrowRuntimeExceptionStrategy
            implements GPlazmaMappingPlugin
    {
        @Override
        public void map(Set<Principal> principals)
                throws AuthenticationException
        {
            throw new RuntimeException("That is what I call an exception");
        }
    }

    private static final class TestAuthenticationException extends AuthenticationException
    {
        static final long serialVersionUID = -5227474403084419369L;

        public TestAuthenticationException(String message)
        {
            super(message);
        }
    }
}
