package org.dcache.gplazma.strategies;

import static org.dcache.gplazma.configuration.ConfigurationItemControl.OPTIONAL;
import static org.dcache.gplazma.configuration.ConfigurationItemControl.REQUIRED;
import static org.dcache.gplazma.configuration.ConfigurationItemControl.REQUISITE;
import static org.dcache.gplazma.configuration.ConfigurationItemControl.SUFFICIENT;
import static org.junit.Assert.assertNotNull;

import java.security.Principal;
import java.util.List;
import java.util.Set;

import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.SessionID;
import org.dcache.gplazma.plugins.GPlazmaAccountPlugin;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 *
 * @author timur
 */
public class AccountStrategyTests {

    private static final String DefaultStrategyFactory =
            "org.dcache.gplazma.strategies.DefaultStrategyFactory";
    private StrategyFactory strategyFactory;

    private List<GPlazmaPluginElement<GPlazmaAccountPlugin>> emptyList =
            Lists.newArrayList();

    private List<GPlazmaPluginElement<GPlazmaAccountPlugin>> oneDoNothingPlugins =
        ImmutableList.of(new GPlazmaPluginElement<GPlazmaAccountPlugin>(new DoNothingStrategy(),"nothing",REQUIRED));

    private List<GPlazmaPluginElement<GPlazmaAccountPlugin>> successRequiredPlugins =
        ImmutableList.of(new GPlazmaPluginElement<GPlazmaAccountPlugin>(new DoNothingStrategy(),"nothing",REQUIRED),
                         new GPlazmaPluginElement<GPlazmaAccountPlugin>(new AlwaysAccountStrategy(),"always",REQUIRED));

    private List<GPlazmaPluginElement<GPlazmaAccountPlugin>> successOptionalPlugins =
        ImmutableList.of(new GPlazmaPluginElement<GPlazmaAccountPlugin>(new DoNothingStrategy(),"nothing",OPTIONAL),
                         new GPlazmaPluginElement<GPlazmaAccountPlugin>(new AlwaysAccountStrategy(),"always",OPTIONAL));

    private List<GPlazmaPluginElement<GPlazmaAccountPlugin>> successRequisitePlugins =
        ImmutableList.of(new GPlazmaPluginElement<GPlazmaAccountPlugin>(new DoNothingStrategy(),"nothing",REQUISITE),
                         new GPlazmaPluginElement<GPlazmaAccountPlugin>(new AlwaysAccountStrategy(),"always",REQUISITE));

    private List<GPlazmaPluginElement<GPlazmaAccountPlugin>> successSufficientPlugins =
        ImmutableList.of(new GPlazmaPluginElement<GPlazmaAccountPlugin>(new DoNothingStrategy(),"nothing",SUFFICIENT),
                         new GPlazmaPluginElement<GPlazmaAccountPlugin>(new AlwaysAccountStrategy(),"always",SUFFICIENT));

    private List<GPlazmaPluginElement<GPlazmaAccountPlugin>> failedPlugins =
        ImmutableList.of(new GPlazmaPluginElement<GPlazmaAccountPlugin>(new AlwaysAccountStrategy(),"always",REQUIRED),
                         new GPlazmaPluginElement<GPlazmaAccountPlugin>(new ThrowAuthenticationExceptionStrategy(),"throw-auth",REQUIRED));

    private List<GPlazmaPluginElement<GPlazmaAccountPlugin>> testOptionalFailingPlugins =
        ImmutableList.of(new GPlazmaPluginElement<GPlazmaAccountPlugin>(new AlwaysAccountStrategy(),"always",REQUIRED),
                         new GPlazmaPluginElement<GPlazmaAccountPlugin>(new ThrowAuthenticationExceptionStrategy(),"throw-auth",OPTIONAL));

    private List<GPlazmaPluginElement<GPlazmaAccountPlugin>> testRequesitePlugins1 =
        ImmutableList.of(new GPlazmaPluginElement<GPlazmaAccountPlugin>(new ThrowTestAuthenticationExceptionStrategy(),"throw-test-auth",REQUISITE),
                         new GPlazmaPluginElement<GPlazmaAccountPlugin>(new ThrowRuntimeExceptionStrategy(),"throw-runtime",REQUIRED));

    private List<GPlazmaPluginElement<GPlazmaAccountPlugin>> testRequesitePlugins2 =
        ImmutableList.of(new GPlazmaPluginElement<GPlazmaAccountPlugin>(new ThrowTestAuthenticationExceptionStrategy(),"throw-test-auth",REQUIRED),
                         new GPlazmaPluginElement<GPlazmaAccountPlugin>(new ThrowAuthenticationExceptionStrategy(),"throw-auth",REQUISITE),
                         new GPlazmaPluginElement<GPlazmaAccountPlugin>(new ThrowRuntimeExceptionStrategy(),"throw-runtime",REQUIRED));

    private List<GPlazmaPluginElement<GPlazmaAccountPlugin>> sufficientPluginFollowedByFailedArray =
        ImmutableList.of(new GPlazmaPluginElement<GPlazmaAccountPlugin>(new AlwaysAccountStrategy(),"always",SUFFICIENT),
                         new GPlazmaPluginElement<GPlazmaAccountPlugin>(new ThrowRuntimeExceptionStrategy(),"throw-runtime",REQUIRED));

    @Before
    public void setUp() {
        strategyFactory = StrategyFactory.getInstance(DefaultStrategyFactory);
    }


    @Test
    public void testDefaultFactoryGetInstanceReturnsAFactory() {
        StrategyFactory factory =
                StrategyFactory.getInstance();
        assertNotNull(factory);
        AccountStrategy authStrategy = factory.newAccountStrategy();
        assertNotNull(authStrategy);
    }

    /**
     *
     * @throws org.dcache.gplazma.AuthenticationException
     */
    @Test
    public void testEmptyConfig() throws AuthenticationException{

        AccountStrategy strategy =
                strategyFactory.newAccountStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(emptyList);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        strategy.account(sessionId,
                authorizedPrincipals);
    }

    @Test
    public void testDoNothingOneElementConfig() throws AuthenticationException{

        AccountStrategy strategy =
                strategyFactory.newAccountStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(oneDoNothingPlugins);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        strategy.account(sessionId,
                authorizedPrincipals);
    }

    @Test (expected=AuthenticationException.class)
    public void testFailedConfig() throws AuthenticationException{

        AccountStrategy strategy =
                strategyFactory.newAccountStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(failedPlugins);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        strategy.account(sessionId,
                authorizedPrincipals);
    }

    @Test
    public void testRequiredConfig() throws AuthenticationException{

        AccountStrategy strategy =
                strategyFactory.newAccountStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(successRequiredPlugins);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        strategy.account(sessionId,
                authorizedPrincipals);
    }

    @Test
    public void testRequisiteConfig() throws AuthenticationException{

        AccountStrategy strategy =
                strategyFactory.newAccountStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(successRequisitePlugins);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        strategy.account(sessionId,
                authorizedPrincipals);
    }
    @Test
    public void testOptionalConfig() throws AuthenticationException{

        AccountStrategy strategy =
                strategyFactory.newAccountStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(successOptionalPlugins);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        strategy.account(sessionId,
                authorizedPrincipals);
    }

    @Test
    public void testSufficientConfig() throws AuthenticationException{

        AccountStrategy strategy =
                strategyFactory.newAccountStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(successSufficientPlugins);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        strategy.account(sessionId,
                authorizedPrincipals);
    }

    /**
     * in this case the first sufficient plugin should succeed and the second plugin
     * that throws RuntimeException should be never called
     */
    @Test
    public void testSufficientPluginFollowedByFailedConfig() throws AuthenticationException{
        AccountStrategy strategy =
                strategyFactory.newAccountStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(sufficientPluginFollowedByFailedArray);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        strategy.account(sessionId,
                authorizedPrincipals);
    }

    /**
     * Failing plugin is optional in testOptionalPlugins
     * So overall authenticate should succeed
     * @throws org.dcache.gplazma.AuthenticationException
     */
    @Test
    public void testOptionalFailingConfig() throws AuthenticationException{

        AccountStrategy strategy =
                strategyFactory.newAccountStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(testOptionalFailingPlugins);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        strategy.account(sessionId,
                authorizedPrincipals);
    }

    /**
     * The exception thrown by first required plugin is
     * thrown when the requisite plugin failure is encountered
     * Third plugin should not be executed.
     * @throws org.dcache.gplazma.AuthenticationException
     */
    @Test (expected=TestAuthenticationException.class)
    public void testRequesiteConfig1() throws AuthenticationException{

        AccountStrategy strategy =
                strategyFactory.newAccountStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(testRequesitePlugins1);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        strategy.account(sessionId,
                authorizedPrincipals);
    }

    /**
     * The exception thrown by first required plugin is
     * thrown when the requisite plugin failure is encountered
     * Third plugin should not be executed.
     * @throws org.dcache.gplazma.TestAuthenticationException
     */
    @Test (expected=TestAuthenticationException.class)
    public void testRequesiteConfig2() throws AuthenticationException{

        AccountStrategy strategy =
                strategyFactory.newAccountStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(testRequesitePlugins2);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        strategy.account(sessionId,
                authorizedPrincipals);
    }

    private static final class DoNothingStrategy
            implements GPlazmaAccountPlugin {

        public void account(SessionID sID,
                Set<Principal> authorizedPrincipals)
                throws AuthenticationException {
        }
    }

    private static final class AlwaysAccountStrategy
        implements GPlazmaAccountPlugin {

        public void account(SessionID sID,
                Set<Principal> authorizedPrincipals)
                throws AuthenticationException {
        }
    }

    private static final class ThrowAuthenticationExceptionStrategy
        implements GPlazmaAccountPlugin {

        public void account(SessionID sID,
                Set<Principal> authorizedPrincipals)
                throws AuthenticationException {
            throw new AuthenticationException("I always fail");
        }
    }

    private static final class ThrowTestAuthenticationExceptionStrategy
        implements GPlazmaAccountPlugin {

        public void account(SessionID sID,
                Set<Principal> authorizedPrincipals)
                throws AuthenticationException {
            throw new TestAuthenticationException("I always fail too");
        }
    }

    private static final class ThrowRuntimeExceptionStrategy
        implements GPlazmaAccountPlugin {

        public void account(SessionID sID,
                Set<Principal> authorizedPrincipals)
                throws AuthenticationException {
            throw new RuntimeException("That is what I call an exception");
        }
    }

    private static final class TestSessionId implements SessionID {

        public <T extends Comparable<? super T>> void setSessionID(T sessID) {
        }
    }

    private static final class TestAuthenticationException extends AuthenticationException {
        private static final long serialVersionUID = 1L;

        public TestAuthenticationException(String message) {
            super(message);
        }
    }

}
