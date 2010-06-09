package org.dcache.gplazma.strategies;

import java.security.Principal;
import java.util.Set;
import java.util.HashSet;
import org.dcache.gplazma.AuthenticationException;
import static org.junit.Assert.assertNotNull;
import org.junit.Before;
import org.junit.Test;
import org.dcache.gplazma.SessionID;
import org.dcache.gplazma.plugins.GPlazmaAccountPlugin;
import static org.dcache.gplazma.configuration.ConfigurationItemControl.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

/**
 *
 * @author timur
 */
public class AccountStrategyTests {

    private static final String DefaultStrategyFactory =
            "org.dcache.gplazma.strategies.DefaultStrategyFactory";
    private StrategyFactory strategyFactory;

    private List<GPlazmaPluginElement<GPlazmaAccountPlugin>> empltyList =
            new ArrayList();

    private GPlazmaPluginElement<GPlazmaAccountPlugin>[] oneDoNopthingPluginArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new DoNotingStrategy(),REQUIRED)
    };

    private GPlazmaPluginElement<GPlazmaAccountPlugin>[] successRequiredPluginArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new DoNotingStrategy(),REQUIRED),
            new GPlazmaPluginElement(new AlwaysAccountStrategy(),REQUIRED)
    };

    private GPlazmaPluginElement<GPlazmaAccountPlugin>[] successOptionalPluginArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new DoNotingStrategy(),OPTIONAL),
            new GPlazmaPluginElement(new AlwaysAccountStrategy(),OPTIONAL)
    };

    private GPlazmaPluginElement<GPlazmaAccountPlugin>[] successRequisitePluginArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new DoNotingStrategy(),REQUISITE),
            new GPlazmaPluginElement(new AlwaysAccountStrategy(),REQUISITE)
    };

    private GPlazmaPluginElement<GPlazmaAccountPlugin>[] successSufficientPluginArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new DoNotingStrategy(),SUFFICIENT),
            new GPlazmaPluginElement(new AlwaysAccountStrategy(),SUFFICIENT)
    };

    private GPlazmaPluginElement<GPlazmaAccountPlugin>[] failedPluginArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new AlwaysAccountStrategy(),REQUIRED),
            new GPlazmaPluginElement(new ThrowAuthenticationExceptionStrategy(),REQUIRED)
    };

    private GPlazmaPluginElement<GPlazmaAccountPlugin>[] testOptionalFailingPluginArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new AlwaysAccountStrategy(),REQUIRED),
            new GPlazmaPluginElement(new ThrowAuthenticationExceptionStrategy(),OPTIONAL)
    };

    private GPlazmaPluginElement<GPlazmaAccountPlugin>[] testRequesitePluginArray1 =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new ThrowTestAuthenticationExceptionStrategy(),REQUISITE),
            new GPlazmaPluginElement(new ThrowRuntimeExceptionStrategy(),REQUIRED),
    };

    private GPlazmaPluginElement<GPlazmaAccountPlugin>[] testRequesitePluginArray2 =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new ThrowTestAuthenticationExceptionStrategy(),REQUIRED),
            new GPlazmaPluginElement(new ThrowAuthenticationExceptionStrategy(),REQUISITE),
            new GPlazmaPluginElement(new ThrowRuntimeExceptionStrategy(),REQUIRED),
    };

    private GPlazmaPluginElement<GPlazmaAccountPlugin>[] sufficientPluginFollowedByFailedArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new AlwaysAccountStrategy(),SUFFICIENT),
            new GPlazmaPluginElement(new ThrowRuntimeExceptionStrategy(),REQUIRED),
    };

    @Before
    public void setUp() {
        strategyFactory = StrategyFactory.getInstance(DefaultStrategyFactory);
    }


    @Test
    public void testDefaultFactoryGetInstanceReturnsAFactory() {
        StrategyFactory factory =
                StrategyFactory.getInstanse();
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
        strategy.setPlugins(empltyList);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = new HashSet();
        strategy.account(sessionId,
                authorizedPrincipals);
    }

    @Test
    public void testDoNothingOneElementConfig() throws AuthenticationException{

        AccountStrategy strategy =
                strategyFactory.newAccountStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(oneDoNopthingPluginArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = new HashSet();
        strategy.account(sessionId,
                authorizedPrincipals);
    }

    @Test (expected=AuthenticationException.class)
    public void testFailedConfig() throws AuthenticationException{

        AccountStrategy strategy =
                strategyFactory.newAccountStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(failedPluginArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = new HashSet();
        strategy.account(sessionId,
                authorizedPrincipals);
    }

    @Test
    public void testRequiredConfig() throws AuthenticationException{

        AccountStrategy strategy =
                strategyFactory.newAccountStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(successRequiredPluginArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = new HashSet();
        strategy.account(sessionId,
                authorizedPrincipals);
    }

    @Test
    public void testRequisiteConfig() throws AuthenticationException{

        AccountStrategy strategy =
                strategyFactory.newAccountStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(successRequisitePluginArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = new HashSet();
        strategy.account(sessionId,
                authorizedPrincipals);
    }
    @Test
    public void testOptionalConfig() throws AuthenticationException{

        AccountStrategy strategy =
                strategyFactory.newAccountStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(successOptionalPluginArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = new HashSet();
        strategy.account(sessionId,
                authorizedPrincipals);
    }

    @Test
    public void testSufficientConfig() throws AuthenticationException{

        AccountStrategy strategy =
                strategyFactory.newAccountStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(successSufficientPluginArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = new HashSet();
        strategy.account(sessionId,
                authorizedPrincipals);
    }

    /**
     * in this case the first sufficient plugin should suceed and the second plugin
     * that throws RuntimeException should be never called
     * But because we do not know what the success condition is for Account plugins
     * We always call all plugins, so the RuntimeException is expected here
     * @throws org.dcache.gplazma.AuthenticationException
     */
    @Test  (expected=RuntimeException.class)
    public void testSufficientPluginFollowedByFailedConfig() throws AuthenticationException{

        AccountStrategy strategy =
                strategyFactory.newAccountStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(sufficientPluginFollowedByFailedArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = new HashSet();
        strategy.account(sessionId,
                authorizedPrincipals);
    }

    /**
     * Failing plugin is optional in testOptionalPluginArray
     * So overall authenticate should succeed
     * @throws org.dcache.gplazma.AuthenticationException
     */
    @Test
    public void testOptionalFailingConfig() throws AuthenticationException{

        AccountStrategy strategy =
                strategyFactory.newAccountStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(testOptionalFailingPluginArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = new HashSet();
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
        strategy.setPlugins(Arrays.asList(testRequesitePluginArray1));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = new HashSet();
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
        strategy.setPlugins(Arrays.asList(testRequesitePluginArray2));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = new HashSet();
        strategy.account(sessionId,
                authorizedPrincipals);
    }

    private static final class DoNotingStrategy
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

        private Comparable sessionId;
        public <T extends Comparable<? super T>> T getSessionID() {
            return (T)sessionId;
        }

        public <T extends Comparable<? super T>> void setSessionID(T sessID) {
            sessionId = sessID;
        }
    }

    private static final class TestAuthenticationException extends AuthenticationException {
        public TestAuthenticationException(String message) {
            super(message);
        }
    }

}
