package org.dcache.gplazma.strategies;

import java.security.Principal;
import java.util.Set;
import java.util.HashSet;
import org.dcache.gplazma.HomeDirectory;
import org.dcache.gplazma.RootDirectory;
import org.dcache.gplazma.AuthenticationException;
import static org.junit.Assert.assertNotNull;
import org.junit.Before;
import org.junit.Test;
import org.dcache.gplazma.SessionID;
import org.dcache.gplazma.SessionAttribute;
import org.dcache.gplazma.plugins.GPlazmaSessionPlugin;
import static org.dcache.gplazma.configuration.ConfigurationItemControl.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

/**
 *
 * @author timur
 */
public class SessionStrategyTests {

    private static final String DefaultStrategyFactory =
            "org.dcache.gplazma.strategies.DefaultStrategyFactory";
    private StrategyFactory strategyFactory;

    private List<GPlazmaPluginElement<GPlazmaSessionPlugin>> empltyList =
            new ArrayList();

    private GPlazmaPluginElement<GPlazmaSessionPlugin>[] oneDoNopthingPluginArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new DoNotingStrategy(),REQUIRED)
    };

    private GPlazmaPluginElement<GPlazmaSessionPlugin>[] successRequiredPluginArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new DoNotingStrategy(),REQUIRED),
            new GPlazmaPluginElement(new AlwaysAssignAttributesStrategy(),REQUIRED)
    };

    private GPlazmaPluginElement<GPlazmaSessionPlugin>[] successOptionalPluginArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new DoNotingStrategy(),OPTIONAL),
            new GPlazmaPluginElement(new AlwaysAssignAttributesStrategy(),OPTIONAL)
    };

    private GPlazmaPluginElement<GPlazmaSessionPlugin>[] successRequisitePluginArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new DoNotingStrategy(),REQUISITE),
            new GPlazmaPluginElement(new AlwaysAssignAttributesStrategy(),REQUISITE)
    };

    private GPlazmaPluginElement<GPlazmaSessionPlugin>[] successSufficientPluginArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new DoNotingStrategy(),SUFFICIENT),
            new GPlazmaPluginElement(new AlwaysAssignAttributesStrategy(),SUFFICIENT)
    };

    private GPlazmaPluginElement<GPlazmaSessionPlugin>[] failedPluginArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new AlwaysAssignAttributesStrategy(),REQUIRED),
            new GPlazmaPluginElement(new ThrowAuthenticationExceptionStrategy(),REQUIRED)
    };

    private GPlazmaPluginElement<GPlazmaSessionPlugin>[] testOptionalFailingPluginArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new AlwaysAssignAttributesStrategy(),REQUIRED),
            new GPlazmaPluginElement(new ThrowAuthenticationExceptionStrategy(),OPTIONAL)
    };

    private GPlazmaPluginElement<GPlazmaSessionPlugin>[] testRequesitePluginArray1 =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new ThrowTestAuthenticationExceptionStrategy(),REQUISITE),
            new GPlazmaPluginElement(new ThrowRuntimeExceptionStrategy(),REQUIRED),
    };

    private GPlazmaPluginElement<GPlazmaSessionPlugin>[] testRequesitePluginArray2 =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new ThrowTestAuthenticationExceptionStrategy(),REQUIRED),
            new GPlazmaPluginElement(new ThrowAuthenticationExceptionStrategy(),REQUISITE),
            new GPlazmaPluginElement(new ThrowRuntimeExceptionStrategy(),REQUIRED),
    };

    private GPlazmaPluginElement<GPlazmaSessionPlugin>[] sufficientPluginFollowedByFailedArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new AlwaysAssignAttributesStrategy(),SUFFICIENT),
            new GPlazmaPluginElement(new ThrowRuntimeExceptionStrategy(),REQUIRED),
    };

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
     * This is expected to succeed as a configuration without
     * any plugins imposes no constraints on a login account, so
     * cannot fail.
     */
    @Test
    public void testEmptyConfig() throws AuthenticationException{

        SessionStrategy strategy =
                strategyFactory.newSessionStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(empltyList);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = new HashSet();
        Set<SessionAttribute> sessionAttributes = new HashSet();
        strategy.session(sessionId,
                authorizedPrincipals, sessionAttributes);
    }

    /**
     * This is expected to succeed as a configuration without
     * a single plugin that always succeeds imposes no
     * constraints on a login account, so cannot fail.
     */
    @Test
    public void testDoNothingOneElementConfig() throws AuthenticationException{

        SessionStrategy strategy =
                strategyFactory.newSessionStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(oneDoNopthingPluginArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = new HashSet();
        Set<SessionAttribute> sessionAttributes = new HashSet();
        strategy.session(sessionId,
                authorizedPrincipals, sessionAttributes);
    }

    @Test (expected=AuthenticationException.class)
    public void testFailedConfig() throws AuthenticationException{

        SessionStrategy strategy =
                strategyFactory.newSessionStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(failedPluginArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = new HashSet();
        Set<SessionAttribute> sessionAttributes = new HashSet();
        strategy.session(sessionId,
                authorizedPrincipals, sessionAttributes);
    }

    @Test
    public void testRequiredConfig() throws AuthenticationException{

        SessionStrategy strategy =
                strategyFactory.newSessionStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(successRequiredPluginArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = new HashSet();
        Set<SessionAttribute> sessionAttributes = new HashSet();
        strategy.session(sessionId,
                authorizedPrincipals, sessionAttributes);
    }

    @Test
    public void testRequisiteConfig() throws AuthenticationException{

        SessionStrategy strategy =
                strategyFactory.newSessionStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(successRequisitePluginArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = new HashSet();
        Set<SessionAttribute> sessionAttributes = new HashSet();
        strategy.session(sessionId,
                authorizedPrincipals, sessionAttributes);
    }
    @Test
    public void testOptionalConfig() throws AuthenticationException{

        SessionStrategy strategy =
                strategyFactory.newSessionStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(successOptionalPluginArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = new HashSet();
        Set<SessionAttribute> sessionAttributes = new HashSet();
        strategy.session(sessionId,
                authorizedPrincipals, sessionAttributes);
    }

    @Test
    public void testSufficientConfig() throws AuthenticationException{

        SessionStrategy strategy =
                strategyFactory.newSessionStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(successSufficientPluginArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = new HashSet();
        Set<SessionAttribute> sessionAttributes = new HashSet();
        strategy.session(sessionId,
                authorizedPrincipals, sessionAttributes);
    }

    /**
     * in this case the first sufficient plugin should suceed and the second plugin
     * that throws RuntimeException should be never called
     * @throws org.dcache.gplazma.AuthenticationException
     */
    @Test
    public void testSufficientPluginFollowedByFailedConfig() throws AuthenticationException{

        SessionStrategy strategy =
                strategyFactory.newSessionStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(sufficientPluginFollowedByFailedArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = new HashSet();
        Set<SessionAttribute> sessionAttributes = new HashSet();
        strategy.session(sessionId,
                authorizedPrincipals, sessionAttributes);
    }

    /**
     * Failing plugin is optional in testOptionalPluginArray
     * So overall authenticate should succeed
     * @throws org.dcache.gplazma.AuthenticationException
     */
    @Test
    public void testOptionalFailingConfig() throws AuthenticationException{

        SessionStrategy strategy =
                strategyFactory.newSessionStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(testOptionalFailingPluginArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = new HashSet();
        Set<SessionAttribute> sessionAttributes = new HashSet();
        strategy.session(sessionId,
                authorizedPrincipals, sessionAttributes);
    }

    /**
     * The exception thrown by first required plugin is
     * thrown when the requisite plugin failure is encountered
     * Third plugin should not be executed.
     * @throws org.dcache.gplazma.AuthenticationException
     */
    @Test (expected=TestAuthenticationException.class)
    public void testRequesiteConfig1() throws AuthenticationException{

        SessionStrategy strategy =
                strategyFactory.newSessionStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(testRequesitePluginArray1));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = new HashSet();
        Set<SessionAttribute> sessionAttributes = new HashSet();
        strategy.session(sessionId,
                authorizedPrincipals, sessionAttributes);
    }

    /**
     * The exception thrown by first required plugin is
     * thrown when the requisite plugin failure is encountered
     * Third plugin should not be executed.
     * @throws org.dcache.gplazma.TestAuthenticationException
     */
    @Test (expected=TestAuthenticationException.class)
    public void testRequesiteConfig2() throws AuthenticationException{

        SessionStrategy strategy =
                strategyFactory.newSessionStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(testRequesitePluginArray2));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = new HashSet();
        Set<SessionAttribute> sessionAttributes = new HashSet();
        strategy.session(sessionId,
                authorizedPrincipals, sessionAttributes);
    }

    private static final class DoNotingStrategy
            implements GPlazmaSessionPlugin {

        public void session(SessionID sID,
                Set<Principal> authorizedPrincipals,
                Set<SessionAttribute> sessionAttributes)
                throws AuthenticationException {
        }
    }

    private static final class AlwaysAssignAttributesStrategy
        implements GPlazmaSessionPlugin {

        public void session(SessionID sID,
                Set<Principal> authorizedPrincipals,
                Set<SessionAttribute> sessionAttributes)
                throws AuthenticationException {
            HomeDirectory homeDir = new HomeDirectory("/home/user");
            RootDirectory rootDir = new RootDirectory("/pnfs/org");
            sessionAttributes.add(homeDir);
            sessionAttributes.add(rootDir);

        }
    }

    private static final class ThrowAuthenticationExceptionStrategy
        implements GPlazmaSessionPlugin {

        public void session(SessionID sID,
                Set<Principal> authorizedPrincipals,
                Set<SessionAttribute> sessionAttributes)
                throws AuthenticationException {
            throw new AuthenticationException("I always fail");
        }
    }

    private static final class ThrowTestAuthenticationExceptionStrategy
        implements GPlazmaSessionPlugin {

        public void session(SessionID sID,
                Set<Principal> authorizedPrincipals,
                Set<SessionAttribute> sessionAttributes)
                throws AuthenticationException {
            throw new TestAuthenticationException("I always fail too");
        }
    }

    private static final class ThrowRuntimeExceptionStrategy
        implements GPlazmaSessionPlugin {

        public void session(SessionID sID,
                Set<Principal> authorizedPrincipals,
                Set<SessionAttribute> sessionAttributes)
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
