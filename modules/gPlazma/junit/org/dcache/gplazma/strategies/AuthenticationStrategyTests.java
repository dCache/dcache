package org.dcache.gplazma.strategies;

import java.security.Principal;
import java.util.Set;
import java.util.HashSet;
import org.dcache.gplazma.AuthenticationException;
import static org.junit.Assert.assertNotNull;
import org.junit.Before;
import org.junit.Test;
import org.dcache.gplazma.SessionID;
import org.dcache.gplazma.plugins.GPlazmaAuthenticationPlugin;
import static org.dcache.gplazma.configuration.ConfigurationItemControl.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

/**
 *
 * @author timur
 */
public class AuthenticationStrategyTests {

    private static final String DefaultStrategyFactory =
            "org.dcache.gplazma.strategies.DefaultStrategyFactory";
    private StrategyFactory strategyFactory;

    private List<GPlazmaPluginElement<GPlazmaAuthenticationPlugin>> empltyList =
            new ArrayList();

    private GPlazmaPluginElement<GPlazmaAuthenticationPlugin>[] oneDoNopthingPluginArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new DoNotingStrategy(),REQUIRED)
    };

    private GPlazmaPluginElement<GPlazmaAuthenticationPlugin>[] successRequiredPluginArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new DoNotingStrategy(),REQUIRED),
            new GPlazmaPluginElement(new AlwaysAuthenticateStrategy(),REQUIRED)
    };

    private GPlazmaPluginElement<GPlazmaAuthenticationPlugin>[] successOptionalPluginArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new DoNotingStrategy(),OPTIONAL),
            new GPlazmaPluginElement(new AlwaysAuthenticateStrategy(),OPTIONAL)
    };

    private GPlazmaPluginElement<GPlazmaAuthenticationPlugin>[] successRequisitePluginArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new DoNotingStrategy(),REQUISITE),
            new GPlazmaPluginElement(new AlwaysAuthenticateStrategy(),REQUISITE)
    };

    private GPlazmaPluginElement<GPlazmaAuthenticationPlugin>[] successSufficientPluginArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new DoNotingStrategy(),SUFFICIENT),
            new GPlazmaPluginElement(new AlwaysAuthenticateStrategy(),SUFFICIENT)
    };

    private GPlazmaPluginElement<GPlazmaAuthenticationPlugin>[] failedPluginArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new AlwaysAuthenticateStrategy(),REQUIRED),
            new GPlazmaPluginElement(new ThrowAuthenticationExceptionStrategy(),REQUIRED)
    };

    private GPlazmaPluginElement<GPlazmaAuthenticationPlugin>[] testOptionalFailingPluginArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new AlwaysAuthenticateStrategy(),REQUIRED),
            new GPlazmaPluginElement(new ThrowAuthenticationExceptionStrategy(),OPTIONAL)
    };

    private GPlazmaPluginElement<GPlazmaAuthenticationPlugin>[] testRequesitePluginArray1 =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new ThrowTestAuthenticationExceptionStrategy(),REQUISITE),
            new GPlazmaPluginElement(new ThrowRuntimeExceptionStrategy(),REQUIRED),
    };

    private GPlazmaPluginElement<GPlazmaAuthenticationPlugin>[] testRequesitePluginArray2 =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new ThrowTestAuthenticationExceptionStrategy(),REQUIRED),
            new GPlazmaPluginElement(new ThrowAuthenticationExceptionStrategy(),REQUISITE),
            new GPlazmaPluginElement(new ThrowRuntimeExceptionStrategy(),REQUIRED),
    };

    private GPlazmaPluginElement<GPlazmaAuthenticationPlugin>[] sufficientPluginFollowedByFailedArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new AlwaysAuthenticateStrategy(),SUFFICIENT),
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
        AuthenticationStrategy authStrategy = factory.newAuthenticationStrategy();
        assertNotNull(authStrategy);
    }

    /**
     *
     * @throws org.dcache.gplazma.AuthenticationException
     */
    @Test
    public void testEmptyConfig() throws AuthenticationException{

        AuthenticationStrategy strategy =
                strategyFactory.newAuthenticationStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(empltyList);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Object> publicCredential = new HashSet();
        Set<Object> privateCredential = new HashSet();
        Set<Principal> identifiedPrincipals = new HashSet();
        strategy.authenticate(sessionId,
                publicCredential,
                privateCredential,
                identifiedPrincipals);
    }

    @Test
    public void testDoNothingOneElementConfig() throws AuthenticationException{

        AuthenticationStrategy strategy =
                strategyFactory.newAuthenticationStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(oneDoNopthingPluginArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Object> publicCredential = new HashSet();
        Set<Object> privateCredential = new HashSet();
        Set<Principal> identifiedPrincipals = new HashSet();
        strategy.authenticate(sessionId,
                publicCredential,
                privateCredential,
                identifiedPrincipals);
    }

    @Test (expected=AuthenticationException.class)
    public void testFailedConfig() throws AuthenticationException{

        AuthenticationStrategy strategy =
                strategyFactory.newAuthenticationStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(failedPluginArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Object> publicCredential = new HashSet();
        Set<Object> privateCredential = new HashSet();
        Set<Principal> identifiedPrincipals = new HashSet();
        strategy.authenticate(sessionId,
                publicCredential,
                privateCredential,
                identifiedPrincipals);
    }

    @Test
    public void testRequiredConfig() throws AuthenticationException{

        AuthenticationStrategy strategy =
                strategyFactory.newAuthenticationStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(successRequiredPluginArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Object> publicCredential = new HashSet();
        Set<Object> privateCredential = new HashSet();
        Set<Principal> identifiedPrincipals = new HashSet();
        strategy.authenticate(sessionId,
                publicCredential,
                privateCredential,
                identifiedPrincipals);
    }

    @Test
    public void testRequisiteConfig() throws AuthenticationException{

        AuthenticationStrategy strategy =
                strategyFactory.newAuthenticationStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(successRequisitePluginArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Object> publicCredential = new HashSet();
        Set<Object> privateCredential = new HashSet();
        Set<Principal> identifiedPrincipals = new HashSet();
        strategy.authenticate(sessionId,
                publicCredential,
                privateCredential,
                identifiedPrincipals);
    }
    @Test
    public void testOptionalConfig() throws AuthenticationException{

        AuthenticationStrategy strategy =
                strategyFactory.newAuthenticationStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(successOptionalPluginArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Object> publicCredential = new HashSet();
        Set<Object> privateCredential = new HashSet();
        Set<Principal> identifiedPrincipals = new HashSet();
        strategy.authenticate(sessionId,
                publicCredential,
                privateCredential,
                identifiedPrincipals);
    }

    @Test
    public void testSufficientConfig() throws AuthenticationException{

        AuthenticationStrategy strategy =
                strategyFactory.newAuthenticationStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(successSufficientPluginArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Object> publicCredential = new HashSet();
        Set<Object> privateCredential = new HashSet();
        Set<Principal> identifiedPrincipals = new HashSet();
        strategy.authenticate(sessionId,
                publicCredential,
                privateCredential,
                identifiedPrincipals);
    }

    /**
     * in this case the first sufficient plugin should succeed and the second plugin
     * that throws RuntimeException should be never called
     */
    @Test
    public void testSufficientPluginFollowedByFailedConfig() throws AuthenticationException{

        AuthenticationStrategy strategy =
                strategyFactory.newAuthenticationStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(sufficientPluginFollowedByFailedArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Object> publicCredential = new HashSet();
        Set<Object> privateCredential = new HashSet();
        Set<Principal> identifiedPrincipals = new HashSet();
        strategy.authenticate(sessionId,
                publicCredential,
                privateCredential,
                identifiedPrincipals);
    }

    /**
     * Failing plugin is optional in testOptionalPluginArray
     * So overall authenticate should succeed
     * @throws org.dcache.gplazma.AuthenticationException
     */
    @Test
    public void testOptionalFailingConfig() throws AuthenticationException{

        AuthenticationStrategy strategy =
                strategyFactory.newAuthenticationStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(testOptionalFailingPluginArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Object> publicCredential = new HashSet();
        Set<Object> privateCredential = new HashSet();
        Set<Principal> identifiedPrincipals = new HashSet();
        strategy.authenticate(sessionId,
                publicCredential,
                privateCredential,
                identifiedPrincipals);
    }

    /**
     * The exception thrown by first required plugin is
     * thrown when the requisite plugin failure is encountered
     * Third plugin should not be executed.
     * @throws org.dcache.gplazma.AuthenticationException
     */
    @Test (expected=TestAuthenticationException.class)
    public void testRequesiteConfig1() throws AuthenticationException{

        AuthenticationStrategy strategy =
                strategyFactory.newAuthenticationStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(testRequesitePluginArray1));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Object> publicCredential = new HashSet();
        Set<Object> privateCredential = new HashSet();
        Set<Principal> identifiedPrincipals = new HashSet();
        strategy.authenticate(sessionId,
                publicCredential,
                privateCredential,
                identifiedPrincipals);
    }

    /**
     * The exception thrown by first required plugin is
     * thrown when the requisite plugin failure is encountered
     * Third plugin should not be executed.
     * @throws org.dcache.gplazma.TestAuthenticationException
     */
    @Test (expected=TestAuthenticationException.class)
    public void testRequesiteConfig2() throws AuthenticationException{

        AuthenticationStrategy strategy =
                strategyFactory.newAuthenticationStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(testRequesitePluginArray2));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Object> publicCredential = new HashSet();
        Set<Object> privateCredential = new HashSet();
        Set<Principal> identifiedPrincipals = new HashSet();
        strategy.authenticate(sessionId,
                publicCredential,
                privateCredential,
                identifiedPrincipals);
    }

    private static final class DoNotingStrategy
            implements GPlazmaAuthenticationPlugin {

        public void authenticate(SessionID sID,
                Set<Object> publicCredentials,
                Set<Object> privateCredentials,
                Set<Principal> identifiedPrincipals)
                throws AuthenticationException {
        }
    }

    private static final class AlwaysAuthenticateStrategy
        implements GPlazmaAuthenticationPlugin {

        public void authenticate(SessionID sID,
                Set<Object> publicCredentials,
                Set<Object> privateCredentials,
                Set<Principal> identifiedPrincipals)
                throws AuthenticationException {
             identifiedPrincipals.add(new TestPrincipal());
        }
    }

    private static final class ThrowAuthenticationExceptionStrategy
        implements GPlazmaAuthenticationPlugin {

        public void authenticate(SessionID sID,
                Set<Object> publicCredentials,
                Set<Object> privateCredentials,
                Set<Principal> identifiedPrincipals)
                throws AuthenticationException {
            throw new AuthenticationException("I always fail");
        }
    }

    private static final class ThrowTestAuthenticationExceptionStrategy
        implements GPlazmaAuthenticationPlugin {

        public void authenticate(SessionID sID,
                Set<Object> publicCredentials,
                Set<Object> privateCredentials,
                Set<Principal> identifiedPrincipals)
                throws AuthenticationException {
            throw new TestAuthenticationException("I always fail too");
        }
    }

    private static final class ThrowRuntimeExceptionStrategy
        implements GPlazmaAuthenticationPlugin {

        public void authenticate(SessionID sID,
                Set<Object> publicCredentials,
                Set<Object> privateCredentials,
                Set<Principal> identifiedPrincipals)
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

    private static final class TestPrincipal implements Principal {

        public String getName() {
            return "TestPrincipal";
        }

    }

    private static final class TestAuthenticationException extends AuthenticationException {
        public TestAuthenticationException(String message) {
            super(message);
        }
    }

}
