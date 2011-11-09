package org.dcache.gplazma.strategies;

import java.security.Principal;
import java.util.Set;
import org.dcache.gplazma.AuthenticationException;
import static org.junit.Assert.assertNotNull;
import org.junit.Before;
import org.junit.Test;
import org.dcache.gplazma.SessionID;
import org.dcache.gplazma.plugins.GPlazmaAuthenticationPlugin;
import static org.dcache.gplazma.configuration.ConfigurationItemControl.*;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.ImmutableList;

/**
 *
 * @author timur
 */
public class AuthenticationStrategyTests {

    private static final String DefaultStrategyFactory =
            "org.dcache.gplazma.strategies.DefaultStrategyFactory";
    private StrategyFactory strategyFactory;

    private List<GPlazmaPluginElement<GPlazmaAuthenticationPlugin>> empltyList =
            Lists.newArrayList();

    private List<GPlazmaPluginElement<GPlazmaAuthenticationPlugin>> oneDoNothingPlugins =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new DoNotingStrategy(),REQUIRED)
    );

    private List<GPlazmaPluginElement<GPlazmaAuthenticationPlugin>> successRequiredPlugins =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new DoNotingStrategy(),REQUIRED),
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new AlwaysAuthenticateStrategy(),REQUIRED)
    );

    private List<GPlazmaPluginElement<GPlazmaAuthenticationPlugin>> successOptionalPlugins =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new DoNotingStrategy(),OPTIONAL),
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new AlwaysAuthenticateStrategy(),OPTIONAL)
    );

    private List<GPlazmaPluginElement<GPlazmaAuthenticationPlugin>> successRequisitePlugins =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new DoNotingStrategy(),REQUISITE),
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new AlwaysAuthenticateStrategy(),REQUISITE)
    );

    private List<GPlazmaPluginElement<GPlazmaAuthenticationPlugin>> successSufficientPlugins =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new DoNotingStrategy(),SUFFICIENT),
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new AlwaysAuthenticateStrategy(),SUFFICIENT)
    );

    private List<GPlazmaPluginElement<GPlazmaAuthenticationPlugin>> failedPlugins =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new AlwaysAuthenticateStrategy(),REQUIRED),
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new ThrowAuthenticationExceptionStrategy(),REQUIRED)
    );

    private List<GPlazmaPluginElement<GPlazmaAuthenticationPlugin>> testOptionalFailingPlugins =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new AlwaysAuthenticateStrategy(),REQUIRED),
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new ThrowAuthenticationExceptionStrategy(),OPTIONAL)
    );

    private List<GPlazmaPluginElement<GPlazmaAuthenticationPlugin>> testRequesitePlugins1 =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new ThrowTestAuthenticationExceptionStrategy(),REQUISITE),
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new ThrowRuntimeExceptionStrategy(),REQUIRED)
    );

    private List<GPlazmaPluginElement<GPlazmaAuthenticationPlugin>> testRequesitePlugins2 =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new ThrowTestAuthenticationExceptionStrategy(),REQUIRED),
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new ThrowAuthenticationExceptionStrategy(),REQUISITE),
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new ThrowRuntimeExceptionStrategy(),REQUIRED)
    );

    private List<GPlazmaPluginElement<GPlazmaAuthenticationPlugin>> sufficientPluginFollowedByFailedArray =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new AlwaysAuthenticateStrategy(),SUFFICIENT),
            new GPlazmaPluginElement<GPlazmaAuthenticationPlugin>(new ThrowRuntimeExceptionStrategy(),REQUIRED)
    );

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
        Set<Object> publicCredential = Sets.newHashSet();
        Set<Object> privateCredential = Sets.newHashSet();
        Set<Principal> identifiedPrincipals = Sets.newHashSet();
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
        strategy.setPlugins(oneDoNothingPlugins);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Object> publicCredential = Sets.newHashSet();
        Set<Object> privateCredential = Sets.newHashSet();
        Set<Principal> identifiedPrincipals = Sets.newHashSet();
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
        strategy.setPlugins(failedPlugins);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Object> publicCredential = Sets.newHashSet();
        Set<Object> privateCredential = Sets.newHashSet();
        Set<Principal> identifiedPrincipals = Sets.newHashSet();
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
        strategy.setPlugins(successRequiredPlugins);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Object> publicCredential = Sets.newHashSet();
        Set<Object> privateCredential = Sets.newHashSet();
        Set<Principal> identifiedPrincipals = Sets.newHashSet();
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
        strategy.setPlugins(successRequisitePlugins);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Object> publicCredential = Sets.newHashSet();
        Set<Object> privateCredential = Sets.newHashSet();
        Set<Principal> identifiedPrincipals = Sets.newHashSet();
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
        strategy.setPlugins(successOptionalPlugins);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Object> publicCredential = Sets.newHashSet();
        Set<Object> privateCredential = Sets.newHashSet();
        Set<Principal> identifiedPrincipals = Sets.newHashSet();
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
        strategy.setPlugins(successSufficientPlugins);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Object> publicCredential = Sets.newHashSet();
        Set<Object> privateCredential = Sets.newHashSet();
        Set<Principal> identifiedPrincipals = Sets.newHashSet();
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
        strategy.setPlugins(sufficientPluginFollowedByFailedArray);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Object> publicCredential = Sets.newHashSet();
        Set<Object> privateCredential = Sets.newHashSet();
        Set<Principal> identifiedPrincipals = Sets.newHashSet();
        strategy.authenticate(sessionId,
                publicCredential,
                privateCredential,
                identifiedPrincipals);
    }

    /**
     * Failing plugin is optional in testOptionalPlugins
     * So overall authenticate should succeed
     * @throws org.dcache.gplazma.AuthenticationException
     */
    @Test
    public void testOptionalFailingConfig() throws AuthenticationException{

        AuthenticationStrategy strategy =
                strategyFactory.newAuthenticationStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(testOptionalFailingPlugins);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Object> publicCredential = Sets.newHashSet();
        Set<Object> privateCredential = Sets.newHashSet();
        Set<Principal> identifiedPrincipals = Sets.newHashSet();
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
        strategy.setPlugins(testRequesitePlugins1);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Object> publicCredential = Sets.newHashSet();
        Set<Object> privateCredential = Sets.newHashSet();
        Set<Principal> identifiedPrincipals = Sets.newHashSet();
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
        strategy.setPlugins(testRequesitePlugins2);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Object> publicCredential = Sets.newHashSet();
        Set<Object> privateCredential = Sets.newHashSet();
        Set<Principal> identifiedPrincipals = Sets.newHashSet();
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

        public <T extends Comparable<? super T>> void setSessionID(T sessID) {
        }
    }

    private static final class TestPrincipal implements Principal {

        public String getName() {
            return "TestPrincipal";
        }

    }

    private static final class TestAuthenticationException extends AuthenticationException {
        static final long serialVersionUID = 1261734010814147892L;

        public TestAuthenticationException(String message) {
            super(message);
        }
    }

}
