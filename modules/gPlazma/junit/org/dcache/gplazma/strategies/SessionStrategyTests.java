package org.dcache.gplazma.strategies;

import java.security.Principal;
import java.util.Set;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.gplazma.AuthenticationException;
import static org.junit.Assert.assertNotNull;
import org.junit.Before;
import org.junit.Test;
import org.dcache.gplazma.SessionID;
import org.dcache.gplazma.plugins.GPlazmaSessionPlugin;
import static org.dcache.gplazma.configuration.ConfigurationItemControl.*;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.ImmutableList;

/**
 *
 * @author timur
 */
public class SessionStrategyTests {

    private static final String DefaultStrategyFactory =
            "org.dcache.gplazma.strategies.DefaultStrategyFactory";
    private StrategyFactory strategyFactory;

    private List<GPlazmaPluginElement<GPlazmaSessionPlugin>> emptyList =
        Lists.newArrayList();

    private List<GPlazmaPluginElement<GPlazmaSessionPlugin>> oneDoNothingPlugins =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaSessionPlugin>(new DoNotingStrategy(),REQUIRED)
    );

    private List<GPlazmaPluginElement<GPlazmaSessionPlugin>> successRequiredPlugins =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaSessionPlugin>(new DoNotingStrategy(),REQUIRED),
            new GPlazmaPluginElement<GPlazmaSessionPlugin>(new AlwaysAssignAttributesStrategy(),REQUIRED)
    );

    private List<GPlazmaPluginElement<GPlazmaSessionPlugin>> successOptionalPlugins =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaSessionPlugin>(new DoNotingStrategy(),OPTIONAL),
            new GPlazmaPluginElement<GPlazmaSessionPlugin>(new AlwaysAssignAttributesStrategy(),OPTIONAL)
    );

    private List<GPlazmaPluginElement<GPlazmaSessionPlugin>> successRequisitePlugins =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaSessionPlugin>(new DoNotingStrategy(),REQUISITE),
            new GPlazmaPluginElement<GPlazmaSessionPlugin>(new AlwaysAssignAttributesStrategy(),REQUISITE)
    );

    private List<GPlazmaPluginElement<GPlazmaSessionPlugin>> successSufficientPlugins =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaSessionPlugin>(new DoNotingStrategy(),SUFFICIENT),
            new GPlazmaPluginElement<GPlazmaSessionPlugin>(new AlwaysAssignAttributesStrategy(),SUFFICIENT)
    );

    private List<GPlazmaPluginElement<GPlazmaSessionPlugin>> failedPlugins =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaSessionPlugin>(new AlwaysAssignAttributesStrategy(),REQUIRED),
            new GPlazmaPluginElement<GPlazmaSessionPlugin>(new ThrowAuthenticationExceptionStrategy(),REQUIRED)
    );

    private List<GPlazmaPluginElement<GPlazmaSessionPlugin>> testOptionalFailingPlugins =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaSessionPlugin>(new AlwaysAssignAttributesStrategy(),REQUIRED),
            new GPlazmaPluginElement<GPlazmaSessionPlugin>(new ThrowAuthenticationExceptionStrategy(),OPTIONAL)
    );

    private List<GPlazmaPluginElement<GPlazmaSessionPlugin>> testRequesitePlugins1 =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaSessionPlugin>(new ThrowTestAuthenticationExceptionStrategy(),REQUISITE),
            new GPlazmaPluginElement<GPlazmaSessionPlugin>(new ThrowRuntimeExceptionStrategy(),REQUIRED)
    );

    private List<GPlazmaPluginElement<GPlazmaSessionPlugin>> testRequesitePlugins2 =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaSessionPlugin>(new ThrowTestAuthenticationExceptionStrategy(),REQUIRED),
            new GPlazmaPluginElement<GPlazmaSessionPlugin>(new ThrowAuthenticationExceptionStrategy(),REQUISITE),
            new GPlazmaPluginElement<GPlazmaSessionPlugin>(new ThrowRuntimeExceptionStrategy(),REQUIRED)
    );

    private List<GPlazmaPluginElement<GPlazmaSessionPlugin>> sufficientPluginFollowedByFailedArray =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaSessionPlugin>(new AlwaysAssignAttributesStrategy(),SUFFICIENT),
            new GPlazmaPluginElement<GPlazmaSessionPlugin>(new ThrowRuntimeExceptionStrategy(),REQUIRED)
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
        strategy.setPlugins(emptyList);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        Set<Object> sessionAttributes = Sets.newHashSet();
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
        strategy.setPlugins(oneDoNothingPlugins);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        Set<Object> sessionAttributes = Sets.newHashSet();
        strategy.session(sessionId,
                authorizedPrincipals, sessionAttributes);
    }

    @Test (expected=AuthenticationException.class)
    public void testFailedConfig() throws AuthenticationException{

        SessionStrategy strategy =
                strategyFactory.newSessionStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(failedPlugins);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        Set<Object> sessionAttributes = Sets.newHashSet();
        strategy.session(sessionId,
                authorizedPrincipals, sessionAttributes);
    }

    @Test
    public void testRequiredConfig() throws AuthenticationException{

        SessionStrategy strategy =
                strategyFactory.newSessionStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(successRequiredPlugins);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        Set<Object> sessionAttributes = Sets.newHashSet();
        strategy.session(sessionId,
                authorizedPrincipals, sessionAttributes);
    }

    @Test
    public void testRequisiteConfig() throws AuthenticationException{

        SessionStrategy strategy =
                strategyFactory.newSessionStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(successRequisitePlugins);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        Set<Object> sessionAttributes = Sets.newHashSet();
        strategy.session(sessionId,
                authorizedPrincipals, sessionAttributes);
    }
    @Test
    public void testOptionalConfig() throws AuthenticationException{

        SessionStrategy strategy =
                strategyFactory.newSessionStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(successOptionalPlugins);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        Set<Object> sessionAttributes = Sets.newHashSet();
        strategy.session(sessionId,
                authorizedPrincipals, sessionAttributes);
    }

    @Test
    public void testSufficientConfig() throws AuthenticationException{

        SessionStrategy strategy =
                strategyFactory.newSessionStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(successSufficientPlugins);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        Set<Object> sessionAttributes = Sets.newHashSet();
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
        strategy.setPlugins(sufficientPluginFollowedByFailedArray);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        Set<Object> sessionAttributes = Sets.newHashSet();
        strategy.session(sessionId,
                authorizedPrincipals, sessionAttributes);
    }

    /**
     * Failing plugin is optional in testOptionalPlugins
     * So overall authenticate should succeed
     * @throws org.dcache.gplazma.AuthenticationException
     */
    @Test
    public void testOptionalFailingConfig() throws AuthenticationException{

        SessionStrategy strategy =
                strategyFactory.newSessionStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(testOptionalFailingPlugins);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        Set<Object> sessionAttributes = Sets.newHashSet();
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
        strategy.setPlugins(testRequesitePlugins1);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        Set<Object> sessionAttributes = Sets.newHashSet();
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
        strategy.setPlugins(testRequesitePlugins2);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        Set<Object> sessionAttributes = Sets.newHashSet();
        strategy.session(sessionId,
                authorizedPrincipals, sessionAttributes);
    }

    private static final class DoNotingStrategy
            implements GPlazmaSessionPlugin {

        public void session(SessionID sID,
                Set<Principal> authorizedPrincipals,
                Set<Object> sessionAttributes)
                throws AuthenticationException {
        }
    }

    private static final class AlwaysAssignAttributesStrategy
        implements GPlazmaSessionPlugin {

        public void session(SessionID sID,
                Set<Principal> authorizedPrincipals,
                Set<Object> sessionAttributes)
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
                Set<Object> sessionAttributes)
                throws AuthenticationException {
            throw new AuthenticationException("I always fail");
        }
    }

    private static final class ThrowTestAuthenticationExceptionStrategy
        implements GPlazmaSessionPlugin {

        public void session(SessionID sID,
                Set<Principal> authorizedPrincipals,
                Set<Object> sessionAttributes)
                throws AuthenticationException {
            throw new TestAuthenticationException("I always fail too");
        }
    }

    private static final class ThrowRuntimeExceptionStrategy
        implements GPlazmaSessionPlugin {

        public void session(SessionID sID,
                Set<Principal> authorizedPrincipals,
                Set<Object> sessionAttributes)
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
        static final long serialVersionUID = -3072227909975189097L;

        public TestAuthenticationException(String message) {
            super(message);
        }
    }

}
