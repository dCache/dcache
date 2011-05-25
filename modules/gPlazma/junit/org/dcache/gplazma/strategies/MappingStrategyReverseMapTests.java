package org.dcache.gplazma.strategies;

import java.security.Principal;
import java.util.Set;
import org.dcache.gplazma.AuthenticationException;
import static org.junit.Assert.assertNotNull;
import org.junit.Before;
import org.junit.Test;
import org.dcache.gplazma.SessionID;
import org.dcache.gplazma.plugins.GPlazmaMappingPlugin;
import static org.dcache.gplazma.configuration.ConfigurationItemControl.*;
import java.util.List;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.auth.FQANPrincipal;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.ImmutableList;

/**
 *
 * @author timur
 */
public class MappingStrategyReverseMapTests {

    private static final String DefaultStrategyFactory =
            "org.dcache.gplazma.strategies.DefaultStrategyFactory";
    private StrategyFactory strategyFactory;

    private List<GPlazmaPluginElement<GPlazmaMappingPlugin>> empltyList =
            Lists.newArrayList();

    private List<GPlazmaPluginElement<GPlazmaMappingPlugin>> oneDoNothingPlugins =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaMappingPlugin>(new DoNotingStrategy(),REQUIRED)
    );

    private List<GPlazmaPluginElement<GPlazmaMappingPlugin>> successRequiredPlugins =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaMappingPlugin>(new DoNotingStrategy(),REQUIRED),
            new GPlazmaPluginElement<GPlazmaMappingPlugin>(new AlwaysMapToCompleteSetStrategy(),REQUIRED)
    );

    private List<GPlazmaPluginElement<GPlazmaMappingPlugin>> successOptionalPlugins =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaMappingPlugin>(new DoNotingStrategy(),OPTIONAL),
            new GPlazmaPluginElement<GPlazmaMappingPlugin>(new AlwaysMapToCompleteSetStrategy(),OPTIONAL)
    );

    private List<GPlazmaPluginElement<GPlazmaMappingPlugin>> successRequisitePlugins =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaMappingPlugin>(new DoNotingStrategy(),REQUISITE),
            new GPlazmaPluginElement<GPlazmaMappingPlugin>(new AlwaysMapToCompleteSetStrategy(),REQUISITE)
    );

    private List<GPlazmaPluginElement<GPlazmaMappingPlugin>> successSufficientPlugins =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaMappingPlugin>(new DoNotingStrategy(),SUFFICIENT),
            new GPlazmaPluginElement<GPlazmaMappingPlugin>(new AlwaysMapToCompleteSetStrategy(),SUFFICIENT)
    );

    private List<GPlazmaPluginElement<GPlazmaMappingPlugin>> failedPlugins =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaMappingPlugin>(new AlwaysMapToCompleteSetStrategy(),REQUIRED),
            new GPlazmaPluginElement<GPlazmaMappingPlugin>(new ThrowAuthenticationExceptionStrategy(),REQUIRED)
    );

    private List<GPlazmaPluginElement<GPlazmaMappingPlugin>> testOptionalFailingPlugins =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaMappingPlugin>(new AlwaysMapToCompleteSetStrategy(),REQUIRED),
            new GPlazmaPluginElement<GPlazmaMappingPlugin>(new ThrowAuthenticationExceptionStrategy(),OPTIONAL)
    );

    private List<GPlazmaPluginElement<GPlazmaMappingPlugin>> testRequesitePlugins1 =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaMappingPlugin>(new ThrowTestAuthenticationExceptionStrategy(),REQUISITE),
            new GPlazmaPluginElement<GPlazmaMappingPlugin>(new ThrowRuntimeExceptionStrategy(),REQUIRED)
    );

    private List<GPlazmaPluginElement<GPlazmaMappingPlugin>> testRequesitePlugins2 =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaMappingPlugin>(new ThrowTestAuthenticationExceptionStrategy(),REQUIRED),
            new GPlazmaPluginElement<GPlazmaMappingPlugin>(new ThrowAuthenticationExceptionStrategy(),REQUISITE),
            new GPlazmaPluginElement<GPlazmaMappingPlugin>(new ThrowRuntimeExceptionStrategy(),REQUIRED)
    );

    private List<GPlazmaPluginElement<GPlazmaMappingPlugin>> sufficientPluginFollowedByFailedArray =
        ImmutableList.of(
            new GPlazmaPluginElement<GPlazmaMappingPlugin>(new AlwaysMapToCompleteSetStrategy(),SUFFICIENT),
            new GPlazmaPluginElement<GPlazmaMappingPlugin>(new ThrowRuntimeExceptionStrategy(),REQUIRED)
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
     * This is expected to succeed as it is valid for
     * a reverse-mapping not to recover the original principal
     * in all cases.
     */
    @Test
    public void testEmptyConfig() throws AuthenticationException{

        MappingStrategy strategy =
                strategyFactory.newMappingStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(empltyList);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Principal principal = new UidPrincipal(1L);
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        strategy.reverseMap(sessionId,
                principal,
                authorizedPrincipals);
    }

    /**
     * This is expected to succeed as it is valid for
     * a reverse-mapping not to recover the original principal
     * in all cases.
     */
    @Test
    public void testDoNothingOneElementConfig() throws AuthenticationException{

        MappingStrategy strategy =
                strategyFactory.newMappingStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(oneDoNothingPlugins);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Principal principal = new UidPrincipal(1L);
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        strategy.reverseMap(sessionId,
                principal,
                authorizedPrincipals);
    }

    @Test (expected=AuthenticationException.class)
    public void testFailedConfig() throws AuthenticationException{

        MappingStrategy strategy =
                strategyFactory.newMappingStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(failedPlugins);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Principal principal = new UidPrincipal(1L);
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        strategy.reverseMap(sessionId,
                principal,
                authorizedPrincipals);
    }

    @Test
    public void testRequiredConfig() throws AuthenticationException{

        MappingStrategy strategy =
                strategyFactory.newMappingStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(successRequiredPlugins);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Principal principal = new UidPrincipal(1L);
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        strategy.reverseMap(sessionId,
                principal,
                authorizedPrincipals);
    }

    @Test
    public void testRequisiteConfig() throws AuthenticationException{

        MappingStrategy strategy =
                strategyFactory.newMappingStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(successRequisitePlugins);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Principal principal = new UidPrincipal(1L);
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        strategy.reverseMap(sessionId,
                principal,
                authorizedPrincipals);
    }
    @Test
    public void testOptionalConfig() throws AuthenticationException{

        MappingStrategy strategy =
                strategyFactory.newMappingStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(successOptionalPlugins);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Principal principal = new UidPrincipal(1L);
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        strategy.reverseMap(sessionId,
                principal,
                authorizedPrincipals);
    }

    @Test
    public void testSufficientConfig() throws AuthenticationException{

        MappingStrategy strategy =
                strategyFactory.newMappingStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(successSufficientPlugins);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Principal principal = new UidPrincipal(1L);
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        strategy.reverseMap(sessionId,
                principal,
                authorizedPrincipals);
    }

    /**
     * in this case the first sufficient plugin should suceed and the second plugin
     * that throws RuntimeException should be never called
     * @throws org.dcache.gplazma.AuthenticationException
     */
    @Test
    public void testSufficientPluginFollowedByFailedConfig() throws AuthenticationException{

        MappingStrategy strategy =
                strategyFactory.newMappingStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(sufficientPluginFollowedByFailedArray);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Principal principal = new UidPrincipal(1L);
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        strategy.reverseMap(sessionId,
                principal,
                authorizedPrincipals);
    }

    /**
     * Failing plugin is optional in testOptionalPlugins
     * So overall authenticate should succeed
     * @throws org.dcache.gplazma.AuthenticationException
     */
    @Test
    public void testOptionalFailingConfig() throws AuthenticationException{

        MappingStrategy strategy =
                strategyFactory.newMappingStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(testOptionalFailingPlugins);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Principal principal = new UidPrincipal(1L);
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        strategy.reverseMap(sessionId,
                principal,
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

        MappingStrategy strategy =
                strategyFactory.newMappingStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(testRequesitePlugins1);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Principal principal = new UidPrincipal(1L);
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        strategy.reverseMap(sessionId,
                principal,
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

        MappingStrategy strategy =
                strategyFactory.newMappingStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(testRequesitePlugins2);
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Principal principal = new UidPrincipal(1L);
        Set<Principal> authorizedPrincipals = Sets.newHashSet();
        strategy.reverseMap(sessionId,
                principal,
                authorizedPrincipals);
    }

    private static final class DoNotingStrategy
            implements GPlazmaMappingPlugin {

        public void map(SessionID sID,
                Set<Principal> principals,
                Set<Principal> authorizedPrincipals)
                throws AuthenticationException {
        }

        public void reverseMap(SessionID sID,
                Principal principal,
                Set<Principal> principals)
                throws AuthenticationException {
        }
    }

    private static final class AlwaysMapToCompleteSetStrategy
        implements GPlazmaMappingPlugin {

        public void map(SessionID sID,
                Set<Principal> principals,
                Set<Principal> authorizedPrincipals)
                throws AuthenticationException {
            UidPrincipal uid = new UidPrincipal(1L);
            GidPrincipal gid = new GidPrincipal(1L, true);
            UserNamePrincipal userName = new UserNamePrincipal("user");
            authorizedPrincipals.add(uid);
            authorizedPrincipals.add(gid);
            authorizedPrincipals.add(userName);
        }
        public void reverseMap(SessionID sID,
                Principal principal,
                Set<Principal> principals)
                throws AuthenticationException {
            FQANPrincipal fqan = new FQANPrincipal("/expreriment/prod", true);
            principals.add(fqan);
        }
    }

    private static final class ThrowAuthenticationExceptionStrategy
        implements GPlazmaMappingPlugin {

        public void map(SessionID sID,
                Set<Principal> principals,
                Set<Principal> authorizedPrincipals)
                throws AuthenticationException {
            throw new AuthenticationException("I always fail");
        }
        public void reverseMap(SessionID sID,
                Principal principal,
                Set<Principal> principals)
                throws AuthenticationException {
            throw new AuthenticationException("I always fail");
        }
    }

    private static final class ThrowTestAuthenticationExceptionStrategy
        implements GPlazmaMappingPlugin {

        public void map(SessionID sID,
                Set<Principal> principals,
                Set<Principal> authorizedPrincipals)
                throws AuthenticationException {
            throw new TestAuthenticationException("I always fail too");
        }
         public void reverseMap(SessionID sID,
                Principal principal,
                Set<Principal> principals)
                throws AuthenticationException {
            throw new TestAuthenticationException("I always fail too");
        }
   }

    private static final class ThrowRuntimeExceptionStrategy
        implements GPlazmaMappingPlugin {

        public void map(SessionID sID,
                Set<Principal> principals,
                Set<Principal> authorizedPrincipals)
                throws AuthenticationException {
            throw new RuntimeException("That is what I call an exception");
        }
         public void reverseMap(SessionID sID,
                Principal principal,
                Set<Principal> principals)
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
        static final long serialVersionUID = 5248624779760311620L;

        public TestAuthenticationException(String message) {
            super(message);
        }
    }

}
