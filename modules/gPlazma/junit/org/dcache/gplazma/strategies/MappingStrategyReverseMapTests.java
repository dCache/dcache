package org.dcache.gplazma.strategies;

import java.security.Principal;
import java.util.Set;
import java.util.HashSet;
import org.dcache.gplazma.AuthenticationException;
import static org.junit.Assert.assertNotNull;
import org.junit.Before;
import org.junit.Test;
import org.dcache.gplazma.SessionID;
import org.dcache.gplazma.plugins.GPlazmaMappingPlugin;
import static org.dcache.gplazma.configuration.ConfigurationItemControl.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.auth.FQANPrincipal;

/**
 *
 * @author timur
 */
public class MappingStrategyReverseMapTests {

    private static final String DefaultStrategyFactory =
            "org.dcache.gplazma.strategies.DefaultStrategyFactory";
    private StrategyFactory strategyFactory;

    private List<GPlazmaPluginElement<GPlazmaMappingPlugin>> empltyList =
            new ArrayList();

    private GPlazmaPluginElement<GPlazmaMappingPlugin>[] oneDoNopthingPluginArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new DoNotingStrategy(),REQUIRED)
    };

    private GPlazmaPluginElement<GPlazmaMappingPlugin>[] successRequiredPluginArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new DoNotingStrategy(),REQUIRED),
            new GPlazmaPluginElement(new AlwaysMapToCompleteSetStrategy(),REQUIRED)
    };

    private GPlazmaPluginElement<GPlazmaMappingPlugin>[] successOptionalPluginArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new DoNotingStrategy(),OPTIONAL),
            new GPlazmaPluginElement(new AlwaysMapToCompleteSetStrategy(),OPTIONAL)
    };

    private GPlazmaPluginElement<GPlazmaMappingPlugin>[] successRequisitePluginArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new DoNotingStrategy(),REQUISITE),
            new GPlazmaPluginElement(new AlwaysMapToCompleteSetStrategy(),REQUISITE)
    };

    private GPlazmaPluginElement<GPlazmaMappingPlugin>[] successSufficientPluginArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new DoNotingStrategy(),SUFFICIENT),
            new GPlazmaPluginElement(new AlwaysMapToCompleteSetStrategy(),SUFFICIENT)
    };

    private GPlazmaPluginElement<GPlazmaMappingPlugin>[] failedPluginArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new AlwaysMapToCompleteSetStrategy(),REQUIRED),
            new GPlazmaPluginElement(new ThrowAuthenticationExceptionStrategy(),REQUIRED)
    };

    private GPlazmaPluginElement<GPlazmaMappingPlugin>[] testOptionalFailingPluginArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new AlwaysMapToCompleteSetStrategy(),REQUIRED),
            new GPlazmaPluginElement(new ThrowAuthenticationExceptionStrategy(),OPTIONAL)
    };

    private GPlazmaPluginElement<GPlazmaMappingPlugin>[] testRequesitePluginArray1 =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new ThrowTestAuthenticationExceptionStrategy(),REQUISITE),
            new GPlazmaPluginElement(new ThrowRuntimeExceptionStrategy(),REQUIRED),
    };

    private GPlazmaPluginElement<GPlazmaMappingPlugin>[] testRequesitePluginArray2 =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new ThrowTestAuthenticationExceptionStrategy(),REQUIRED),
            new GPlazmaPluginElement(new ThrowAuthenticationExceptionStrategy(),REQUISITE),
            new GPlazmaPluginElement(new ThrowRuntimeExceptionStrategy(),REQUIRED),
    };

    private GPlazmaPluginElement<GPlazmaMappingPlugin>[] sufficientPluginFollowedByFailedArray =
        new GPlazmaPluginElement[] {
            new GPlazmaPluginElement(new AlwaysMapToCompleteSetStrategy(),SUFFICIENT),
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
        Set<Principal> authorizedPrincipals = new HashSet();
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
        strategy.setPlugins(Arrays.asList(oneDoNopthingPluginArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Principal principal = new UidPrincipal(1L);
        Set<Principal> authorizedPrincipals = new HashSet();
        strategy.reverseMap(sessionId,
                principal,
                authorizedPrincipals);
    }

    @Test (expected=AuthenticationException.class)
    public void testFailedConfig() throws AuthenticationException{

        MappingStrategy strategy =
                strategyFactory.newMappingStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(failedPluginArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Principal principal = new UidPrincipal(1L);
        Set<Principal> authorizedPrincipals = new HashSet();
        strategy.reverseMap(sessionId,
                principal,
                authorizedPrincipals);
    }

    @Test
    public void testRequiredConfig() throws AuthenticationException{

        MappingStrategy strategy =
                strategyFactory.newMappingStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(successRequiredPluginArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Principal principal = new UidPrincipal(1L);
        Set<Principal> authorizedPrincipals = new HashSet();
        strategy.reverseMap(sessionId,
                principal,
                authorizedPrincipals);
    }

    @Test
    public void testRequisiteConfig() throws AuthenticationException{

        MappingStrategy strategy =
                strategyFactory.newMappingStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(successRequisitePluginArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Principal principal = new UidPrincipal(1L);
        Set<Principal> authorizedPrincipals = new HashSet();
        strategy.reverseMap(sessionId,
                principal,
                authorizedPrincipals);
    }
    @Test
    public void testOptionalConfig() throws AuthenticationException{

        MappingStrategy strategy =
                strategyFactory.newMappingStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(successOptionalPluginArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Principal principal = new UidPrincipal(1L);
        Set<Principal> authorizedPrincipals = new HashSet();
        strategy.reverseMap(sessionId,
                principal,
                authorizedPrincipals);
    }

    @Test
    public void testSufficientConfig() throws AuthenticationException{

        MappingStrategy strategy =
                strategyFactory.newMappingStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(successSufficientPluginArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Principal principal = new UidPrincipal(1L);
        Set<Principal> authorizedPrincipals = new HashSet();
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
        strategy.setPlugins(Arrays.asList(sufficientPluginFollowedByFailedArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Principal principal = new UidPrincipal(1L);
        Set<Principal> authorizedPrincipals = new HashSet();
        strategy.reverseMap(sessionId,
                principal,
                authorizedPrincipals);
    }

    /**
     * Failing plugin is optional in testOptionalPluginArray
     * So overall authenticate should succeed
     * @throws org.dcache.gplazma.AuthenticationException
     */
    @Test
    public void testOptionalFailingConfig() throws AuthenticationException{

        MappingStrategy strategy =
                strategyFactory.newMappingStrategy();
        assertNotNull(strategy);
        strategy.setPlugins(Arrays.asList(testOptionalFailingPluginArray));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Principal principal = new UidPrincipal(1L);
        Set<Principal> authorizedPrincipals = new HashSet();
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
        strategy.setPlugins(Arrays.asList(testRequesitePluginArray1));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Principal principal = new UidPrincipal(1L);
        Set<Principal> authorizedPrincipals = new HashSet();
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
        strategy.setPlugins(Arrays.asList(testRequesitePluginArray2));
        TestSessionId sessionId = new TestSessionId();
        sessionId.setSessionID(Integer.valueOf(0));
        Principal principal = new UidPrincipal(1L);
        Set<Principal> authorizedPrincipals = new HashSet();
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
        public TestAuthenticationException(String message) {
            super(message);
        }
    }

}
