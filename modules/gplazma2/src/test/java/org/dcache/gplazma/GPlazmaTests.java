package org.dcache.gplazma;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.security.auth.Subject;

import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.dcache.auth.GidPrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.gplazma.configuration.Configuration;
import org.dcache.gplazma.configuration.ConfigurationItem;
import org.dcache.gplazma.configuration.ConfigurationLoadingStrategy;
import org.dcache.gplazma.configuration.StaticContentConfigurationLoadingStrategy;
import org.dcache.gplazma.loader.PluginXmlGenerator;
import org.dcache.gplazma.loader.Utf8DataClassLoader;
import org.dcache.gplazma.loader.XmlResourcePluginRepositoryFactory;

import static org.dcache.gplazma.configuration.ConfigurationItemControl.*;
import static org.dcache.gplazma.configuration.ConfigurationItemType.*;
import static org.junit.Assert.*;

/**
 *
 * @author timur
 */
public class GPlazmaTests {

    private static final String AUTH_NAME = "findPrincipals";
    private static final String MAPPING_NAME="identityMap";
    private static final String ACCOUNT_NAME="uidAccount";
    private static final String SESSION_NAME="homeRootSession";
    private static final String FAIL_NAME = "fail_all";
    private static final String USER_NAME = "root";
    private static final String HOME_PATH_ARG_VALUE = "/root";
    private static final String ROOT_PATH_ARG_VALUE = "/";
    private static final String READ_ONLY_ARG_VALUE = "true";
    private static final String ROOT_UID = "0";
    private static final String ROOT_GID = "0";

    public static final Properties EMPTY_PROPERTIES = new Properties();

    private static final ConfigurationItem AUTH_CONFIG_ITEM =
            new ConfigurationItem(AUTHENTICATION, REQUIRED, AUTH_NAME, EMPTY_PROPERTIES);
    private static final ConfigurationItem MAPPING_CONFIG_ITEM =
            new ConfigurationItem(MAPPING, REQUIRED, MAPPING_NAME, EMPTY_PROPERTIES);
    private static ConfigurationItem ACCOUNT_CONFIG_ITEM;
    private static ConfigurationItem SESSION_CONFIG_ITEM;
    private static final ConfigurationItem FAIL_AUTH_CONFIG_ITEM =
            new ConfigurationItem(AUTHENTICATION, REQUIRED, FAIL_NAME, EMPTY_PROPERTIES);
    private static final ConfigurationItem FAIL_MAPPING_CONFIG_ITEM =
            new ConfigurationItem(MAPPING, REQUIRED, FAIL_NAME, EMPTY_PROPERTIES);
    private static final ConfigurationItem FAIL_ACCOUNT_CONFIG_ITEM =
            new ConfigurationItem(ACCOUNT, REQUIRED, FAIL_NAME, EMPTY_PROPERTIES);
    private static final ConfigurationItem FAIL_SESSION_CONFIG_ITEM =
            new ConfigurationItem(SESSION, REQUIRED, FAIL_NAME, EMPTY_PROPERTIES);
    private static final ConfigurationItem FAIL_OPTIONAL_AUTH_CONFIG_ITEM =
            new ConfigurationItem(AUTHENTICATION, OPTIONAL, FAIL_NAME, EMPTY_PROPERTIES);
    private static final ConfigurationItem FAIL_OPTIONAL_MAPPING_CONFIG_ITEM =
            new ConfigurationItem(MAPPING, OPTIONAL, FAIL_NAME, EMPTY_PROPERTIES);
    private static final ConfigurationItem FAIL_OPTIONAL_ACCOUNT_CONFIG_ITEM =
            new ConfigurationItem(ACCOUNT, OPTIONAL, FAIL_NAME, EMPTY_PROPERTIES);
    private static final ConfigurationItem FAIL_OPTIONAL_SESSION_CONFIG_ITEM =
            new ConfigurationItem(SESSION, OPTIONAL, FAIL_NAME, EMPTY_PROPERTIES);

    private Utf8DataClassLoader _classLoader;
    private PluginXmlGenerator _pluginXml;

    private Subject _inputSubject;

    private static final ConfigurationItem[] EMPTY_CONFIG_ARRAY =
            new ConfigurationItem[0];
    private static final Configuration EMPTY_CONFIG =
            new Configuration(Arrays.asList(EMPTY_CONFIG_ARRAY));

    @Before
    public void setUp() {
        _classLoader = new Utf8DataClassLoader(XmlResourcePluginRepositoryFactory.RESOURCE_PATH);
        Thread currentThread = Thread.currentThread();
        currentThread.setContextClassLoader( _classLoader);

        Properties accountProperties = new Properties();
        accountProperties.put("uid", ROOT_UID);
        ACCOUNT_CONFIG_ITEM = new ConfigurationItem(ACCOUNT, REQUISITE, ACCOUNT_NAME, accountProperties);

        Properties sessionProperties = new Properties();
        sessionProperties.put(AddHomeRootSessionPlugin.USER_KEY, USER_NAME);
        sessionProperties.put(AddHomeRootSessionPlugin.HOME_KEY, HOME_PATH_ARG_VALUE);
        sessionProperties.put(AddHomeRootSessionPlugin.ROOT_KEY, ROOT_PATH_ARG_VALUE);
        sessionProperties.put(AddHomeRootSessionPlugin.READONLY_KEY, READ_ONLY_ARG_VALUE);
        SESSION_CONFIG_ITEM = new ConfigurationItem(SESSION, REQUIRED, SESSION_NAME, sessionProperties);

        _pluginXml = new PluginXmlGenerator();
        _pluginXml.clear();
        _pluginXml.addPlugin( Collections.singleton( AUTH_NAME),
                FindPrincipalsInCredentialsPlugin.class);
        _pluginXml.addPlugin( Collections.singleton( MAPPING_NAME),
                IdentityMappingPlugin.class);
        _pluginXml.addPlugin( Collections.singleton( ACCOUNT_NAME),
                CheckUIDAccountPlugin.class);
        _pluginXml.addPlugin( Collections.singleton( SESSION_NAME),
                AddHomeRootSessionPlugin.class);
        _pluginXml.addPlugin( Collections.singleton( FAIL_NAME),
                AlwaysFailPlugin.class);
        _classLoader.addResource( _pluginXml);
        CheckUIDAccountPlugin.reset();

       //input subject
       _inputSubject = newTestSubject();
    }

    /**
     * The authenticatie strategy will throw an exception as there are no principals
     * identified
     * @throws AuthenticationException
     */
    @Test (expected=AuthenticationException.class)
    public void testEmptyConfigLogin() throws AuthenticationException {
        Subject subject = new Subject(false,
                Collections.<Principal>emptySet(),
                Collections.emptySet(),
                Collections.emptySet());

        GPlazma instance = new GPlazma(newLoadStrategy(EMPTY_CONFIG),EMPTY_PROPERTIES);
        LoginReply result = instance.login(subject);

        Assert.assertNotNull(result);
    }

    /**
     *
     * Test of login method, of class GPlazma.
     * This test should succeed as we configure the four plugins in
     * configuration that will fill in minimally required plincipals and
     * attributes
     * <br>
     * First plugin is authentication type FindPrincipalsInCredentialsPlugin,
     * which should check both public and private credentials and find all
     * objects of type Principal.
     * <br>
     * Second plugin is mapping type IdentityMappingPlugin (self explanatory)
     * <br>
     * Third plugin is account type  which will check that specified uid Principal
     *  is present, otherwise it fails
     * It also records found uids in the static set, which we can check to verify
     * that is was called
     * <br>
     * And the last plugin to be called is AddHomeRootSessionPlugin that
     * adds home and root attributes only if certain user principal is present
     *
     */
    @Test
    public void testLogin() throws AuthenticationException {
        //configuration
        Configuration config = newConfiguration (
            AUTH_CONFIG_ITEM,
            MAPPING_CONFIG_ITEM,
            ACCOUNT_CONFIG_ITEM,
            SESSION_CONFIG_ITEM);

        runLoginAssertions(config);
   }

    /**
     * Configuration is same as above, but we add optional plugins of each type that
     * fail
     * @throws AuthenticationException
     */
    @Test
    public void testLoginWithOptionalFailed() throws AuthenticationException {
        //configuration
        Configuration config = newConfiguration (
            AUTH_CONFIG_ITEM,
            FAIL_OPTIONAL_AUTH_CONFIG_ITEM,
            MAPPING_CONFIG_ITEM,
            FAIL_OPTIONAL_MAPPING_CONFIG_ITEM,
            ACCOUNT_CONFIG_ITEM,
            FAIL_OPTIONAL_ACCOUNT_CONFIG_ITEM,
            SESSION_CONFIG_ITEM,
            FAIL_OPTIONAL_SESSION_CONFIG_ITEM);

        runLoginAssertions(config);
   }

    /**
     * Configuration is same as in testLogin, but we add required failing authentication plugin
     * fail
     * @throws AuthenticationException
     */
    @Test (expected=AuthenticationException.class)
    public void testLoginWithRequiredAuthenticationFailed() throws AuthenticationException {
        //configuration
        Configuration config = newConfiguration (
            AUTH_CONFIG_ITEM,
            FAIL_AUTH_CONFIG_ITEM,
            MAPPING_CONFIG_ITEM,
            ACCOUNT_CONFIG_ITEM,
            SESSION_CONFIG_ITEM);

        assertFalse(CheckUIDAccountPlugin.isCalled());

        // do the work here
        LoginReply result = new GPlazma(newLoadStrategy(config), EMPTY_PROPERTIES).login(_inputSubject);

        Assert.assertNotNull(result);
   }

    /**
     * Configuration is same as in testLogin, but we add required failing authentication plugin
     * fail
     * @throws AuthenticationException
     */
    @Test (expected=AuthenticationException.class)
    public void testLoginWithRequiredMappingFailed() throws AuthenticationException {
        //configuration
        Configuration config = newConfiguration (
            AUTH_CONFIG_ITEM,
            MAPPING_CONFIG_ITEM,
            FAIL_MAPPING_CONFIG_ITEM,
            ACCOUNT_CONFIG_ITEM,
            SESSION_CONFIG_ITEM);

        assertFalse(CheckUIDAccountPlugin.isCalled());

        // do the work here
        LoginReply result = new GPlazma(newLoadStrategy(config), EMPTY_PROPERTIES).login(_inputSubject);

        Assert.assertNotNull(result);
   }
    /**
     * Configuration is same as in testLogin, but we add required failing authentication plugin
     * fail
     * @throws AuthenticationException
     */
    @Test (expected=AuthenticationException.class)
    public void testLoginWithRequiredAccountFailed() throws AuthenticationException {
        //configuration
        Configuration config = newConfiguration (
            AUTH_CONFIG_ITEM,
            MAPPING_CONFIG_ITEM,
            ACCOUNT_CONFIG_ITEM,
            FAIL_ACCOUNT_CONFIG_ITEM,
            SESSION_CONFIG_ITEM);

        assertFalse(CheckUIDAccountPlugin.isCalled());

        // do the work here
        LoginReply result = new GPlazma(newLoadStrategy(config), EMPTY_PROPERTIES).login(_inputSubject);

        Assert.assertNotNull(result);
   }
    /**
     * Configuration is same as in testLogin, but we add required failing authentication plugin
     * fail
     * @throws AuthenticationException
     */
    @Test (expected=AuthenticationException.class)
    public void testLoginWithRequiredSessionFailed() throws AuthenticationException {
        //configuration
        Configuration config  = newConfiguration (
            AUTH_CONFIG_ITEM,
            MAPPING_CONFIG_ITEM,
            ACCOUNT_CONFIG_ITEM,
            SESSION_CONFIG_ITEM,
            FAIL_SESSION_CONFIG_ITEM);

        assertFalse(CheckUIDAccountPlugin.isCalled());

        // do the work here
        LoginReply result = new GPlazma(newLoadStrategy(config), EMPTY_PROPERTIES).login(_inputSubject);

        Assert.assertNotNull(result);
   }

    /**
     * Configuration is same as in testLogin, but without session
     * should fail in the check of presence of the home and root attributes
     * @throws AuthenticationException
     */
    @Test (expected=AuthenticationException.class)
    public void testLoginWithoutSession() throws AuthenticationException {
        //configuration
        Configuration config  = newConfiguration (
            AUTH_CONFIG_ITEM,
            MAPPING_CONFIG_ITEM,
            ACCOUNT_CONFIG_ITEM);

        assertFalse(CheckUIDAccountPlugin.isCalled());

        // do the work here
        LoginReply result = new GPlazma(newLoadStrategy(config), EMPTY_PROPERTIES).login(_inputSubject);

        Assert.assertNotNull(result);
   }

    /**
     * Configuration is same as in testLogin, but without mapping.
     * @throws AuthenticationException
     */
    @Test
    public void testLoginWithoutMapping() throws AuthenticationException {
        Configuration config = newConfiguration(
                AUTH_CONFIG_ITEM,
                ACCOUNT_CONFIG_ITEM,
                SESSION_CONFIG_ITEM);
        runLoginAssertions(config);
   }
    /**
     * Configuration is same as in testLogin, but without authentication
     * should fail in the check of presence identified principlas
     * @throws AuthenticationException
     */
    @Test (expected=AuthenticationException.class)
    public void testLoginWithoutAuthentication() throws AuthenticationException {
        //configuration
        Configuration config  = newConfiguration (
            MAPPING_CONFIG_ITEM,
            ACCOUNT_CONFIG_ITEM,
            SESSION_CONFIG_ITEM);

        assertFalse(CheckUIDAccountPlugin.isCalled());

        // do the work here
        LoginReply result = new GPlazma(newLoadStrategy(config), EMPTY_PROPERTIES).login(_inputSubject);

        Assert.assertNotNull(result);
   }

    private static Configuration newConfiguration(ConfigurationItem ... items) {
        return new Configuration(
                Arrays.asList(items));
    }

    private static Subject newTestSubject() {
                //input subject
        Set<Object> publicCredentials = new HashSet<>();
        publicCredentials.add(new UserNamePrincipal(USER_NAME));
        publicCredentials.add(new UidPrincipal(ROOT_UID));

        Set<Object> privateCredentials = new HashSet<>();
        privateCredentials.add(new GidPrincipal(ROOT_GID,true));

        return new Subject(
                false,
                Collections.<Principal>emptySet(),
                publicCredentials,
                privateCredentials);
    }

    private static ConfigurationLoadingStrategy newLoadStrategy(Configuration configuration) {
        return new StaticContentConfigurationLoadingStrategy(configuration);
    }

    private void runLoginAssertions(Configuration config) throws AuthenticationException {
        assertFalse(CheckUIDAccountPlugin.isCalled());

        // do the work here
        LoginReply result = new GPlazma(newLoadStrategy(config), EMPTY_PROPERTIES).login(_inputSubject);

        // check the results
        assertTrue(CheckUIDAccountPlugin.isCalled());

        Set<Principal> expectedPrincipals = new HashSet<>();
        expectedPrincipals.add(new UserNamePrincipal(USER_NAME));
        expectedPrincipals.add(new UidPrincipal(ROOT_UID));
        expectedPrincipals.add(new GidPrincipal(ROOT_GID,true));

        Set<Principal> resultPrincipals = result.getSubject().getPrincipals();
        assertEquals(resultPrincipals, expectedPrincipals);

        Set<Object> expectedAttributes = new HashSet<>();
        expectedAttributes.add(new HomeDirectory(HOME_PATH_ARG_VALUE));
        expectedAttributes.add(new RootDirectory(ROOT_PATH_ARG_VALUE));
        expectedAttributes.add(Restrictions.readOnly());

        Set<Object> resultAttributes = result.getSessionAttributes();

        assertEquals(expectedAttributes, resultAttributes);
    }
}
