package org.dcache.gplazma.plugins;

import com.google.common.io.Resources;
import java.security.Principal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;

import org.dcache.gplazma.AuthenticationException;
import org.globus.gsi.jaas.GlobusPrincipal;
import org.junit.Test;

/**
 * These tests test the gPlazma plugin for Argus. Some of them rely on the following
 * policies:
 *
 * ****************************************************************************
 * default (local):
 *
 * resource ".*" {
 *   action ".*" {
 *     rule deny { subject="CN=La Bogus,OU=BOGUS,O=Bugus Org,C=BG" }
 *   }
 * }
 *
 * resource "http://example.org/dcache" {
 *   action "http://glite.org/xacml/action/execute" {
 *     rule permit { subject="CN=La Bogus,OU=BOGUS,O=Bugus Org,C=BG" }
 *     rule permit { subject="CN=Some One/O=Example Org/OU=SOMEUNIT/C=EX" }
 *   }
 * }
 * ****************************************************************************
 *
 * and on the machine swords.desy.de with a running Argus system and the
 * corresponding certificates.
 *
 * @author karsten
 *
 */
public class GPlazmaArgusPluginITCase {

    private static final String GLOBUS_DN = "CN=Some One/O=Example Org/OU=SOMEUNIT/C=EX";
    private static final String VALID_DN = "CN=Some One,OU=SOMEUNIT,O=Example Org,C=EX";
    private static final String VALID_ENDPOINT = "http://swords.desy.de:8154/authz";
    private static final String PEP_ENDPOINT = "gplazma.argus.endpoint";
    private static final String INVALID_ENDPOINT = "https://swords.desy.de:666/authz";
    private static final String RESOURCE_ID = "gplazma.argus.resource";
    private static final String VALID_RESOURCE = "http://example.org/dcache";
    private static final String ACTION_ID = "gplazma.argus.action";
    private static final String VALID_ACTION = "http://glite.org/xacml/action/execute";
    private static final String TRUST_MATERIAL = "gplazma.argus.ca";
    private static final String VALID_CERT_PATH = "/etc/grid-security/certificates";
    private static final String HOST_CERT = "gplazma.argus.hostcert";
    private static final String VALID_HOSTCERT = Resources.getResource("org/dcache/gplazma/plugins/test.crt").getFile();
    private static final String HOST_KEY = "gplazma.argus.hostkey";
    private static final String VALID_HOSTKEY = Resources.getResource("org/dcache/gplazma/plugins/test.key").getFile();
    private static final String KEY_PASS = "gplazma.argus.hostkey.password";
    private static final String BANNED_DN = "CN=La Bogus,OU=BOGUS,O=Bugus Org,C=BG";

    private static final HashSet<Principal> ValidPrincipals = new HashSet<Principal>(Arrays.asList(new GlobusPrincipal(VALID_DN)));
    private static final HashSet<Principal> BannedPrincipals = new HashSet<Principal>(Arrays.asList(new GlobusPrincipal(BANNED_DN)));
    private static final HashSet<Principal> MixedPrincipals = new HashSet<Principal>(Arrays.asList(new GlobusPrincipal(VALID_DN), new GlobusPrincipal(BANNED_DN)));
    private static final HashSet<Principal> GlobusPrincipals = new HashSet<Principal>(Arrays.asList(new GlobusPrincipal(GLOBUS_DN)));

    /**
     * Test successful authorisation with correct parameters
     * @throws AuthenticationException
     */
    @Test
    public void testGPlazmaArgusPluginWithValidArgs()
            throws AuthenticationException {
        Properties properties = new Properties();
        properties.put(PEP_ENDPOINT, VALID_ENDPOINT);
        properties.put(RESOURCE_ID, VALID_RESOURCE);
        properties.put(ACTION_ID, VALID_ACTION);
        properties.put(TRUST_MATERIAL, VALID_CERT_PATH);
        properties.put(HOST_CERT, VALID_HOSTCERT);
        properties.put(HOST_KEY, VALID_HOSTKEY);
        properties.put(KEY_PASS, "");

        GPlazmaArgusPlugin plugin = new GPlazmaArgusPlugin(properties);

        plugin.account(ValidPrincipals);
    }

    /**
     * Test successful authorisation with correct parameters
     * @throws AuthenticationException
     */
    @Test
    public void testGPlazmaArgusPluginWithValidArgsAndMultipleEndpoints()
            throws AuthenticationException {

        Properties properties = new Properties();
        properties.put(PEP_ENDPOINT, INVALID_ENDPOINT);
        properties.put(PEP_ENDPOINT, VALID_ENDPOINT);
        properties.put(RESOURCE_ID, VALID_RESOURCE);
        properties.put(ACTION_ID, VALID_ACTION);
        properties.put(TRUST_MATERIAL, VALID_CERT_PATH);
        properties.put(HOST_CERT, VALID_HOSTCERT);
        properties.put(HOST_KEY, VALID_HOSTKEY);
        properties.put(KEY_PASS, "");

        GPlazmaArgusPlugin plugin = new GPlazmaArgusPlugin(properties);

        plugin.account(ValidPrincipals);
    }

    /**
     * Authorisation success on unknown DN
     * (here the DN is in wrong order)
     * @throws AuthenticationException
     */
    @Test
    public void testGPlazmaArgusPluginWithUnknownDN()
            throws AuthenticationException {
        Properties properties = new Properties();
        properties.put(PEP_ENDPOINT, VALID_ENDPOINT);
        properties.put(RESOURCE_ID, VALID_RESOURCE);
        properties.put(ACTION_ID, VALID_ACTION);
        properties.put(TRUST_MATERIAL, VALID_CERT_PATH);
        properties.put(HOST_CERT, VALID_HOSTCERT);
        properties.put(HOST_KEY, VALID_HOSTKEY);
        properties.put(KEY_PASS, "");

        GPlazmaArgusPlugin plugin = new GPlazmaArgusPlugin(properties);

        plugin.account(GlobusPrincipals);
    }

    /**
     * Test result DENY authorisation with banned user
     * @throws AuthenticationException
     */
    @Test(expected=AuthenticationException.class)
    public void testGPlazmaArgusPluginWithValidArgsBannedUser()
            throws AuthenticationException {
        Properties properties = new Properties();
        properties.put(PEP_ENDPOINT, VALID_ENDPOINT);
        properties.put(RESOURCE_ID, VALID_RESOURCE);
        properties.put(ACTION_ID, VALID_ACTION);
        properties.put(TRUST_MATERIAL, VALID_CERT_PATH);
        properties.put(HOST_CERT, VALID_HOSTCERT);
        properties.put(HOST_KEY, VALID_HOSTKEY);
        properties.put(KEY_PASS, "");

        GPlazmaArgusPlugin plugin = new GPlazmaArgusPlugin(properties);

        plugin.account(BannedPrincipals);
    }

    /**
     * Test result DENY authorisation with banned user
     * @throws AuthenticationException
     */
    @Test(expected=AuthenticationException.class)
    public void testGPlazmaArgusPluginWithValidArgsMixedUser()
            throws AuthenticationException {
        Properties properties = new Properties();
        properties.put(PEP_ENDPOINT, VALID_ENDPOINT);
        properties.put(RESOURCE_ID, VALID_RESOURCE);
        properties.put(ACTION_ID, VALID_ACTION);
        properties.put(TRUST_MATERIAL, VALID_CERT_PATH);
        properties.put(HOST_CERT, VALID_HOSTCERT);
        properties.put(HOST_KEY, VALID_HOSTKEY);
        properties.put(KEY_PASS, "");

        GPlazmaArgusPlugin plugin = new GPlazmaArgusPlugin(properties);

        plugin.account(MixedPrincipals);
    }

    /**
     * Test result DENY with invalid/unreachable PEP
     * @throws AuthenticationException
     */
    @Test(expected=AuthenticationException.class)
    public void testGPlazmaArgusPluginWithNonExistendPEPEndpoint()
            throws AuthenticationException {
        Properties properties = new Properties();
        properties.put(PEP_ENDPOINT, INVALID_ENDPOINT);
        properties.put(RESOURCE_ID, VALID_RESOURCE);
        properties.put(ACTION_ID, VALID_ACTION);
        properties.put(TRUST_MATERIAL, VALID_CERT_PATH);
        properties.put(HOST_CERT, VALID_HOSTCERT);
        properties.put(HOST_KEY, VALID_HOSTKEY);
        properties.put(KEY_PASS, "");

        GPlazmaArgusPlugin plugin = new GPlazmaArgusPlugin(properties);

        plugin.account(BannedPrincipals);
    }

}