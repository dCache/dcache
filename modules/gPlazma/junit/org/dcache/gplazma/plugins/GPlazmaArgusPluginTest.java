package org.dcache.gplazma.plugins;

import java.security.Principal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;

import org.dcache.gplazma.AuthenticationException;
import org.glite.authz.common.model.Request;
import org.glite.authz.common.model.Response;
import org.glite.authz.common.model.Result;
import org.glite.authz.pep.client.PEPClient;
import org.glite.authz.pep.client.PEPClientException;
import org.glite.authz.pep.client.config.PEPClientConfiguration;
import org.globus.gsi.jaas.GlobusPrincipal;
import org.junit.Ignore;
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
 *     rule deny { subject="CN=Bill Gates,OU=DESY,O=GermanGrid,C=DE" }
 *   }
 * }
 *
 * resource "http://karsten-vm01.desy.de/dcache" {
 *   action "http://glite.org/xacml/action/execute" {
 *     rule permit { subject="CN=Bill Gates,OU=DESY,O=GermanGrid,C=DE" }
 *     rule permit { subject="CN=Karsten Schwank,OU=DESY,O=GermanGrid,C=DE" }
 *   }
 * }
 * ****************************************************************************
 *
 * and on the machine karsten-vm01.desy.de with a running Argus system and the
 * corresponding certificates. For this reasons the tests are deactivated for
 * standard test runs.
 *
 * The remaining tests work with a mock PEP Client and are environment independent.
 *
 * @author karsten
 *
 */
public class GPlazmaArgusPluginTest {

    private static final String GLOBUS_DN = "CN=Karsten Schwank/O=GermanGrid/OU=DESY/C=DE";
    private static final String VALID_DN = "CN=Karsten Schwank,OU=DESY,O=GermanGrid,C=DE";
    private static final String VALID_ENDPOINT = "https://karsten-vm01.desy.de:8154/authz";
    private static final String PEP_ENDPOINT = "PEPEndpoint";
    private static final String INVALID_ENDPOINT = "https://karsten-vm01.desy.de:666/authz";
    private static final String RESOURCE_ID = "ResourceID";
    private static final String VALID_RESOURCE = "http://karsten-vm01.desy.de/dcache";
    private static final String ACTION_ID = "ActionID";
    private static final String VALID_ACTION = "http://glite.org/xacml/action/execute";
    private static final String TRUST_MATERIAL = "TrustMaterial";
    private static final String VALID_CERT_PATH = "/etc/grid-security/certificates";
    private static final String HOST_CERT = "HostCert";
    private static final String VALID_HOSTCERT = "/etc/grid-security/hostcert.pem";
    private static final String HOST_KEY = "HostKey";
    private static final String VALID_HOSTKEY = "/etc/grid-security/hostkey.pem";
    private static final String KEY_PASS = "KeyPass";
    private static final String BANNED_DN = "CN=Bill Gates,OU=DESY,O=GermanGrid,C=DE";

    private static final HashSet<Principal> ValidPrincipals = new HashSet<Principal>(Arrays.asList(new GlobusPrincipal(VALID_DN)));
    private static final HashSet<Principal> BannedPrincipals = new HashSet<Principal>(Arrays.asList(new GlobusPrincipal(BANNED_DN)));
    private static final HashSet<Principal> MixedPrincipals = new HashSet<Principal>(Arrays.asList(new GlobusPrincipal(VALID_DN), new GlobusPrincipal(BANNED_DN)));
    private static final HashSet<Principal> GlobusPrincipals = new HashSet<Principal>(Arrays.asList(new GlobusPrincipal(GLOBUS_DN)));

    @Test(expected = NullPointerException.class)
    public void testGPlazmaArgusPluginWithArgsNull() {
        new GPlazmaArgusPlugin((Properties)null);
    }

    /**
     * Test successful authorisation with correct parameters
     * @throws AuthenticationException
     */
    @Test
    @Ignore("Depends on running, preconfigured Argus System.")
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
        plugin.account(null, ValidPrincipals);
    }

    /**
     * Test successful authorisation with correct parameters
     * @throws AuthenticationException
     */
    @Test
    @Ignore("Fails because ArgumentMap currently cannot handle multiple entries with the same key.")
    public void testGPlazmaArgusPluginWithValidArgsAndMultipleEndpoints() throws AuthenticationException {

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

        plugin.account(null, ValidPrincipals);
    }

    /**
     * Authorisation success on unknown DN
     * (here the DN is in wrong order)
     * @throws AuthenticationException
     */
    @Test
    @Ignore("Depends on running, preconfigured Argus System.")
    public void testGPlazmaArgusPluginWithUnknownDN() throws AuthenticationException {
        Properties properties = new Properties();
        properties.put(PEP_ENDPOINT, VALID_ENDPOINT);
        properties.put(RESOURCE_ID, VALID_RESOURCE);
        properties.put(ACTION_ID, VALID_ACTION);
        properties.put(TRUST_MATERIAL, VALID_CERT_PATH);
        properties.put(HOST_CERT, VALID_HOSTCERT);
        properties.put(HOST_KEY, VALID_HOSTKEY);
        properties.put(KEY_PASS, "");

        GPlazmaArgusPlugin plugin = new GPlazmaArgusPlugin(properties);

        plugin.account(null, GlobusPrincipals);
    }

    /**
     * Test result DENY authorisation with banned user
     * @throws AuthenticationException
     */
    @Test(expected=AuthenticationException.class)
    @Ignore("Depends on running, preconfigured Argus System.")
    public void testGPlazmaArgusPluginWithValidArgsBannedUser() throws AuthenticationException {
        Properties properties = new Properties();
        properties.put(PEP_ENDPOINT, VALID_ENDPOINT);
        properties.put(RESOURCE_ID, VALID_RESOURCE);
        properties.put(ACTION_ID, VALID_ACTION);
        properties.put(TRUST_MATERIAL, VALID_CERT_PATH);
        properties.put(HOST_CERT, VALID_HOSTCERT);
        properties.put(HOST_KEY, VALID_HOSTKEY);
        properties.put(KEY_PASS, "");

        GPlazmaArgusPlugin plugin = new GPlazmaArgusPlugin(properties);

        plugin.account(null, BannedPrincipals);
    }

    /**
     * Test result DENY authorisation with banned user
     * @throws AuthenticationException
     */
    @Test(expected=AuthenticationException.class)
    @Ignore("Depends on running, preconfigured Argus System.")
    public void testGPlazmaArgusPluginWithValidArgsMixedUser() throws AuthenticationException {
        Properties properties = new Properties();
        properties.put(PEP_ENDPOINT, VALID_ENDPOINT);
        properties.put(RESOURCE_ID, VALID_RESOURCE);
        properties.put(ACTION_ID, VALID_ACTION);
        properties.put(TRUST_MATERIAL, VALID_CERT_PATH);
        properties.put(HOST_CERT, VALID_HOSTCERT);
        properties.put(HOST_KEY, VALID_HOSTKEY);
        properties.put(KEY_PASS, "");

        GPlazmaArgusPlugin plugin = new GPlazmaArgusPlugin(properties);

        plugin.account(null, MixedPrincipals);
    }

    /**
     * Test result DENY with invalid/unreachable PEP
     * @throws AuthenticationException
     */
    @Test(expected=AuthenticationException.class)
    @Ignore("Depends on running, preconfigured Argus System.")
    public void testGPlazmaArgusPluginWithNonExistendPEPEndpoint() throws AuthenticationException {
        Properties properties = new Properties();
        properties.put(PEP_ENDPOINT, INVALID_ENDPOINT);
        properties.put(RESOURCE_ID, VALID_RESOURCE);
        properties.put(ACTION_ID, VALID_ACTION);
        properties.put(TRUST_MATERIAL, VALID_CERT_PATH);
        properties.put(HOST_CERT, VALID_HOSTCERT);
        properties.put(HOST_KEY, VALID_HOSTKEY);
        properties.put(KEY_PASS, "");

        GPlazmaArgusPlugin plugin = new GPlazmaArgusPlugin(properties);

        plugin.account(null, BannedPrincipals);
    }

    @Test
    public void testResponsePermit() throws PEPClientException, AuthenticationException {

        GPlazmaArgusPlugin plugin = new GPlazmaArgusPlugin(MockPEPClient.create(null, Result.DECISION_PERMIT));
        plugin.account(null, ValidPrincipals);
    }

    @Test(expected=AuthenticationException.class)
    public void testResponseDeny() throws PEPClientException, AuthenticationException {

        GPlazmaArgusPlugin plugin = new GPlazmaArgusPlugin(MockPEPClient.create(null, Result.DECISION_DENY));
        plugin.account(null, ValidPrincipals);
    }

    @Test(expected=AuthenticationException.class)
    public void testResponseIndeterminate() throws PEPClientException, AuthenticationException {

        GPlazmaArgusPlugin plugin = new GPlazmaArgusPlugin(MockPEPClient.create(null, Result.DECISION_INDETERMINATE));
        plugin.account(null, ValidPrincipals);
    }

    @Test
    public void testResponseNotApplicable() throws PEPClientException, AuthenticationException {

        GPlazmaArgusPlugin plugin = new GPlazmaArgusPlugin(MockPEPClient.create(null, Result.DECISION_NOT_APPLICABLE));
        plugin.account(null, ValidPrincipals);
    }

    @Test(expected=AuthenticationException.class)
    public void testMixedResponsePermitDeny() throws PEPClientException, AuthenticationException {

        GPlazmaArgusPlugin plugin = new GPlazmaArgusPlugin(MockPEPClient.create(null, Result.DECISION_PERMIT, Result.DECISION_DENY));
        plugin.account(null, ValidPrincipals);
    }

    @Test(expected=AuthenticationException.class)
    public void testMixedResponsePermitNotApplicableIndeterminatePermit() throws PEPClientException, AuthenticationException {

        GPlazmaArgusPlugin plugin = new GPlazmaArgusPlugin(MockPEPClient.create(null, Result.DECISION_PERMIT, Result.DECISION_NOT_APPLICABLE, Result.DECISION_INDETERMINATE, Result.DECISION_PERMIT));
        plugin.account(null, ValidPrincipals);
    }

    @Test(expected=AuthenticationException.class)
    public void testException() throws PEPClientException, AuthenticationException {

        new GPlazmaArgusPlugin(MockPEPClient.create(new PEPClientException(), Result.DECISION_PERMIT)).account(null, ValidPrincipals);
    }

    /**
     * Mock PEPClient to test arbitrary responses
     * @author karsten
     *
     */
    private static class MockPEPClient extends PEPClient {

        private final Response _response;
        private final PEPClientException _exception;

        public static MockPEPClient create(PEPClientException exception, int... decisions) throws PEPClientException {
            return new MockPEPClient(createConfig(), createResponse(decisions), exception);
        }

        private MockPEPClient(PEPClientConfiguration config, Response response, PEPClientException exception)
                throws PEPClientException {
            super(config);
            _exception = exception;
            _response = response;
        }

        @Override
        public Response authorize(Request request) throws PEPClientException {
            if (_exception != null)
                throw _exception;
            return _response;
        }

        private static PEPClientConfiguration createConfig() {
            PEPClientConfiguration dummyConfig = new PEPClientConfiguration();
            dummyConfig.addPEPDaemonEndpoint("");
            return dummyConfig;
        }

        private static Response createResponse(int... decisions) {
            Response response = new Response();
            for (int i : decisions) {
                Result result = new Result();
                result.setDecision(i);
                response.getResults().add(result);
            }
            return response;
        }

    }


}
