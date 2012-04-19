package org.dcache.gplazma.plugins;

import static com.google.common.collect.ImmutableSet.of;
import com.google.common.io.Resources;
import java.util.Properties;
import java.util.Set;

import org.dcache.gplazma.AuthenticationException;
import org.globus.gsi.jaas.GlobusPrincipal;
import org.junit.Test;

/**
 * These tests test the gPlazma plugin for Argus. They rely on the following
 * configuration:
 *
 * Argus PEP Endpoint at http://swords.desy.de:8154/authz
 * with the following configuration:
 *
 * --- pepd.ini
 * [SERVICE]
 * entityId = http://swords.desy.de/authz
 * hostname = swords.desy.de
 * port = 8154
 * adminPort = 8155
 * adminPassword =
 *
 * # PIPs to apply on incoming request
 * pips = OPENSSLSUBJECT_PIP
 *
 * [PDP]
 * pdps = http://localhost:8152/authz
 *
 * [SECURITY]
 * servicePrivateKey = /etc/grid-security/hostkey.pem
 * serviceCertificate = /etc/grid-security/hostcert.pem
 * trustInfoDir = /etc/grid-security/certificates
 * enableSSL = false
 * requireClientCertAuthentication = true
 *
 * [OPENSSLSUBJECT_PIP]
 * parserClass = org.glite.authz.pep.pip.provider.OpenSSLSubjectPIPIniConfigurationParser
 * opensslSubjectAttributeIDs = urn:oasis:names:tc:xacml:1.0:subject:subject-id
 * opensslSubjectAttributeDatatypes = http://www.w3.org/2001/XMLSchema#string
 * ---
 *
 * and the policies created by running these 3 commands on the PAP server:
 * # pap-admin ap --action access --resource dcache permit subject="/C=EX/O=Example Org/OU=SOMEUNIT/CN=Some One"
 * # pap-admin ap --action access --resource dcache permit subject="/C=BG/O=Bogus Org/OU=BOGUS/CN=La Bogus"
 * # pap-admin ban subject "/C=BG/O=Bogus Org/OU=BOGUS/CN=La Bogus"
 *
 *
 * # pap-admin lp
 * should then print the following:
 * *****************************************************************************
 * default (local):
 *
 * resource ".*" {
 *
 *     action ".*" {
 *         rule deny { subject="CN=La Bogus,OU=BOGUS,O=Bogus Org,C=BG" }
 *     }
 * }
 *
 * resource "dcache" {
 *
 *     action "access" {
 *         rule permit { subject="CN=La Bogus,OU=BOGUS,O=Bogus Org,C=BG" }
 *         rule permit { subject="CN=Some One,OU=SOMEUNIT,O=Example Org,C=EX" }
 *     }
 * }
 *
 * *****************************************************************************
 *
 * and on the machine swords.desy.de with a running Argus system and the
 * corresponding certificates.
 *
 * @author karsten
 *
 */
public class GPlazmaArgusPluginITCase {

    private static final String PERMITTED_DN = "/C=EX/O=Example Org/OU=SOMEUNIT/CN=Some One";
    private static final String PERMITTED_BANNED_DN = "/C=BG/O=Bogus Org/OU=BOGUS/CN=La Bogus";
    private static final String UNKNOWN_DN = "/C=XY/O=Unknows Org/OU=UNKNOWN/CN=A Stranger";
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

    private static final Set PermittedPrincipal = of(new GlobusPrincipal(PERMITTED_DN));
    private static final Set BannedPrincipal = of(new GlobusPrincipal(PERMITTED_BANNED_DN));
    private static final Set PermittedAndBannedPrincipals = of(new GlobusPrincipal(PERMITTED_DN), new GlobusPrincipal(PERMITTED_BANNED_DN));
    private static final Set UnknownPrincipals = of(new GlobusPrincipal(UNKNOWN_DN));

    /**
     * Test successful authorisation with correct parameters
     * @throws AuthenticationException
     */
    @Test
    public void shouldSucceedForPermittedPrincipal()
            throws AuthenticationException {
        Properties givenConfiguration = new Properties();
        givenConfiguration.put(PEP_ENDPOINT, VALID_ENDPOINT);
        givenConfiguration.put(RESOURCE_ID, VALID_RESOURCE);
        givenConfiguration.put(ACTION_ID, VALID_ACTION);
        givenConfiguration.put(TRUST_MATERIAL, VALID_CERT_PATH);
        givenConfiguration.put(HOST_CERT, VALID_HOSTCERT);
        givenConfiguration.put(HOST_KEY, VALID_HOSTKEY);
        givenConfiguration.put(KEY_PASS, "");

        GPlazmaArgusPlugin plugin = new GPlazmaArgusPlugin(givenConfiguration);

        plugin.account(PermittedPrincipal);
    }

    /**
     * Test successful authorisation with correct parameters
     * @throws AuthenticationException
     */
    @Test
    public void shouldSucceedForMultipleEndpointsAndPermittedPrincipal()
            throws AuthenticationException {

        Properties givenConfiguration = new Properties();
        givenConfiguration.put(PEP_ENDPOINT, INVALID_ENDPOINT);
        givenConfiguration.put(PEP_ENDPOINT, VALID_ENDPOINT);
        givenConfiguration.put(RESOURCE_ID, VALID_RESOURCE);
        givenConfiguration.put(ACTION_ID, VALID_ACTION);
        givenConfiguration.put(TRUST_MATERIAL, VALID_CERT_PATH);
        givenConfiguration.put(HOST_CERT, VALID_HOSTCERT);
        givenConfiguration.put(HOST_KEY, VALID_HOSTKEY);
        givenConfiguration.put(KEY_PASS, "");

        GPlazmaArgusPlugin plugin = new GPlazmaArgusPlugin(givenConfiguration);

        plugin.account(PermittedPrincipal);
    }

    /**
     * Authorisation success on unknown DN
     * (here the DN is in wrong order)
     * @throws AuthenticationException
     */
    @Test
    public void shouldSucceedForUnknownPrincipal()
            throws AuthenticationException {
        Properties givenConfiguration = new Properties();
        givenConfiguration.put(PEP_ENDPOINT, VALID_ENDPOINT);
        givenConfiguration.put(RESOURCE_ID, VALID_RESOURCE);
        givenConfiguration.put(ACTION_ID, VALID_ACTION);
        givenConfiguration.put(TRUST_MATERIAL, VALID_CERT_PATH);
        givenConfiguration.put(HOST_CERT, VALID_HOSTCERT);
        givenConfiguration.put(HOST_KEY, VALID_HOSTKEY);
        givenConfiguration.put(KEY_PASS, "");

        GPlazmaArgusPlugin plugin = new GPlazmaArgusPlugin(givenConfiguration);

        plugin.account(UnknownPrincipals);
    }

    /**
     * Test result DENY authorisation with banned user
     * @throws AuthenticationException
     */
    @Test(expected=AuthenticationException.class)
    public void shouldFailForBannedPrincipal()
            throws AuthenticationException {
        Properties givenConfiguration = new Properties();
        givenConfiguration.put(PEP_ENDPOINT, VALID_ENDPOINT);
        givenConfiguration.put(RESOURCE_ID, VALID_RESOURCE);
        givenConfiguration.put(ACTION_ID, VALID_ACTION);
        givenConfiguration.put(TRUST_MATERIAL, VALID_CERT_PATH);
        givenConfiguration.put(HOST_CERT, VALID_HOSTCERT);
        givenConfiguration.put(HOST_KEY, VALID_HOSTKEY);
        givenConfiguration.put(KEY_PASS, "");

        GPlazmaArgusPlugin plugin = new GPlazmaArgusPlugin(givenConfiguration);

        plugin.account(BannedPrincipal);
    }

    /**
     * Test result DENY authorisation with banned user
     * @throws AuthenticationException
     */
    @Test(expected=AuthenticationException.class)
    public void shouldFailForPermittedAndBannedPrincipal()
            throws AuthenticationException {
        Properties givenConfiguration = new Properties();
        givenConfiguration.put(PEP_ENDPOINT, VALID_ENDPOINT);
        givenConfiguration.put(RESOURCE_ID, VALID_RESOURCE);
        givenConfiguration.put(ACTION_ID, VALID_ACTION);
        givenConfiguration.put(TRUST_MATERIAL, VALID_CERT_PATH);
        givenConfiguration.put(HOST_CERT, VALID_HOSTCERT);
        givenConfiguration.put(HOST_KEY, VALID_HOSTKEY);
        givenConfiguration.put(KEY_PASS, "");

        GPlazmaArgusPlugin plugin = new GPlazmaArgusPlugin(givenConfiguration);

        plugin.account(PermittedAndBannedPrincipals);
    }

    /**
     * Test result DENY with invalid/unreachable PEP
     * @throws AuthenticationException
     */
    @Test(expected=AuthenticationException.class)
    public void shouldFailForNonExistentPepEndpoint()
            throws AuthenticationException {
        Properties givenConfiguration = new Properties();
        givenConfiguration.put(PEP_ENDPOINT, INVALID_ENDPOINT);
        givenConfiguration.put(RESOURCE_ID, VALID_RESOURCE);
        givenConfiguration.put(ACTION_ID, VALID_ACTION);
        givenConfiguration.put(TRUST_MATERIAL, VALID_CERT_PATH);
        givenConfiguration.put(HOST_CERT, VALID_HOSTCERT);
        givenConfiguration.put(HOST_KEY, VALID_HOSTKEY);
        givenConfiguration.put(KEY_PASS, "");

        GPlazmaArgusPlugin plugin = new GPlazmaArgusPlugin(givenConfiguration);

        plugin.account(BannedPrincipal);
    }
}
