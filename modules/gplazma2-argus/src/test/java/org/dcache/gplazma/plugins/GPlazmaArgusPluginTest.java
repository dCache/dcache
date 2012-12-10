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
import org.globus.gsi.gssapi.jaas.GlobusPrincipal;
import org.junit.Test;

/**
 * @author karsten
 *
 */
public class GPlazmaArgusPluginTest {

    private static final String GLOBUS_DN = "CN=Some One/O=Example Org/OU=SOMEUNIT/C=EX";
    private static final String VALID_DN = "CN=Some One,OU=SOMEUNIT,O=Example Org,C=EX";
    private static final String BANNED_DN = "CN=La Bogus,OU=BOGUS,O=Bugus Org,C=BG";

    private static final HashSet<Principal> ValidPrincipals = new HashSet<Principal>(Arrays.asList(new GlobusPrincipal(VALID_DN)));

    @Test(expected = NullPointerException.class)
    public void testGPlazmaArgusPluginWithArgsNull() {
        new GPlazmaArgusPlugin((Properties)null);
    }

    @Test
    public void testResponsePermit() throws PEPClientException, AuthenticationException {

        GPlazmaArgusPlugin plugin = new GPlazmaArgusPlugin(MockPEPClient.create(null, Result.DECISION_PERMIT));
        plugin.account(ValidPrincipals);
    }

    @Test(expected=AuthenticationException.class)
    public void testResponseDeny() throws PEPClientException, AuthenticationException {

        GPlazmaArgusPlugin plugin = new GPlazmaArgusPlugin(MockPEPClient.create(null, Result.DECISION_DENY));
        plugin.account(ValidPrincipals);
    }

    @Test(expected=AuthenticationException.class)
    public void testResponseIndeterminate() throws PEPClientException, AuthenticationException {

        GPlazmaArgusPlugin plugin = new GPlazmaArgusPlugin(MockPEPClient.create(null, Result.DECISION_INDETERMINATE));
        plugin.account(ValidPrincipals);
    }

    @Test
    public void testResponseNotApplicable() throws PEPClientException, AuthenticationException {

        GPlazmaArgusPlugin plugin = new GPlazmaArgusPlugin(MockPEPClient.create(null, Result.DECISION_NOT_APPLICABLE));
        plugin.account(ValidPrincipals);
    }

    @Test(expected=AuthenticationException.class)
    public void testMixedResponsePermitDeny() throws PEPClientException, AuthenticationException {

        GPlazmaArgusPlugin plugin = new GPlazmaArgusPlugin(MockPEPClient.create(null, Result.DECISION_PERMIT, Result.DECISION_DENY));
        plugin.account(ValidPrincipals);
    }

    @Test(expected=AuthenticationException.class)
    public void testMixedResponsePermitNotApplicableIndeterminatePermit() throws PEPClientException, AuthenticationException {

        GPlazmaArgusPlugin plugin = new GPlazmaArgusPlugin(MockPEPClient.create(null, Result.DECISION_PERMIT, Result.DECISION_NOT_APPLICABLE, Result.DECISION_INDETERMINATE, Result.DECISION_PERMIT));
        plugin.account(ValidPrincipals);
    }

    @Test(expected=AuthenticationException.class)
    public void testException() throws PEPClientException, AuthenticationException {

        new GPlazmaArgusPlugin(MockPEPClient.create(new PEPClientException(), Result.DECISION_PERMIT)).account(ValidPrincipals);
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
            if (_exception != null) {
                throw _exception;
            }
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
