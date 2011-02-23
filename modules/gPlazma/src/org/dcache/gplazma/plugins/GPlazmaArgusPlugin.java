package org.dcache.gplazma.plugins;

import java.security.Principal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.SessionID;
import org.glite.authz.common.model.Request;
import org.glite.authz.common.model.Response;
import org.glite.authz.common.model.Result;
import org.glite.authz.pep.client.PEPClient;
import org.glite.authz.pep.client.PEPClientException;
import org.glite.authz.pep.client.config.PEPClientConfiguration;
import org.glite.authz.pep.client.config.PEPClientConfigurationException;
import org.glite.authz.pep.profile.GridWNAuthorizationProfile;
import org.globus.gsi.jaas.GlobusPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Collections2;

/**
 * Implementation of an Argus Authorisation plugin for gPlazma2
 *
 * The plugin supports blacklisting of subjects identified by X509 Certificates
 * and/or their DN.
 *
 * @author karsten
 *
 */
public class GPlazmaArgusPlugin implements GPlazmaAccountPlugin {

    private static final Logger _log = LoggerFactory.getLogger(GPlazmaArgusPlugin.class);

    private static final String CREATING_ARGUS_PLUGIN_WITH_PARAMETERS_params = "Creating Argus Plugin with parameters: %s";
    private static final String COULD_NOT_CREATE_PEP_CLIENT_exception = "Could not create PEP Client";
    private static final String INVALID_CONFIGURATION_FOR_ARGUS_PLUGIN = "Invalid configuration for ARGUS plugin.";
    private static final String G_PLAZMA_ARGUS_PLUGIN_ARGS_MUST_NOT_BE_NULL = "GPlazmaArgusPlugin: args must not be null.";
    private static final String INITIALISING_PEP_CLIENT_CONFIGURATION = "Initialising PEPClientConfiguration";
    private static final String CONFIGURATION_resourceid_actionid = "Configuration: [%s]; [%s]";
    private static final String AUTHORISING_SUBJECT_dn_FOR_SESSION_session = "Authorising subject %s for session %s.";
    private static final String CREATED_REQUEST_request = "Created request: %s";
    private static final String RECEIVED_RESPONSE_response = "Received response: %s";
    private static final String USER_dn_IS_BLACKLISTED = "User '%s' is blacklisted.";
    private static final String BLACKLIST_CHECK_FAILED_FOR_USER_WITH_DN_dn = "Blacklist check failed for user with dn '%s'.";
    private static final String INDETERMINATE_RESULT_FOR_USER_dn = "Indeterminate result for user '%s'";
    private static final String BLACKLIST_CHECK_FOR_USER_dn_FAILED_DUE_TO_EXCEPTION_IN_PLUGIN = "Blacklist check for user '%s' failed due to exception in plugin.";
    private static final String DECISION_CODE_code = "Decision code: ";

    private static final String KEY_PASS = "KeyPass";
    private static final String HOST_KEY = "HostKey";
    private static final String HOST_CERT = "HostCert";
    private static final String TRUST_MATERIAL = "TrustMaterial";
    private static final String ACTION_ID = "ActionID";
    private static final String RESOURCE_ID = "ResourceID";
    private static final String PEP_ENDPOINT = "PEPEndpoint";

    private static final Set<String> KEYNAMES = new HashSet<String>(Arrays.asList( new String[] { PEP_ENDPOINT, RESOURCE_ID, ACTION_ID, TRUST_MATERIAL, HOST_CERT, HOST_KEY, KEY_PASS } ));


    private final PEPClient _pepClient;

    private String _resourceId;
    private String _actionId;

    /**
     * Constructor
     * @param args a set of key value pairs containing the plugins configuration.
     */
    public GPlazmaArgusPlugin(String[] args) {

        _log.debug(CREATING_ARGUS_PLUGIN_WITH_PARAMETERS_params, args);

        try {
            PEPClientConfiguration pepConfiguration = initPepConfiguration(args);
            _pepClient = new PEPClient(pepConfiguration);
        } catch (PEPClientException e) {
            _log.error(COULD_NOT_CREATE_PEP_CLIENT_exception, e);
            throw new IllegalArgumentException(INVALID_CONFIGURATION_FOR_ARGUS_PLUGIN, e);
        }
    }

    /**
     * Constructor
     * @param pepClient client to be used by plugin
     */
    public GPlazmaArgusPlugin(PEPClient pepClient) {
        _pepClient = pepClient;
    }

    /**
     * This method initialises the instance's configuration, by parsing
     * the parameters given in args. Required key/value is PEPEndpoint.
     * @param args array of key value pairs containing the plugins configuration ( key1, value1, key2, value2, ...)
     * @throws PEPClientConfigurationException
     */
    private PEPClientConfiguration initPepConfiguration(String args[]) throws PEPClientConfigurationException {
        if (args==null) throw new IllegalArgumentException(G_PLAZMA_ARGUS_PLUGIN_ARGS_MUST_NOT_BE_NULL);

        PEPClientConfiguration pepConfig = new PEPClientConfiguration();

        _log.debug(INITIALISING_PEP_CLIENT_CONFIGURATION);

        Map<String, String> kvmap = ArgumentMapFactory.createFromAllKeyValuePairs(KEYNAMES, args);
        if (kvmap.get(PEP_ENDPOINT) != null) pepConfig.addPEPDaemonEndpoint(kvmap.get(PEP_ENDPOINT));
        _resourceId = kvmap.get(RESOURCE_ID) != null? _resourceId = kvmap.get(RESOURCE_ID) : "DUMMY-RESOURCE";
        _actionId = kvmap.get(ACTION_ID) != null? _actionId = kvmap.get(ACTION_ID) : "DUMMY-ACTION";
        String trustMaterial = com.google.common.base.Strings.nullToEmpty(kvmap.get(TRUST_MATERIAL));
        String hostCert = com.google.common.base.Strings.nullToEmpty(kvmap.get(HOST_CERT));
        String hostKey = com.google.common.base.Strings.nullToEmpty(kvmap.get(HOST_KEY));
        String keyPass = com.google.common.base.Strings.nullToEmpty(kvmap.get(KEY_PASS));

        pepConfig.setTrustMaterial(trustMaterial);
        pepConfig.setKeyMaterial(hostCert, hostKey, keyPass);

        _log.debug(CONFIGURATION_resourceid_actionid, _resourceId, _actionId);

        return pepConfig;
    }

    @Override
    public void account(SessionID sID, Set<Principal> authorizedPrincipals)
            throws AuthenticationException {

        int decision = Result.DECISION_NOT_APPLICABLE;

        String dn = null;
        try {
            Collection<Principal> globusPrincipals = Collections.emptySet();
            if (authorizedPrincipals != null)
                globusPrincipals = Collections2.filter(authorizedPrincipals, new ClassTypePredicate<Principal>(GlobusPrincipal.class));

            for (Principal principal : globusPrincipals) {
                dn = principal.getName();

                _log.info(AUTHORISING_SUBJECT_dn_FOR_SESSION_session, dn, sID);
                Request request = ArgusPepRequestFactory.create(dn, _resourceId, _actionId, GridWNAuthorizationProfile.getInstance());
                _log.debug(CREATED_REQUEST_request, request);
                Response response = _pepClient.authorize(request);
                _log.debug(RECEIVED_RESPONSE_response, response);

                for (Result result : response.getResults()) {
                    decision = result.getDecision();
                    switch (decision) {
                    case Result.DECISION_DENY:
                        throw new AuthenticationException(String.format(USER_dn_IS_BLACKLISTED, dn));
                    case Result.DECISION_INDETERMINATE:
                        throw new AuthenticationException(String.format(INDETERMINATE_RESULT_FOR_USER_dn, dn));
                    }
                }
            }
        } catch (PEPClientException e) {
            decision = Result.DECISION_DENY;
            _log.warn(BLACKLIST_CHECK_FOR_USER_dn_FAILED_DUE_TO_EXCEPTION_IN_PLUGIN, dn ,e);
            throw new AuthenticationException(String.format(BLACKLIST_CHECK_FAILED_FOR_USER_WITH_DN_dn, dn), e);

        } finally {
            _log.info(DECISION_CODE_code, decision);
        }
    }
}
