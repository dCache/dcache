package org.dcache.gplazma.plugins;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.instanceOf;

import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

import org.dcache.gplazma.AuthenticationException;
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

    private static final String CREATING_ARGUS_PLUGIN_WITH_PARAMETERS_params = "Creating Argus Plugin with parameters: {}";
    private static final String COULD_NOT_CREATE_PEP_CLIENT_exception = "Could not create PEP Client";
    private static final String INVALID_CONFIGURATION_FOR_ARGUS_PLUGIN = "Invalid configuration for ARGUS plugin.";
    private static final String G_PLAZMA_ARGUS_PLUGIN_ARGS_MUST_NOT_BE_NULL = "GPlazmaArgusPlugin: args must not be null.";
    private static final String INITIALISING_PEP_CLIENT_CONFIGURATION = "Initialising PEPClientConfiguration";
    private static final String CONFIGURATION_resourceid_actionid = "Configuration: [{}]; [{}]";
    private static final String AUTHORISING_SUBJECT_dn = "Authorising subject {}.";
    private static final String CREATED_REQUEST_request = "Created request: {}";
    private static final String RECEIVED_RESPONSE_response = "Received response: {}";
    private static final String BLACKLIST_CHECK_FOR_USER_dn_FAILED_DUE_TO_EXCEPTION_IN_PLUGIN = "Blacklist check for user '{}' failed due to exception in plugin.";
    private static final String DECISION_CODE_code = "Decision code: ";

    private static final String KEY_PASS =
        "gplazma.argus.hostkey.password";
    private static final String HOST_KEY =
        "gplazma.argus.hostkey";
    private static final String HOST_CERT =
        "gplazma.argus.hostcert";
    private static final String TRUST_MATERIAL =
        "gplazma.argus.ca";
    private static final String ACTION_ID =
        "gplazma.argus.action";
    private static final String RESOURCE_ID =
        "gplazma.argus.resource";
    private static final String PEP_ENDPOINT =
        "gplazma.argus.endpoint";

    private final PEPClient _pepClient;

    private String _resourceId;
    private String _actionId;

    /**
     * Constructor
     * @param properties a set of key value pairs containing the plugins configuration.
     */
    public GPlazmaArgusPlugin(Properties properties) {

        _log.debug(CREATING_ARGUS_PLUGIN_WITH_PARAMETERS_params, properties);

        try {
            PEPClientConfiguration pepConfiguration = initPepConfiguration(properties);
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
     * @param properties array of key value pairs containing the plugins configuration ( key1, value1, key2, value2, ...)
     * @throws PEPClientConfigurationException
     */
    private PEPClientConfiguration initPepConfiguration(Properties properties) throws PEPClientConfigurationException {
        checkNotNull(properties, G_PLAZMA_ARGUS_PLUGIN_ARGS_MUST_NOT_BE_NULL);

        PEPClientConfiguration pepConfig = new PEPClientConfiguration();

        _log.debug(INITIALISING_PEP_CLIENT_CONFIGURATION);

        pepConfig.addPEPDaemonEndpoint(getProperty(properties, PEP_ENDPOINT));
        _resourceId = getProperty(properties, RESOURCE_ID);
        _actionId = getProperty(properties, ACTION_ID);
        String trustMaterial = getProperty(properties, TRUST_MATERIAL);
        String hostCert = getProperty(properties, HOST_CERT);
        String hostKey = getProperty(properties, HOST_KEY);
        String keyPass = getProperty(properties, KEY_PASS);

        pepConfig.setTrustMaterial(trustMaterial);
        pepConfig.setKeyMaterial(hostCert, hostKey, keyPass);

        _log.debug(CONFIGURATION_resourceid_actionid, _resourceId, _actionId);

        return pepConfig;
    }

    private String getProperty(Properties properties, String key)
    {
        String value = properties.getProperty(key);
        checkArgument(value != null, "Undefined property: " + key);
        return value;
    }

    @Override
    public void account(Set<Principal> authorizedPrincipals)
            throws AuthenticationException {

        int decision = Result.DECISION_NOT_APPLICABLE;

        String dn = null;
        try {
            Collection<Principal> globusPrincipals = Collections.emptySet();
            if (authorizedPrincipals != null) {
                globusPrincipals = Collections2
                        .filter(authorizedPrincipals, instanceOf(GlobusPrincipal.class));
            }

            for (Principal principal : globusPrincipals) {
                dn = principal.getName();

                _log.info(AUTHORISING_SUBJECT_dn, dn);
                Request request = ArgusPepRequestFactory.create(dn, _resourceId, _actionId, GridWNAuthorizationProfile.getInstance());
                _log.debug(CREATED_REQUEST_request, request);
                Response response = _pepClient.authorize(request);
                _log.debug(RECEIVED_RESPONSE_response, response);

                for (Result result : response.getResults()) {
                    decision = result.getDecision();
                    switch (decision) {
                    case Result.DECISION_DENY:
                        throw new AuthenticationException("user banned");
                    case Result.DECISION_INDETERMINATE:
                        throw new AuthenticationException("indeterminate result");
                    }
                }
            }
        } catch (PEPClientException e) {
            decision = Result.DECISION_DENY;
            _log.warn(BLACKLIST_CHECK_FOR_USER_dn_FAILED_DUE_TO_EXCEPTION_IN_PLUGIN, dn ,e);
            throw new AuthenticationException("check failed", e);
        } finally {
            _log.info(DECISION_CODE_code, decision);
        }
    }
}
