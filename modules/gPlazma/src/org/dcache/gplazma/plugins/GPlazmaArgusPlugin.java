package org.dcache.gplazma.plugins;

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
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

    private final PEPClient _pepClient;

    private String _resourceId;
    private String _actionId;

    /**
     * Constructor
     * @param args a set of key value pairs containing the plugins configuration.
     */
    public GPlazmaArgusPlugin(String[] args) {

        _log.debug("Creating Argus Plugin with parameters: {}", args);

        try {
            PEPClientConfiguration pepConfiguration = initPepConfiguration(args);
            _pepClient = new PEPClient(pepConfiguration);
        } catch (PEPClientException e) {
            _log.error("Could not create PEP Client", e);
            throw new IllegalArgumentException("Invalid configuration for ARGUS plugin.", e);
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
        if (args==null) throw new IllegalArgumentException("GPlazmaArgusPlugin: args must not be null.");

        PEPClientConfiguration pepConfig = new PEPClientConfiguration();

        List<String> pepEndPoints = new ArrayList<String>();
        String trustMaterial = "";
        String hostCert = "";
        String hostKey = "";
        String keyPass = "";
        _resourceId = "DUMMY-RESOURCE"; // TODO: Needs to be set, due bug in ARGUS PDP (GGUS ticket #65839)
        _actionId = "DUMMY-ACTION"; // TODO: Needs to be set, due bug in ARGUS PDP (GGUS ticket #65839)

        _log.debug("Initialising PEPClientConfiguration");
        // parse config from args
        String prev = null;
        for (String arg: args) {
            if ("pependpoint".equalsIgnoreCase(prev)) {
                pepEndPoints.add(arg);
                arg = null;
            } else if ("resourceid".equalsIgnoreCase(prev)) {
                _resourceId = arg;
                arg = null;
            } else if ("actionid".equalsIgnoreCase(prev)) {
                _actionId = arg;
                arg = null;
            } else if ("trustmaterial".equalsIgnoreCase(prev)) {
                trustMaterial = arg;
                arg = null;
            } else if ("hostcert".equalsIgnoreCase(prev)) {
                hostCert = arg;
                arg = null;
            } else if ("hostkey".equalsIgnoreCase(prev)) {
                hostKey = arg;
                arg = null;
            } else if ("keypass".equalsIgnoreCase(prev)) {
                keyPass = arg;
                arg = null;
            }
            prev = arg;
        }

        for (String endPoint : pepEndPoints) pepConfig.addPEPDaemonEndpoint(endPoint);

        pepConfig.setTrustMaterial(trustMaterial);
        pepConfig.setKeyMaterial(hostCert, hostKey, keyPass);

        _log.debug("Configuration: \n{}\n{}\n", _resourceId, _actionId);

        return pepConfig;
    }


    /**
     * Extracts the DN from a GlobusPrincipal contained in authorizedPrincipals
     *
     * @param authorizedPrincipals
     * @return DN if principal exists, null otherwise
     */
    private String getDn(Set<Principal> authorizedPrincipals) {

        for (Principal principal : authorizedPrincipals) {
            if (principal instanceof GlobusPrincipal) {
                return principal.getName();
            }
        }
        return null;
    }

    @Override
    public void account(SessionID sID, Set<Principal> authorizedPrincipals)
            throws AuthenticationException {

        int decision = Result.DECISION_INDETERMINATE;
        String dn = getDn(authorizedPrincipals);

        _log.info("Authorising subject {} for session {}.", dn, sID);

        try {
            if (dn == null) return;

            Request request = ArgusPepRequestFactory.create(dn, _resourceId, _actionId, GridWNAuthorizationProfile.getInstance());

            _log.debug("Created request:\n{}", request);

            Response response = _pepClient.authorize(request);

            _log.debug("Received response:\n{}", response);

            for (Result result : response.getResults()) {
                decision = result.getDecision();
                switch (decision) {
                case Result.DECISION_DENY:
                    throw new AuthenticationException(String.format("User '{}' is blacklisted.", dn));
                case Result.DECISION_INDETERMINATE:
                    throw new AuthenticationException(String.format("Indeterminate result for user '{}'", dn));
                }
            }
        } catch (PEPClientException e) {
            decision = Result.DECISION_DENY;
            _log.warn("Blacklist check failed for user with dn '{}'. Exception: {}", dn ,e);
            throw new AuthenticationException(String.format("Blacklist check for user {} failed due to exception in plugin.", dn), e);
        } finally {
            _log.info("Decision code: {}", decision);
        }
    }
}
