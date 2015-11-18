package org.dcache.gplazma.plugins;

import org.apache.axis.EngineConfiguration;
import org.opensaml.xacml.XACMLConstants;
import org.opensciencegrid.authz.xacml.client.MapCredentialsClient;
import org.opensciencegrid.authz.xacml.common.LocalId;

import java.util.Properties;

import org.dcache.gplazma.AuthenticationException;

/**
 * Exports the relevant methods on the privilege {@link MapCredentialsClient}.
 * This wrapper is used to provide the ability to swap in a custom
 * implementation of the client (e.g., for local stand-alone testing).
 *
 * @author arossi
 */
public interface IMapCredentialsClient {

    /**
     * This method should always be called post-construction, as it is
     * responsible for any special set-up on the client.
     *
     * @param properties
     */
    void configure(Properties properties);

    /**
     * Does the actual mapping work based on the various attributes set on the
     * client.
     *
     * @param mappingServiceURL
     *            may be <code>null</code> or unused if the client is a test
     *            delegate; otherwise gives service endpoint.
     * @return local id to which the attributes and DN are mapped, or
     *         <code>null</code> if no match
     * @throws AuthenticationException
     */
    LocalId mapCredentials(String mappingServiceURL)
                    throws AuthenticationException;

    /**
     * @param name
     *            a VOMS fully qualified attribute name
     */
    void setFqan(String name);

    /**
     * @param action
     *            usually XACMLConstants.ACTION_ACCESS (
     *            <code>http://authz-interop.org/xacml/action/action-type/access</code>
     *            ), access permission)
     *            {@link XACMLConstants}
     */
    void setRequestedaction(String action);

    /**
     * @param resourceDNSHostName
     *            usually determined by the plugin
     */
    void setResourceDNSHostName(String resourceDNSHostName);

    /**
     * @param type
     *            usually XACMLConstants.RESOURCE_SE (
     *            <code>http://authz-interop.org/xacml/resource/resource-type/se</code>
     *            ), storage resource) {@link XACMLConstants}
     */
    void setResourceType(String type);

    /**
     * @param targetServiceName
     *            usually determined by the plugin from host certificate
     */
    void setResourceX509ID(String targetServiceName);

    /**
     * @param targetServiceIssuer
     *            usually determined by the plugin from host certificate
     */
    void setResourceX509Issuer(String targetServiceIssuer);

    /**
     * @param vo
     *            (extended VOMS certificate attribute)
     */
    void setVO(String vo);

    /**
     * @param vomsSigningIssuer
     *            (extended VOMS certificate attribute)
     */
    void setVOMSSigningIssuer(String vomsSigningIssuer);

    /**
     * @param vomsSigningSubject
     *            (extended VOMS certificate attribute)
     */
    void setVOMSSigningSubject(String vomsSigningSubject);

    /**
     * @param subject
     *            the user DN
     */
    void setX509Subject(String subject);

    /**
     * @param x509SubjectIssuer
     *            , authority for the user DN
     */
    void setX509SubjectIssuer(String x509SubjectIssuer);

    void setAxisConfiguration(EngineConfiguration axisConfiguration);
}
