package org.dcache.gplazma.plugins;

import org.apache.axis.EngineConfiguration;
import org.opensciencegrid.authz.xacml.client.MapCredentialsClient;
import org.opensciencegrid.authz.xacml.common.LocalId;

import java.util.Properties;

import org.dcache.gplazma.AuthenticationException;

/**
 * Simple wrapper around {@link MapCredentialsClient}. This is the default
 * client for the {@link XACMLPlugin}.
 *
 * @author arossi
 */
public class PrivilegeDelegate implements IMapCredentialsClient {

    private MapCredentialsClient client;

    @Override
    public void configure(Properties properties) {
        client = new MapCredentialsClient();
    }

    @Override
    public LocalId mapCredentials(String mappingServiceURL)
                    throws AuthenticationException {
        try {
            return client.mapCredentials(mappingServiceURL);
            /*
             * Generic exception is thrown by privilege API ...
             */
        } catch (RuntimeException t) {
            throw t;
        } catch (Exception t) {
            throw new AuthenticationException(t.getMessage(), t);
        }
    }

    @Override
    public void setFqan(String name) {
        client.setFqan(name);
    }

    @Override
    public void setRequestedaction(String actionAccess) {
        client.setRequestedaction(actionAccess);
    }

    @Override
    public void setResourceDNSHostName(String resourceDNSHostName) {
        client.setResourceDNSHostName(resourceDNSHostName);
    }

    @Override
    public void setResourceType(String type) {
        client.setResourceType(type);
    }

    @Override
    public void setResourceX509ID(String targetServiceName) {
        client.setResourceX509ID(targetServiceName);
    }

    @Override
    public void setResourceX509Issuer(String targetServiceIssuer) {
        client.setResourceX509Issuer(targetServiceIssuer);
    }

    @Override
    public void setVO(String vo) {
        client.setVO(vo);
    }

    @Override
    public void setVOMSSigningIssuer(String vomsSigningIssuer) {
        client.setVOMSSigningIssuer(vomsSigningIssuer);
    }

    @Override
    public void setVOMSSigningSubject(String vomsSigningSubject) {
        client.setVOMSSigningSubject(vomsSigningSubject);
    }

    @Override
    public void setX509Subject(String subject) {
        client.setX509Subject(subject);
    }

    @Override
    public void setX509SubjectIssuer(String x509SubjectIssuer) {
        client.setX509SubjectIssuer(x509SubjectIssuer);
    }

    @Override
    public void setAxisConfiguration(EngineConfiguration axisConfiguration)
    {
        client.setAxisConfiguration(axisConfiguration);
    }
}
