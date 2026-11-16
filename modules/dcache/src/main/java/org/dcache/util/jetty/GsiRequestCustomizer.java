package org.dcache.util.jetty;

import static org.dcache.gsi.ServerGsiEngine.X509_CREDENTIAL;

import eu.emi.security.authn.x509.X509Credential;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.io.ssl.SslConnection.SslEndPoint;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Customizer that extracts the GSI attributes from an {@link javax.net.ssl.SSLContext} and sets
 * them on the request with {@link javax.servlet.ServletRequest#setAttribute(String, Object)}
 * according to JGlobus requirements.
 */
public class GsiRequestCustomizer implements HttpConfiguration.Customizer {

    private static final Logger LOGGER = LoggerFactory.getLogger(GsiRequestCustomizer.class);

    @Override
    public Request customize(Request request, HttpFields.Mutable responseHeaders) {
        EndPoint endPoint = request.getConnectionMetaData().getConnection().getEndPoint();
        if (endPoint instanceof SslEndPoint sslEndPoint) {
            customize(sslEndPoint.getSslConnection().getSSLEngine(), request);
        }
        return request;
    }

    /**
     * Inject the delegated credentials into the request as attribute org.globus.gsi.credentials.
     */
    public void customize(SSLEngine sslEngine, Request request) {
        SSLSession sslSession = sslEngine.getSession();
        try {
            X509Credential delegCred = (X509Credential) sslSession.getValue(X509_CREDENTIAL);
            if (delegCred != null) {
                request.setAttribute(X509_CREDENTIAL, delegCred);
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to inject delegated credential", e);
        }
    }

    @Override
    public String toString() {
        return String.format("%s@%x", this.getClass().getSimpleName(), hashCode());
    }
}
