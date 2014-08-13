package org.dcache.util.jetty;

import org.eclipse.jetty.io.ssl.SslConnection;
import org.eclipse.jetty.io.ssl.SslConnection.DecryptedEndPoint;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.util.log.Log;
import org.ietf.jgss.GSSCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;

import static org.globus.axis.gsi.GSIConstants.GSI_CREDENTIALS;


/** Customizer that extracts the GSI attributes from an {@link javax.net.ssl.SSLContext}
 * and sets them on the request with {@link javax.servlet.ServletRequest#setAttribute(String, Object)}
 * according to JGlobus requirements.
 */
public class GsiRequestCustomizer implements HttpConfiguration.Customizer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GsiRequestCustomizer.class);

    @Override
    public void customize(Connector connector, HttpConfiguration channelConfig, Request request)
    {
        if (request.getHttpChannel().getEndPoint() instanceof DecryptedEndPoint) {
            DecryptedEndPoint ssl_endp = (DecryptedEndPoint)request.getHttpChannel().getEndPoint();
            SslConnection sslConnection = ssl_endp.getSslConnection();
            SSLEngine sslEngine=sslConnection.getSSLEngine();
            customize(sslEngine,request);
        }
    }

    /**
     * Inject the delegated credentials into the request as attribute org.globus.gsi.credentials.
     */
    public void customize(SSLEngine sslEngine, Request request)
    {
        SSLSession sslSession = sslEngine.getSession();
        try {
            GSSCredential delegCred = (GSSCredential) sslSession.getValue(GSI_CREDENTIALS);
            if (delegCred != null) {
                request.setAttribute(GSI_CREDENTIALS, delegCred);
            }
        } catch (Exception e) {
            LOGGER.warn(Log.EXCEPTION, e);
        }
    }

    @Override
    public String toString()
    {
        return String.format("%s@%x",this.getClass().getSimpleName(),hashCode());
    }
}
