/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.xrootd.plugins;

import eu.emi.security.authn.x509.CrlCheckingMode;
import eu.emi.security.authn.x509.NamespaceCheckingMode;
import eu.emi.security.authn.x509.OCSPCheckingMode;
import io.netty.channel.ChannelHandler;
import io.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import java.io.File;
import java.util.List;
import java.util.Properties;

import dmg.cells.nucleus.CDC;

import org.dcache.ssl.CanlContextFactory;
import org.dcache.xrootd.plugins.tls.SSLHandlerFactory;

/**
 *  Provides an SSLHandler constructed from the SSLContext established
 *  via properties.  Each handler has a separate SSLEngine.  The handler
 *  is always constructed in startTls mode, as it should not be added
 *  to the pipeline until ready to send the last unprotected response
 *  (server) or initiate the TLS handshake (client).
 */
public class XrootdTLSHandlerFactory extends SSLHandlerFactory
{
    public static final String SERVER_TLS = "tls";
    public static final String CLIENT_TLS = "tls-client";

    private static final String SERVICE_KEY = "xrootd.security.tls.hostcert.key";
    private static final String SERVICE_CERT = "xrootd.security.tls.hostcert.cert";
    private static final String SERVICE_CACERTS = "xrootd.security.tls.ca.path";
    private static final String NAMESPACE_MODE = "xrootd.security.tls.ca.namespace-mode";
    private static final String CRL_MODE = "xrootd.security.tls.ca.crl-mode";
    private static final String OCSP_MODE = "xrootd.security.tls.ca.ocsp-mode";

    public static SSLHandlerFactory getHandlerFactory(String name,
                                                      List<ChannelHandlerFactory> list)
    {
        return (SSLHandlerFactory) list.stream()
                                       .filter(h -> name.equalsIgnoreCase(h.getName()))
                                       .findFirst().orElse(null);
    }

    public XrootdTLSHandlerFactory(Properties properties, boolean startTls) throws Exception
    {
        initialize(properties, startTls);
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public String getDescription()
    {
        return "Creates and configures Netty SSLHandler for the xrootd pipeline.";
    }

    @Override
    public ChannelHandler createHandler()
    {
        SSLEngine engine = sslContext.createSSLEngine();
        return new SslHandler(engine, startTls);
    }

    /*
     *  Called by the contructor.
     */
    @Override
    protected SSLContext buildContext(Properties properties) throws Exception
    {
        File serviceKey = new File(properties.getProperty(SERVICE_KEY));
        File serviceCert = new File(properties.getProperty(SERVICE_CERT));
        File serviceCaCerts = new File(properties.getProperty(SERVICE_CACERTS));
        NamespaceCheckingMode namespaceMode =
                        NamespaceCheckingMode.valueOf(properties.getProperty(NAMESPACE_MODE));
        CrlCheckingMode crlMode
                        = CrlCheckingMode.valueOf(properties.getProperty(CRL_MODE));
        OCSPCheckingMode ocspMode
                        = OCSPCheckingMode.valueOf(properties.getProperty(OCSP_MODE));

        return CanlContextFactory.custom()
                                 .withCertificatePath(serviceCert.toPath())
                                 .withKeyPath(serviceKey.toPath())
                                 .withCertificateAuthorityPath(serviceCaCerts.toPath())
                                 .withCrlCheckingMode(crlMode)
                                 .withOcspCheckingMode(ocspMode)
                                 .withNamespaceMode(namespaceMode)
                                 .withLazy(false)
                                 .withLoggingContext(new CDC()::restore)
                                 .buildWithCaching()
                                 .call();
    }
}
