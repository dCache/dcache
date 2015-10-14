/*
 * ====================================================================
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 *
 */

package org.dcache.srm.client;

import eu.emi.security.authn.x509.X509Credential;
import org.apache.http.HttpHost;
import org.apache.http.annotation.ThreadSafe;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.Args;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.security.auth.x500.X500Principal;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.dcache.ssl.SslContextFactory;

import static org.apache.http.conn.ssl.SSLConnectionSocketFactory.getDefaultHostnameVerifier;

/**
 * Layered socket factory for TLS/SSL connections.
 * <p>
 * FlexibleCredentialConnectionSocketFactory can be used to validate the identity of the
 * HTTPS server against a list of trusted certificates and to authenticate to the HTTPS
 * server using a private key.
 * <p>
 * A clone of org.apache.http.conn.ssl.SSLConnectionSocketFactory to allow the use of a
 * per-socket client certificate. The certificate is extracted from the HttpContext.
 */
@ThreadSafe @SuppressWarnings("deprecation")
public class FlexibleCredentialSSLConnectionSocketFactory implements LayeredConnectionSocketFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FlexibleCredentialSSLConnectionSocketFactory.class);

    private final SslContextFactory contextProvider;
    private final HostnameVerifier hostnameVerifier;
    private final String[] supportedProtocols;
    private final String[] supportedCipherSuites;

    public FlexibleCredentialSSLConnectionSocketFactory(SslContextFactory contextProvider) {
        this(contextProvider, getDefaultHostnameVerifier());
    }

    /**
     * @since 4.4
     */
    public FlexibleCredentialSSLConnectionSocketFactory(
            SslContextFactory contextProvider, HostnameVerifier hostnameVerifier) {
        this(contextProvider, null, null, hostnameVerifier);
    }

    /**
     * @since 4.4
     */
    public FlexibleCredentialSSLConnectionSocketFactory(
            SslContextFactory contextProvider,
            String[] supportedProtocols,
            String[] supportedCipherSuites,
            HostnameVerifier hostnameVerifier) {
        this.contextProvider = Args.notNull(contextProvider, "SSL socket factory");
        this.supportedProtocols = supportedProtocols;
        this.supportedCipherSuites = supportedCipherSuites;
        this.hostnameVerifier = hostnameVerifier != null ? hostnameVerifier : getDefaultHostnameVerifier();
    }

    /**
     * Performs any custom initialization for a newly created SSLSocket
     * (before the SSL handshake happens).
     *
     * The default implementation is a no-op, but could be overridden to, e.g.,
     * call {@link SSLSocket#setEnabledCipherSuites(String[])}.
     * @throws IOException may be thrown if overridden
     */
    protected void prepareSocket(final SSLSocket socket) throws IOException {
    }

    @Override
    public Socket createSocket(final HttpContext context) throws IOException {
        return SocketFactory.getDefault().createSocket();
    }

    @Override
    public Socket connectSocket(
            final int connectTimeout,
            final Socket socket,
            final HttpHost host,
            final InetSocketAddress remoteAddress,
            final InetSocketAddress localAddress,
            final HttpContext context) throws IOException {
        Args.notNull(host, "HTTP host");
        Args.notNull(remoteAddress, "Remote address");
        final Socket sock = socket != null ? socket : createSocket(context);
        if (localAddress != null) {
            sock.bind(localAddress);
        }
        try {
            if (connectTimeout > 0 && sock.getSoTimeout() == 0) {
                sock.setSoTimeout(connectTimeout);
            }
            LOGGER.debug("Connecting socket to {} with timeout {}", remoteAddress, connectTimeout);
            sock.connect(remoteAddress, connectTimeout);
        } catch (final IOException ex) {
            try {
                sock.close();
            } catch (final IOException ignore) {
            }
            throw ex;
        }
        // Setup SSL layering if necessary
        if (sock instanceof SSLSocket) {
            final SSLSocket sslsock = (SSLSocket) sock;
            LOGGER.debug("Starting handshake");
            sslsock.startHandshake();
            verifyHostname(sslsock, host.getHostName());
            return sock;
        } else {
            return createLayeredSocket(sock, host.getHostName(), remoteAddress.getPort(), context);
        }
    }

    @Override
    public Socket createLayeredSocket(
            final Socket socket,
            final String target,
            final int port,
            final HttpContext context) throws IOException {

        final X509Credential credential = (X509Credential) context.getAttribute(HttpClientTransport.TRANSPORT_HTTP_CREDENTIALS);
        if (credential == null) {
            throw new IOException("Client credentials are missing from context.");
        }
        final SSLContext sslContext;
        try {
            sslContext = contextProvider.getContext(credential);
        } catch (GeneralSecurityException e) {
            throw new IOException("Failed to create SSLContext: " + e.getMessage(), e);
        }
        final SSLSocket sslsock = (SSLSocket) sslContext.getSocketFactory().createSocket(
                socket,
                target,
                port,
                true);
        if (supportedProtocols != null) {
            sslsock.setEnabledProtocols(supportedProtocols);
        } else {
            // If supported protocols are not explicitly set, remove all SSL protocol versions
            final String[] allProtocols = sslsock.getEnabledProtocols();
            final List<String> enabledProtocols = new ArrayList<String>(allProtocols.length);
            for (String protocol: allProtocols) {
                if (!protocol.startsWith("SSL")) {
                    enabledProtocols.add(protocol);
                }
            }
            if (!enabledProtocols.isEmpty()) {
                sslsock.setEnabledProtocols(enabledProtocols.toArray(new String[enabledProtocols.size()]));
            }
        }
        if (supportedCipherSuites != null) {
            sslsock.setEnabledCipherSuites(supportedCipherSuites);
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Enabled protocols: {}", Arrays.asList(sslsock.getEnabledProtocols()));
            LOGGER.debug("Enabled cipher suites: {}", Arrays.asList(sslsock.getEnabledCipherSuites()));
        }

        prepareSocket(sslsock);
        LOGGER.debug("Starting handshake");
        sslsock.startHandshake();
        verifyHostname(sslsock, target);
        return sslsock;
    }

    private void verifyHostname(final SSLSocket sslsock, final String hostname) throws IOException {
        try {
            SSLSession session = sslsock.getSession();
            if (session == null) {
                // In our experience this only happens under IBM 1.4.x when
                // spurious (unrelated) certificates show up in the server'
                // chain.  Hopefully this will unearth the real problem:
                final InputStream in = sslsock.getInputStream();
                in.available();
                // If ssl.getInputStream().available() didn't cause an
                // exception, maybe at least now the session is available?
                session = sslsock.getSession();
                if (session == null) {
                    // If it's still null, probably a startHandshake() will
                    // unearth the real problem.
                    sslsock.startHandshake();
                    session = sslsock.getSession();
                }
            }
            if (session == null) {
                throw new SSLHandshakeException("SSL session not available");
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Secure session established");
                LOGGER.debug(" negotiated protocol: {}", session.getProtocol());
                LOGGER.debug(" negotiated cipher suite: {}", session.getCipherSuite());

                try {

                    final Certificate[] certs = session.getPeerCertificates();
                    final X509Certificate x509 = (X509Certificate) certs[0];
                    final X500Principal peer = x509.getSubjectX500Principal();

                    LOGGER.debug(" peer principal: {}", peer);
                    final Collection<List<?>> altNames1 = x509.getSubjectAlternativeNames();
                    if (altNames1 != null) {
                        final List<String> altNames = new ArrayList<>();
                        for (final List<?> aC : altNames1) {
                            if (!aC.isEmpty()) {
                                altNames.add((String) aC.get(1));
                            }
                        }
                        LOGGER.debug(" peer alternative names: {}", altNames);
                    }

                    final X500Principal issuer = x509.getIssuerX500Principal();
                    LOGGER.debug(" issuer principal: {}", issuer);
                    final Collection<List<?>> altNames2 = x509.getIssuerAlternativeNames();
                    if (altNames2 != null) {
                        final List<String> altNames = new ArrayList<>();
                        for (final List<?> aC : altNames2) {
                            if (!aC.isEmpty()) {
                                altNames.add((String) aC.get(1));
                            }
                        }
                        LOGGER.debug(" issuer alternative names: {}", altNames);
                    }
                } catch (Exception ignore) {
                }
            }

            if (!this.hostnameVerifier.verify(hostname, session)) {
                final Certificate[] certs = session.getPeerCertificates();
                final X509Certificate x509 = (X509Certificate) certs[0];
                final X500Principal x500Principal = x509.getSubjectX500Principal();
                throw new SSLPeerUnverifiedException("Host name '" + hostname + "' does not match " +
                        "the certificate subject provided by the peer (" + x500Principal.toString() + ")");
            }
            // verifyHostName() didn't blowup - good!
        } catch (RuntimeException | IOException iox) {
            // close the socket before re-throwing the exception
            try { sslsock.close(); } catch (final Exception x) { iox.addSuppressed(x); }
            throw iox;
        }
    }
}
