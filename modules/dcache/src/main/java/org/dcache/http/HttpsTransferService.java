/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017-2020 Deutsches Elektronen-Synchrotron
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
package org.dcache.http;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.InetAddresses;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.HttpProtocolInfo;

import dmg.cells.nucleus.CDC;

import eu.emi.security.authn.x509.CrlCheckingMode;
import eu.emi.security.authn.x509.OCSPCheckingMode;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.annotation.PostConstruct;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import java.net.InetAddress;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.Callable;


public class HttpsTransferService extends HttpTransferService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpsTransferService.class);

    private static final String PROTOCOL_HTTPS = "https";

    private Path _serverCertificatePath;
    private Path _serverKeyPath;
    private Path _serverCaPath;
    private CrlCheckingMode _crlCheckingMode;
    private OCSPCheckingMode _ocspCheckingMode;
    private Callable<SSLContext> _sslContextProvider;


    @Required
    public void setServerCertificatePath(Path serverCertificatePath) {
        _serverCertificatePath = serverCertificatePath;
    }

    @Required
    public void setServerKeyPath(Path serverKeyPath) {
        _serverKeyPath = serverKeyPath;
    }

    @Required
    public void setServerCaPath(Path serverCaPath) {
        _serverCaPath = serverCaPath;
    }

    @Required
    public void setCrlCheckingMode(CrlCheckingMode crlCheckingMode)
    {
        _crlCheckingMode = crlCheckingMode;
    }

    @Required
    public void setOcspCheckingMode(OCSPCheckingMode ocspCheckingMode)
    {
        _ocspCheckingMode = ocspCheckingMode;
    }

    @PostConstruct
    public void buildSSLContextProvider() throws Exception
    {
        _sslContextProvider = org.dcache.ssl.CanlContextFactory.custom()
                .withCertificateAuthorityPath(_serverCaPath)
                .withCrlCheckingMode(_crlCheckingMode)
                .withOcspCheckingMode(_ocspCheckingMode)
                .withCertificatePath(_serverCertificatePath)
                .withKeyPath(_serverKeyPath)
                .withLazy(false)
                .withLoggingContext(new CDC()::restore)
                .buildWithCaching();

        /*
         * Fail-fast: try to build an SSLContext (and discard result) so that
         * bad deployment / configuration causes the pool to die on startup.
         */
        _sslContextProvider.call();
    }

    /**
     * Obtain the hostname or IP address from a URL.  Unlike URI#getHost, this
     * method returns any IPv6 address without the square brackets.
     * @param url
     * @return
     */
    @VisibleForTesting
    static String getHost(URI url)
    {
        String host = url.getHost();
        return host != null && !host.isEmpty() && host.charAt(0) == '[' && host.charAt(host.length()-1) == ']'
                ? host.substring(1, host.length()-1)
                : host;
    }

    @Override
    protected URI getUri(HttpProtocolInfo protocolInfo, int port, UUID uuid)
            throws SocketException, CacheException, URISyntaxException {

        URI plainUrl = super.getUri(protocolInfo, port, uuid);
        String host = getHost(plainUrl);
        try {
            if (InetAddresses.isInetAddress(host)) {
                // An IP address is unlikely to be in the X.509 host credential.
                host = InetAddress.getByName(host).getCanonicalHostName();
            }
            if (InetAddresses.isInetAddress(host)) {
                LOGGER.warn("Unable to resolve IP address {} to a canonical name", host);
            }
        } catch (UnknownHostException e) {
            // This should not happen as getByName should never throw this
            // exception for a valid IP address
            LOGGER.warn("Unable to resolve IP address {}: {}", host, e.toString());
        }
        return new URI(PROTOCOL_HTTPS,
                plainUrl.getUserInfo(),
                host,
                plainUrl.getPort(),
                plainUrl.getPath(),
                plainUrl.getQuery(),
                plainUrl.getFragment());
    }

    @Override
    protected void addChannelHandlers(ChannelPipeline pipeline) throws Exception
    {
        SSLEngine engine = _sslContextProvider.call().createSSLEngine();
        engine.setUseClientMode(false);
        engine.setWantClientAuth(false);

        pipeline.addLast("ssl", new SslHandler(engine));
        super.addChannelHandlers(pipeline);
    }
}
