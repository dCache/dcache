/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017-2018 Deutsches Elektronen-Synchrotron
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

import com.google.common.net.InetAddresses;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.HttpProtocolInfo;

import dmg.cells.nucleus.CDC;

import eu.emi.security.authn.x509.CrlCheckingMode;
import eu.emi.security.authn.x509.OCSPCheckingMode;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslHandler;
import org.springframework.beans.factory.annotation.Required;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.UUID;


public class HttpsTransferService extends HttpTransferService {

    private static final String PROTOCOL_HTTPS = "https";

    private Path _serverCertificatePath;
    private Path _serverKeyPath;
    private Path _serverCaPath;
    private CrlCheckingMode _crlCheckingMode;
    private OCSPCheckingMode _ocspCheckingMode;
    private volatile SSLEngine _sslEngine;


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

    @Override
    protected URI getUri(HttpProtocolInfo protocolInfo, int port, UUID uuid)
            throws SocketException, CacheException, URISyntaxException {

        URI plainUrl = super.getUri(protocolInfo, port, uuid);
        String host = plainUrl.getHost();
        try {
            if (InetAddresses.isInetAddress(host)) {
                // An IP address is unlikely to be in the X.509 host credential.
                host = InetAddress.getByName(host).getCanonicalHostName();
            }
        } catch (UnknownHostException e) {
            // This should not happen as getByName should never throw this
            // exception for a valid IP address
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
    protected synchronized void startServer() throws IOException {
        super.startServer();
        try {
            _sslEngine = createContext().createSSLEngine();
            _sslEngine.setUseClientMode(false);
            _sslEngine.setWantClientAuth(false);
        } catch (Exception e) {
            throw new IOException("Failed to create SSL engine: " + e, e);
        }

    }

    @Override
    protected void addChannelHandlers(ChannelPipeline pipeline)
    {
        pipeline.addLast("ssl", new SslHandler(_sslEngine));
        super.addChannelHandlers(pipeline);
    }

    private synchronized SSLContext createContext() throws Exception {

        return org.dcache.ssl.CanlContextFactory.custom()
                .withCertificateAuthorityPath(_serverCaPath)
                .withCrlCheckingMode(_crlCheckingMode)
                .withOcspCheckingMode(_ocspCheckingMode)
                .withCertificatePath(_serverCertificatePath)
                .withKeyPath(_serverKeyPath)
                .withLazy(false)
                .withLoggingContext(new CDC()::restore)
                .buildWithCaching()
                .call();
    }
}
