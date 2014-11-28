package org.dcache.util.aspects;

import org.eclipse.jetty.server.HttpConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLHandshakeException;

import java.net.InetSocketAddress;

public aspect LogSSLHandshakeExceptionAspect
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpConnection.class);

    before(HttpConnection c, Exception e) : withincode(void HttpConnection.onFillable()) && this(c) && handler(Exception) && args(e) {
        if (e instanceof SSLHandshakeException) {
            InetSocketAddress remoteAddress = c.getEndPoint().getRemoteAddress();
            LOGGER.warn("SSL handshake with {}:{} failed: {}", remoteAddress.getAddress().getHostAddress(), remoteAddress.getPort(), e.getMessage());
        }
    }
}
