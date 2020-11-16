/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 Deutsches Elektronen-Synchrotron
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

import com.google.common.base.Stopwatch;
import com.google.common.net.InetAddresses;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.handler.HandlerWrapper;
import org.slf4j.Logger;

import javax.security.auth.Subject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.cert.X509Certificate;

import dmg.cells.nucleus.CDC;

import org.dcache.util.NetLoggerBuilder;

import static org.dcache.http.AuthenticationHandler.DCACHE_SUBJECT_ATTRIBUTE;

/**
 * This class act as a base logging class for logging servlet-based activity.
 * It is expected that this class is subclassed to add protocol-specific
 * logging.
 */
public abstract class AbstractLoggingHandler extends HandlerWrapper
{
    private static final String X509_CERTIFICATE_ATTRIBUTE =
            "javax.servlet.request.X509Certificate";

    /** The SLF4J Logger to which we send access log entries. */
    protected abstract Logger accessLogger();

    /** The name of the request events. */
    protected abstract String requestEventName();

    @Override
    public void handle(String target, Request baseRequest,
            HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException
    {
        if (isStarted() && !baseRequest.isHandled()) {
            Stopwatch processingTime = Stopwatch.createStarted();

            super.handle(target, baseRequest, request, response);

            processingTime.stop();

            NetLoggerBuilder.Level logLevel = logLevel(request, response);
            NetLoggerBuilder log = new NetLoggerBuilder(logLevel, requestEventName())
                    .omitNullValues();
            describeOperation(log, request, response);
            log.add("duration", processingTime.elapsed().toMillis());
            log.toLogger(accessLogger());
        }
    }

    protected void describeOperation(NetLoggerBuilder log,
            HttpServletRequest request, HttpServletResponse response)
    {
        log.add("session", CDC.getSession());
        log.add("request.method", request.getMethod());
        log.add("request.url", request.getRequestURL());
        log.add("response.code", response.getStatus());
        log.add("response.reason", getReason(response));
        log.add("location", response.getHeader("Location"));
        InetAddress addr = InetAddresses.forUriString(request.getRemoteAddr());
        log.add("socket.remote", new InetSocketAddress(addr, request.getRemotePort()));
        log.add("user-agent", request.getHeader("User-Agent"));

        log.add("user.dn", getCertificateName(request));
        log.add("user.mapped", getSubject(request));
    }

    private static String getReason(HttpServletResponse response)
    {
        if (response instanceof Response) {
            return ((Response) response).getReason();
        } else {
            return HttpStatus.getMessage(response.getStatus());
        }
    }

    protected NetLoggerBuilder.Level logLevel(HttpServletRequest request,
            HttpServletResponse response)
    {
        int code = response.getStatus();
        if (code >= 500) {
            return NetLoggerBuilder.Level.ERROR;
        } else if (code >= 400) {
            return NetLoggerBuilder.Level.WARN;
        } else {
            return NetLoggerBuilder.Level.INFO;
        }
    }

    private static String getCertificateName(HttpServletRequest request)
    {
        Object object = request.getAttribute(X509_CERTIFICATE_ATTRIBUTE);

        if (object instanceof X509Certificate[]) {
            X509Certificate[] chain = (X509Certificate[]) object;

            if (chain.length >= 1) {
                return chain[0].getSubjectX500Principal().getName();
            }
        }

        return null;
    }

    private static Subject getSubject(HttpServletRequest request)
    {
        Object object = request.getAttribute(DCACHE_SUBJECT_ATTRIBUTE);
        return (object instanceof Subject) ? (Subject) object : null;
    }
}
