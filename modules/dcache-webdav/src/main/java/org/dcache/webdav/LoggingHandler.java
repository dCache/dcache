package org.dcache.webdav;

import com.google.common.net.InetAddresses;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.handler.HandlerWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import org.dcache.webdav.macaroons.MacaroonRequestHandler;
import org.dcache.webdav.transfer.CopyFilter;

import static org.dcache.http.AuthenticationHandler.DCACHE_SUBJECT_ATTRIBUTE;
import static org.dcache.webdav.DcacheResourceFactory.TRANSACTION_ATTRIBUTE;


public class LoggingHandler extends HandlerWrapper {

    private final Logger ACCESS_LOGGER =
            LoggerFactory.getLogger("org.dcache.access.webdav");

    private static final String X509_CERTIFICATE_ATTRIBUTE =
            "javax.servlet.request.X509Certificate";

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        if (isStarted() && !baseRequest.isHandled()) {
            super.handle(target, baseRequest, request, response);

            NetLoggerBuilder log = new NetLoggerBuilder(logLevel(request, response),
                    "org.dcache.webdav.request").omitNullValues();
            log.add("session", CDC.getSession());
            log.add("transaction", getTransaction(request));
            log.add("request.method", request.getMethod());
            log.add("request.url", request.getRequestURL());
            log.add("request.macaroon-request", MacaroonRequestHandler.getMacaroonRequest(request));
            log.add("response.code", response.getStatus());
            log.add("response.reason", getReason(response));
            log.add("response.macaroon-id", MacaroonRequestHandler.getMacaroonId(request));
            log.add("location", response.getHeader("Location"));
            InetAddress addr = InetAddresses.forString(request.getRemoteAddr());
            log.add("socket.remote", new InetSocketAddress(addr, request.getRemotePort()));
            log.add("user-agent", request.getHeader("User-Agent"));
            log.add("tpc.credential", CopyFilter.getTpcCredential(request));
            log.add("tpc.error", CopyFilter.getTpcError(request));
            log.add("tpc.require-checksum", CopyFilter.getTpcRequireChecksumVerification(request));
            log.add("tpc.source", CopyFilter.getTpcSource(request));
            log.add("tpc.destination", CopyFilter.getTpcDestination(request));

            log.add("user.dn", getCertificateName(request));
            log.add("user.mapped", getSubject(request));
            log.toLogger(ACCESS_LOGGER);
        }
    }

    private static String getReason(HttpServletResponse response)
    {
        if (response instanceof Response) {
            return ((Response) response).getReason();
        } else {
            return HttpStatus.getMessage(response.getStatus());
        }
    }

    private static NetLoggerBuilder.Level logLevel(HttpServletRequest request,
            HttpServletResponse response)
    {
        int code = response.getStatus();
        if (code >= 500) {
            return NetLoggerBuilder.Level.ERROR;
        } else if (code >= 400 || CopyFilter.getTpcError(request) != null) {
            return NetLoggerBuilder.Level.WARN;
        } else {
            return NetLoggerBuilder.Level.INFO;
        }
    }

    private static String getCertificateName(HttpServletRequest request) {

        Object object = request.getAttribute(X509_CERTIFICATE_ATTRIBUTE);

        if (object instanceof X509Certificate[]) {
            X509Certificate[] chain = (X509Certificate[]) object;

            if (chain.length >= 1) {
                return chain[0].getSubjectX500Principal().getName();
            }
        }

        return null;
    }

    private static String getTransaction(HttpServletRequest request) {
        Object object = request.getAttribute(TRANSACTION_ATTRIBUTE);

        return object == null ? null : String.valueOf(object);
    }

    private static Subject getSubject(HttpServletRequest request) {
        Object object = request.getAttribute(DCACHE_SUBJECT_ATTRIBUTE);

        return (object instanceof Subject) ? (Subject) object : null;
    }
}
