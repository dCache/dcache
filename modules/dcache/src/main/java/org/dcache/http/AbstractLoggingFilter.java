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

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static org.dcache.http.AuthenticationFilter.DCACHE_SUBJECT_ATTRIBUTE;

import com.google.common.base.Stopwatch;
import com.google.common.net.InetAddresses;
import dmg.cells.nucleus.CDC;
import jakarta.servlet.AsyncEvent;
import jakarta.servlet.AsyncListener;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpServletResponseWrapper;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;
import javax.security.auth.Subject;
import org.dcache.auth.JwtJtiPrincipal;
import org.dcache.auth.OidcSubjectPrincipal;
import org.dcache.auth.Subjects;
import org.dcache.util.NetLoggerBuilder;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class act as a base logging class for logging servlet-based activity. It is expected that
 * this class is subclassed to add protocol-specific logging.
 */
public abstract class AbstractLoggingFilter implements Filter {

    private static final String X509_CERTIFICATE_ATTRIBUTE =
          "jakarta.servlet.request.X509Certificate";
    private static final String REMOTE_ADDRESS = "org.dcache.remote-address";
    private static final String PROCESSING_TIME = "org.dcache.processing-time";
    private static final String ERROR_MESSAGE = "org.dcache.error-message";

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractLoggingFilter.class);

    private class LogOnComplete implements AsyncListener {

        @Override
        public void onComplete(AsyncEvent event) {
            requestCompletedNormally(
                    (HttpServletRequest) event.getSuppliedRequest(),
                    (HttpServletResponse) event.getSuppliedResponse());
        }

        @Override
        public void onTimeout(AsyncEvent event) {
            var timeout = Duration.ofMillis(event.getAsyncContext().getTimeout());
            requestTimedOut(
                    (HttpServletRequest) event.getSuppliedRequest(),
                    (HttpServletResponse) event.getSuppliedResponse(),
                    timeout);
        }

        @Override
        public void onError(AsyncEvent event) {
            Throwable cause = event.getThrowable();
            if (cause instanceof Exception) {
                requestCompletedExceptionally(
                        (HttpServletRequest) event.getSuppliedRequest(),
                        (HttpServletResponse) event.getSuppliedResponse(),
                        (Exception) cause);
            }
        }

        @Override
        public void onStartAsync(AsyncEvent event) {
            LOGGER.warn("Unexpected reuse of async processing context for {}",
                  event.getAsyncContext().getRequest());
        }
    }

    /**
     * The SLF4J Logger to which we send access log entries.
     */
    protected abstract Logger accessLogger();

    /**
     * The name of the request events.
     */
    protected abstract String requestEventName();

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse,
          FilterChain chain) throws IOException, ServletException {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;
        Stopwatch processingTime = Stopwatch.createStarted();

        // Cache the remote client address because the client may disconnect
        // while dCache is processing the request, in which case Jetty
        // "forgets".
        request.setAttribute(REMOTE_ADDRESS, remoteAddress(request).orElse(null));
        request.setAttribute(PROCESSING_TIME, processingTime);

        try {
            chain.doFilter(request, new ErrorMessageResponse(request, response));
        } catch (IOException|RuntimeException|ServletException e) {
            requestCompletedExceptionally(request, response, e);

            throwIfInstanceOf(e, IOException.class);
            throwIfInstanceOf(e, ServletException.class);
            throwIfUnchecked(e);
            throw new AssertionError(e);
        }

        if (request.isAsyncStarted()) {
            request.getAsyncContext().addListener(new LogOnComplete());
        } else {
            requestCompletedNormally(request, response);
        }
    }

    private void requestCompletedExceptionally(HttpServletRequest request, HttpServletResponse response, Exception e) {
        requestCompleted(request, response, l -> {
                    l.add("failure", e);
                });
    }

    private void requestTimedOut(HttpServletRequest request, HttpServletResponse response, Duration timeout) {
        requestCompleted(request, response, l -> {
                    l.add("timeout", timeout);
                });
    }

    private void requestCompletedNormally(HttpServletRequest request, HttpServletResponse response) {
        requestCompleted(request, response, l -> {
                    l.add("response.reason", getReason(request, response));
                    l.add("response.code", response.getStatus());
                    l.add("location", response.getHeader("Location"));
                });
    }

    private void requestCompleted(HttpServletRequest request, HttpServletResponse response, Consumer<NetLoggerBuilder> logEnricher) {
        var processingTime = (Stopwatch) request.getAttribute(PROCESSING_TIME);

        processingTime.stop();

        NetLoggerBuilder.Level logLevel = logLevel(request, response);
        NetLoggerBuilder log = new NetLoggerBuilder(logLevel, requestEventName())
              .omitNullValues();
        describeOperation(log, request, response);
        log.add("duration", processingTime.elapsed().toMillis());
        logEnricher.accept(log);
        log.toLogger(accessLogger());
    }

    protected void describeOperation(NetLoggerBuilder log,
          HttpServletRequest request, HttpServletResponse response) {
        log.add("session", CDC.getSession());
        log.add("request.method", request.getMethod());
        log.add("request.url", request.getRequestURL());
        log.add("socket.remote", (InetSocketAddress) request.getAttribute(REMOTE_ADDRESS));
        log.add("user-agent", request.getHeader("User-Agent"));

        log.add("user.dn", getCertificateName(request));
        var subject = getSubject(request);
        log.add("user.sub", subject
                .flatMap(s -> Optional.ofNullable(Subjects.getPrincipalNames(s, OidcSubjectPrincipal.class)))
                .orElse(null));
        log.add("user.jti", subject
                .flatMap(s -> Optional.ofNullable(Subjects.getPrincipalNames(s, JwtJtiPrincipal.class)))
                .orElse(null));
        log.add("user.mapped", subject.orElse(null));
    }

    /**
     * Provide this connection's remote address; that is, the address of the client.  The method
     * returns Optional.empty if this cannot be determined, for whatever reason.
     */
    private static Optional<InetSocketAddress> remoteAddress(HttpServletRequest request) {
        String addrString = request.getRemoteAddr();
        int port = request.getRemotePort();

        if (addrString.isEmpty() || port == 0) { // Sometimes Jetty just doesn't know (!)
            return Optional.empty();
        }

        InetAddress addr;
        try {
            addr = InetAddresses.forUriString(addrString);
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Cannot build internet address: {}", e.getMessage());
            return Optional.empty();
        }

        return Optional.of(new InetSocketAddress(addr, port));
    }

    /**
     * Returns the message of the error sent, or else the standard reason phrase of the status.
     */
    private static String getReason(HttpServletRequest request, HttpServletResponse response) {
        Object message = request.getAttribute(ERROR_MESSAGE);
        return message != null ? message.toString() : HttpStatus.getMessage(response.getStatus());
    }

    /**
     * Records the message of an error sent, for the access log. The message is kept in a request
     * attribute, as an asynchronous request completes with the original response.
     */
    private static class ErrorMessageResponse extends HttpServletResponseWrapper {

        private final HttpServletRequest request;

        ErrorMessageResponse(HttpServletRequest request, HttpServletResponse response) {
            super(response);
            this.request = request;
        }

        @Override
        public void sendError(int code, String message) throws IOException {
            request.setAttribute(ERROR_MESSAGE, message);
            super.sendError(code, message);
        }
    }

    protected NetLoggerBuilder.Level logLevel(HttpServletRequest request,
          HttpServletResponse response) {
        int code = response.getStatus();
        if (code >= 500) {
            return NetLoggerBuilder.Level.ERROR;
        } else if (code >= 400) {
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

    private static Optional<Subject> getSubject(HttpServletRequest request) {
        Object object = request.getAttribute(DCACHE_SUBJECT_ATTRIBUTE);
        return Optional.ofNullable((object instanceof Subject) ? (Subject) object : null);
    }
}
