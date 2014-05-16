package org.dcache.webdav;

import io.milton.http.Filter;
import io.milton.http.FilterChain;
import io.milton.http.Request;
import io.milton.http.Response;
import io.milton.http.Response.Status;
import io.milton.servlet.ServletRequest;
import io.milton.servlet.ServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.security.cert.X509Certificate;
import java.util.Arrays;

import dmg.cells.nucleus.CDC;

import org.dcache.auth.Subjects;
import org.dcache.util.NetLoggerBuilder;

import static org.dcache.webdav.DcacheResourceFactory.TRANSACTION_ATTRIBUTE;
import static org.dcache.webdav.SecurityFilter.DCACHE_SUBJECT_ATTRIBUTE;

/**
 * Request logging for WebDAV door following the NetLogger format. Interim
 * solution until we switch to a better logging framework with direct support
 * for Jetty.
 */
public class LoggingFilter implements Filter
{
    private final Logger ACCESS_LOGGER =
            LoggerFactory.getLogger("org.dcache.access.webdav");

    private static final String X509_CERTIFICATE_ATTRIBUTE =
        "javax.servlet.request.X509Certificate";

    @Override
    public void process(final FilterChain filterChain,
                        final Request request,
                        final Response response)
    {
        filterChain.process(request, response);

        int code = getCode(response);

        NetLoggerBuilder log = new NetLoggerBuilder(logLevel(code),
                "org.dcache.webdav.request").omitNullValues();

        log.add("request.method", request.getMethod());
        log.add("request.url", request.getAbsoluteUrl());
        log.add("response.code", code);
        log.add("response.reason", getReason(response));
        log.add("location", ServletResponse.getResponse().getHeader("Location"));
        log.add("host.remote", request.getFromAddress());
        log.add("user-agent", request.getUserAgentHeader());

        log.add("user.dn", getCertificateName());
        Subject subject = getSubject();
        if (subject != null) {
            if (Subjects.isNobody(subject)) {
                log.add("user", "NOBODY");
            } else if (Subjects.isRoot(subject)) {
                log.add("user", "ROOT");
            } else {
                log.add("user.uid", Subjects.getUid(subject));
                log.add("user.gid", Arrays.toString(Subjects.getGids(subject)));
            }
        }
        log.add("session", CDC.getSession());
        log.add("transaction", getTransaction());
        log.toLogger(ACCESS_LOGGER);
    }

    private static int getCode(Response response)
    {
        Status status = response.getStatus();

        return (status != null) ? status.code :
                ServletResponse.getResponse().getStatus();
    }

    private static String getReason(Response response)
    {
        Status status = response.getStatus();

        if (status != null) {
            return status.text;
        }

        HttpServletResponse servletResponse = ServletResponse.getResponse();

        if (servletResponse instanceof org.eclipse.jetty.server.Response) {
            return ((org.eclipse.jetty.server.Response) servletResponse).getReason();
        }

        return null;
    }

    private static NetLoggerBuilder.Level logLevel(int code)
    {
        if (code >= 500) {
            return NetLoggerBuilder.Level.ERROR;
        } else if (code >= 400) {
            return NetLoggerBuilder.Level.WARN;
        } else {
            return NetLoggerBuilder.Level.INFO;
        }
    }

    private static String getCertificateName()
    {
        Object object = ServletRequest.getRequest().
                getAttribute(X509_CERTIFICATE_ATTRIBUTE);

        if (object instanceof X509Certificate[]) {
            X509Certificate[] chain = (X509Certificate[]) object;

            if (chain.length >= 1) {
                return chain[0].getSubjectX500Principal().getName();
            }
        }

        return null;
    }

    private static String getTransaction()
    {
        Object object = ServletRequest.getRequest().
                getAttribute(TRANSACTION_ATTRIBUTE);

        return object == null ? null : String.valueOf(object);
    }

    private static Subject getSubject()
    {
        Object object = ServletRequest.getRequest().
                getAttribute(DCACHE_SUBJECT_ATTRIBUTE);

        return (object instanceof Subject) ? (Subject) object : null;
    }
}
