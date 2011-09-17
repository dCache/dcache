package org.dcache.webdav;

import com.bradmcevoy.http.Filter;
import com.bradmcevoy.http.FilterChain;
import com.bradmcevoy.http.Request;
import com.bradmcevoy.http.Response;
import com.bradmcevoy.http.ServletRequest;

import org.dcache.auth.Subjects;

import com.sun.security.auth.UserPrincipal;
import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;
import java.security.AccessController;
import java.security.cert.X509Certificate;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple request logging for WebDAV door. Interim solution until we
 * switch to a better logging framework with direct support for Jetty.
 */
public class LoggingFilter implements Filter
{
    private final Logger _log = LoggerFactory.getLogger(LoggingFilter.class);

    private static final String X509_CERTIFICATE_ATTRIBUTE =
        "javax.servlet.request.X509Certificate";

    @Override
    public void process(final FilterChain filterChain,
                        final Request request,
                        final Response response)
    {
        try {
            filterChain.process(request, response);

            Response.Status status = response.getStatus();
            if (status != null) {
                _log.info("{} {} {} {} {}",
                          new Object[] { request.getFromAddress(),
                                         request.getMethod(),
                                         request.getAbsolutePath(),
                                         getUser(),
                                         status.code });
            } else {
                _log.info("{} {} {} {}",
                          new Object[] { request.getFromAddress(),
                                         request.getMethod(),
                                         request.getAbsolutePath(),
                                         getUser() });
            }
        } catch (RuntimeException e) {
            _log.warn(String.format("%s %s %s %s",
                                    request.getFromAddress(),
                                    request.getMethod(),
                                    request.getAbsolutePath(),
                                    getUser()),
                                    e);
            throw e;
        }
    }

    private String getUser()
    {
        HttpServletRequest servletRequest = ServletRequest.getRequest();
        Object object =
            servletRequest.getAttribute(X509_CERTIFICATE_ATTRIBUTE);
        if (object instanceof X509Certificate[]) {
            X509Certificate[] chain = (X509Certificate[]) object;
            if (chain.length >= 1) {
                return chain[0].getSubjectX500Principal().getName();
            }
        }
        return "";
   }
}