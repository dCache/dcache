package org.dcache.webdav;

import com.bradmcevoy.http.Filter;
import com.bradmcevoy.http.FilterChain;
import com.bradmcevoy.http.Request;
import com.bradmcevoy.http.Response;

import org.dcache.auth.Subjects;

import com.sun.security.auth.UserPrincipal;
import javax.security.auth.Subject;
import java.security.AccessController;
import java.util.Set;

import org.apache.log4j.Logger;

/**
 * Simple request logging for WebDAV door. Interim solution until we
 * switch to a better logging framework with direct support for Jetty.
 */
public class LoggingFilter implements Filter
{
    private final Logger _log = Logger.getLogger(LoggingFilter.class);

    @Override
    public void process(final FilterChain filterChain,
                        final Request request,
                        final Response response)
    {
        try {
            filterChain.process(request, response);

            _log.info(String.format("%s %s %s %s %d",
                                    request.getFromAddress(),
                                    request.getMethod(),
                                    request.getAbsolutePath(),
                                    getUser(),
                                    response.getStatus().code));
        } catch (RuntimeException e) {
            _log.warn(String.format("%s %s %s %s %s",
                                    request.getFromAddress(),
                                    request.getMethod(),
                                    request.getAbsolutePath(),
                                    getUser(),
                                    e));
            throw e;
        }
    }

    private String getUser()
    {
        Subject subject = Subject.getSubject(AccessController.getContext());
        if (subject != null) {
            String user = Subjects.getDn(subject);
            if (user == null)  {
                Set<UserPrincipal> principals =
                    subject.getPrincipals(UserPrincipal.class);
                if (!principals.isEmpty()) {
                    return principals.iterator().next().getName();
                }
            }
        }
        return "";
    }
}